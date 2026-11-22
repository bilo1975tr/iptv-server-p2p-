package packager

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"iptv-p2p-server/pkg/protocol"
	"log"
	"sync"
	"time"
)

// ChunkRingBuffer keeps the last 60 seconds of chunks in memory
type ChunkRingBuffer struct {
	mu        sync.RWMutex
	chunks    map[uint64]*protocol.P2PMessage // sequence -> message
	maxSize   int
	latestSeq uint64
}

func NewChunkRingBuffer(maxSize int) *ChunkRingBuffer {
	return &ChunkRingBuffer{
		chunks:  make(map[uint64]*protocol.P2PMessage),
		maxSize: maxSize,
	}
}

func (rb *ChunkRingBuffer) Add(msg *protocol.P2PMessage) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	seq := msg.Header.Sequence
	rb.chunks[seq] = msg
	rb.latestSeq = seq

	if len(rb.chunks) > rb.maxSize && seq > uint64(rb.maxSize) {
		minSeq := seq - uint64(rb.maxSize)
		for s := range rb.chunks {
			if s <= minSeq {
				delete(rb.chunks, s)
			}
		}
	}
}

func (rb *ChunkRingBuffer) Get(seq uint64) (*protocol.P2PMessage, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	msg, found := rb.chunks[seq]
	return msg, found
}

func (rb *ChunkRingBuffer) GetLatestSequence() uint64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.latestSeq
}

// Chunker processes raw fMP4 stream from FFmpeg into box-aligned P2P chunks
type Chunker struct {
	ChannelID    string
	Codec        string // HEVC or AVC
	Resolution   string // 1080p, 720p, etc.
	IsPaidTier   bool
	AESMasterKey []byte

	ringBuffer  *ChunkRingBuffer
	initMu      sync.RWMutex
	initSegment []byte

	mu          sync.Mutex
	subscribers map[chan *protocol.P2PMessage]struct{}
	closed      bool
}

func NewChunker(channelID, codec, resolution string, isPaid bool, masterKeyHex string) (*Chunker, error) {
	var masterKey []byte
	if isPaid {
		if len(masterKeyHex) == 64 {
			key, err := hex.DecodeString(masterKeyHex)
			if err == nil && len(key) == 32 {
				masterKey = key
			}
		}
		if masterKey == nil && len(masterKeyHex) == 32 {
			masterKey = []byte(masterKeyHex)
		}
		if masterKey == nil {
			hash := sha256.Sum256([]byte(masterKeyHex))
			masterKey = hash[:]
		}
	}

	return &Chunker{
		ChannelID:    channelID,
		Codec:        codec,
		Resolution:   resolution,
		IsPaidTier:   isPaid,
		AESMasterKey: masterKey,
		ringBuffer:   NewChunkRingBuffer(60),
		subscribers:  make(map[chan *protocol.P2PMessage]struct{}),
	}, nil
}

// Subscribe returns a channel that receives newly created chunks in real-time
func (c *Chunker) Subscribe() chan *protocol.P2PMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan *protocol.P2PMessage, 16)
	if !c.closed {
		c.subscribers[ch] = struct{}{}
	} else {
		close(ch)
	}
	return ch
}

// Unsubscribe removes and closes a subscriber channel
func (c *Chunker) Unsubscribe(ch chan *protocol.P2PMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subscribers[ch]; ok {
		delete(c.subscribers, ch)
		close(ch)
	}
}

// Close closes all active subscriber channels
func (c *Chunker) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for ch := range c.subscribers {
		close(ch)
		delete(c.subscribers, ch)
	}
}

// GetRingBuffer returns the chunk storage buffer
func (c *Chunker) GetRingBuffer() *ChunkRingBuffer {
	return c.ringBuffer
}

// SetCodec updates the active video codec label (AVC or HEVC) safely
func (c *Chunker) SetCodec(codec string) {
	c.initMu.Lock()
	defer c.initMu.Unlock()
	c.Codec = codec
}

// GetCodec returns the active video codec label (AVC or HEVC)
func (c *Chunker) GetCodec() string {
	c.initMu.RLock()
	defer c.initMu.RUnlock()
	return c.Codec
}

// GetInitSegment returns the fMP4 initialization segment (ftyp + moov)
func (c *Chunker) GetInitSegment() []byte {
	c.initMu.RLock()
	defer c.initMu.RUnlock()
	if len(c.initSegment) == 0 {
		return nil
	}
	out := make([]byte, len(c.initSegment))
	copy(out, c.initSegment)
	return out
}

// readMP4Box reads one complete ISO BMFF / MP4 box from reader
func readMP4Box(reader io.Reader) (boxType string, boxBytes []byte, err error) {
	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return "", nil, err
	}

	size32 := binary.BigEndian.Uint32(header[0:4])
	boxType = string(header[4:8])

	if size32 == 1 {
		var ext [8]byte
		if _, err := io.ReadFull(reader, ext[:]); err != nil {
			return "", nil, err
		}
		size64 := binary.BigEndian.Uint64(ext[:])
		if size64 < 16 || size64 > 64*1024*1024 {
			return "", nil, io.ErrUnexpectedEOF
		}
		buf := make([]byte, size64)
		copy(buf[0:8], header[:])
		copy(buf[8:16], ext[:])
		if _, err := io.ReadFull(reader, buf[16:]); err != nil {
			return "", nil, err
		}
		return boxType, buf, nil
	}

	if size32 < 8 || size32 > 64*1024*1024 {
		return "", nil, io.ErrUnexpectedEOF
	}

	buf := make([]byte, size32)
	copy(buf[0:8], header[:])
	if _, err := io.ReadFull(reader, buf[8:]); err != nil {
		return "", nil, err
	}
	return boxType, buf, nil
}

// ProcessStream reads an fMP4 byte stream from FFmpeg and slices it on MP4 moof+mdat fragment boundaries.
// Returns the number of chunks emitted before EOF or read error.
func (c *Chunker) ProcessStream(reader io.Reader) int {
	seq := c.ringBuffer.GetLatestSequence() + 1
	var initBuf []byte
	var pendingFrag []byte
	emitted := 0

	for {
		boxType, boxData, err := readMP4Box(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("[CHUNKER-%s] Stream read stopped: %v", c.ChannelID, err)
			}
			break
		}

		switch boxType {
		case "ftyp":
			initBuf = append(initBuf[:0], boxData...)
		case "moov":
			initBuf = append(initBuf, boxData...)
			c.initMu.Lock()
			c.initSegment = make([]byte, len(initBuf))
			copy(c.initSegment, initBuf)
			c.initMu.Unlock()
		case "moof":
			pendingFrag = append(pendingFrag[:0], boxData...)
		case "mdat":
			if len(pendingFrag) > 0 {
				fragment := make([]byte, len(pendingFrag)+len(boxData))
				copy(fragment, pendingFrag)
				copy(fragment[len(pendingFrag):], boxData)
				pendingFrag = pendingFrag[:0]
				c.emitChunk(seq, fragment)
				seq++
				emitted++
			}
		default:
			if len(pendingFrag) > 0 {
				pendingFrag = append(pendingFrag, boxData...)
			}
		}
	}
	return emitted
}

func (c *Chunker) emitChunk(seq uint64, rawChunk []byte) {
	hash := sha256.Sum256(rawChunk)
	checksumHex := hex.EncodeToString(hash[:])

	payload := rawChunk
	isEncrypted := false

	if c.IsPaidTier && len(c.AESMasterKey) > 0 {
		encPayload, err := encryptAESGCM(c.AESMasterKey, rawChunk)
		if err == nil {
			payload = encPayload
			isEncrypted = true
		} else {
			log.Printf("[CHUNKER-%s] Encryption failed: %v", c.ChannelID, err)
		}
	}

	header := &protocol.ChunkHeader{
		ChannelID:      c.ChannelID,
		Sequence:       seq,
		Codec:          c.Codec,
		Resolution:     c.Resolution,
		TimestampMs:    time.Now().UnixMilli(),
		DurationMs:     1000,
		IsKeyframe:     true,
		IsEncrypted:    isEncrypted,
		ChecksumSHA256: checksumHex,
		PayloadSize:    uint32(len(payload)),
	}

	msg := &protocol.P2PMessage{
		Type:    protocol.MessageTypeChunkResponse,
		Header:  header,
		Payload: payload,
	}

	c.ringBuffer.Add(msg)

	c.mu.Lock()
	for sub := range c.subscribers {
		select {
		case sub <- msg:
		default:
		}
	}
	c.mu.Unlock()
}

func encryptAESGCM(key []byte, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// DecryptAESGCM decrypts AES-GCM encrypted chunk payloads using the configured master key
func DecryptAESGCM(key []byte, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, io.ErrUnexpectedEOF
	}
	nonce, enc := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, enc, nil)
}
