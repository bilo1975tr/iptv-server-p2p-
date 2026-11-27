package protocol

import (
	"encoding/json"
	"fmt"
	"github.com/klauspost/compress/zstd"
)

// Message Types
const (
	MessageTypeSignalingOffer  = "OFFER"
	MessageTypeSignalingAnswer = "ANSWER"
	MessageTypeICECandidate    = "ICE_CANDIDATE"
	MessageTypeChunkRequest    = "CHUNK_REQUEST"
	MessageTypeChunkResponse   = "CHUNK_RESPONSE"
	MessageTypeHaveBitfield    = "HAVE_BITFIELD"
	MessageTypeP2PStats        = "P2P_STATS"
)

// PeerInfo describes a swarm participant with server-verified tier
type PeerInfo struct {
	ID          string `json:"id"`
	Tier        string `json:"tier"` // guest, free, paid, admin
	IsIPv6      bool   `json:"is_ipv6,omitempty"`
	IsTailscale bool   `json:"is_tailscale,omitempty"`
}

// P2PStatsReport carries real byte counters from a connected P2P client
type P2PStatsReport struct {
	P2PBytesIn      int64 `json:"p2p_bytes_in"`
	HTTPBytesIn     int64 `json:"http_bytes_in"`
	P2POutFreeBytes int64 `json:"p2p_out_free_bytes"`
	P2POutPaidBytes int64 `json:"p2p_out_paid_bytes"`
}

// Codec Constants
const (
	CodecH265 = "HEVC"
	CodecH264 = "AVC"
)

// ChunkHeader holds metadata for a 1-second video segment
type ChunkHeader struct {
	ChannelID      string `json:"channel_id"`
	Sequence       uint64 `json:"seq"`
	Codec          string `json:"codec"`           // HEVC or AVC
	Resolution     string `json:"resolution"`      // 1080p, 720p, 480p
	TimestampMs    int64  `json:"timestamp_ms"`    // Unix millisecond timestamp
	DurationMs     uint32 `json:"duration_ms"`     // Default ~1000ms
	IsKeyframe     bool   `json:"is_keyframe"`     // Always true for chunk start
	IsEncrypted    bool   `json:"is_encrypted"`    // True for paid channel tier
	ChecksumSHA256 string `json:"checksum_sha256"` // Payload verification SHA-256
	PayloadSize    uint32 `json:"payload_size"`    // Compressed video bytes count
}

// P2PMessage represents a compressed P2P packet transferred via WebRTC DataChannel or WS signaling
type P2PMessage struct {
	Type       string          `json:"type"`
	SenderID   string          `json:"sender_id"`
	TargetID   string          `json:"target_id,omitempty"`
	SelfTier   string          `json:"self_tier,omitempty"`
	Header     *ChunkHeader    `json:"header,omitempty"`
	Payload    []byte          `json:"payload,omitempty"`   // Video chunk bytes (fMP4)
	Bitfield   []uint64        `json:"bitfield,omitempty"`  // Sequence numbers available at peer
	Peers      []string        `json:"peers,omitempty"`     // Active peer IDs in swarm
	PeerInfos  []PeerInfo      `json:"peer_infos,omitempty"` // Peers with server-verified tiers
	TotalPeers int             `json:"total_peers,omitempty"`
	Stats      *P2PStatsReport `json:"stats,omitempty"`
	SDP        string          `json:"sdp,omitempty"`       // WebRTC SDP
	Candidate  string          `json:"candidate,omitempty"` // WebRTC ICE Candidate
}

// Global Zstd Encoder/Decoder instances for high speed
var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize Zstd encoder: %v", err))
	}

	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize Zstd decoder: %v", err))
	}
}

// EncodeCompressed encodes a P2PMessage to JSON and compresses it with Zstandard
func EncodeCompressed(msg *P2PMessage) ([]byte, error) {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal json failed: %w", err)
	}
	compressed := zstdEncoder.EncodeAll(jsonData, make([]byte, 0, len(jsonData)))
	return compressed, nil
}

// DecodeCompressed decompresses Zstandard data and unmarshals it into P2PMessage
func DecodeCompressed(compressedData []byte) (*P2PMessage, error) {
	decompressed, err := zstdDecoder.DecodeAll(compressedData, nil)
	if err != nil {
		// Fallback: If not zstd compressed (e.g. raw JSON from web client), attempt direct json decode
		var rawMsg P2PMessage
		if jsonErr := json.Unmarshal(compressedData, &rawMsg); jsonErr == nil {
			return &rawMsg, nil
		}
		return nil, fmt.Errorf("zstd decode failed: %w", err)
	}

	var msg P2PMessage
	if err := json.Unmarshal(decompressed, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal msg failed: %w", err)
	}
	return &msg, nil
}
