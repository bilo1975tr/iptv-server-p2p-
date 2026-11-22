package seeder

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"iptv-p2p-server/internal/acestream"
	"iptv-p2p-server/internal/auth"
	"iptv-p2p-server/internal/ingester"
	"iptv-p2p-server/internal/packager"
)

type ChannelConfig struct {
	ID         string
	Tier       string
	SourceURLs []string
}

type activeStream struct {
	chunker    *packager.Chunker
	pipeline   *ingester.StreamPipeline
	lastAccess time.Time
}

type SeedServer struct {
	authManager     *auth.AuthManager
	aceManager      *acestream.Manager
	masterKey       string
	encoderType     string
	fallbackEncoder string
	preset          string
	targetFPS       int
	gopSeconds      int
	proxySecret     []byte
	httpClient      *http.Client

	mu       sync.RWMutex
	configs  map[string]ChannelConfig
	active   map[string]*activeStream
	viewers  map[string]map[string]time.Time // channelID -> viewerKey -> lastSeen
}

func NewSeedServer(authMgr *auth.AuthManager, masterKey, encoderType, fallbackEncoder, preset string, targetFPS, gopSeconds int, proxySecret string) *SeedServer {
	s := &SeedServer{
		authManager:     authMgr,
		masterKey:       masterKey,
		encoderType:     encoderType,
		fallbackEncoder: fallbackEncoder,
		preset:          preset,
		targetFPS:       targetFPS,
		gopSeconds:      gopSeconds,
		proxySecret:     []byte(proxySecret),
		httpClient: &http.Client{
			Timeout: 12 * time.Second,
		},
		configs: make(map[string]ChannelConfig),
		active:  make(map[string]*activeStream),
		viewers: make(map[string]map[string]time.Time),
	}
	go s.idleReaperLoop()
	return s
}

func viewerKeyFromRequest(r *http.Request) string {
	if vid := strings.TrimSpace(r.URL.Query().Get("vid")); vid != "" && len(vid) <= 64 {
		return "v:" + vid
	}
	host := r.RemoteAddr
	if idx := strings.LastIndex(host, ":"); idx > 0 && !strings.Contains(host[idx:], "]") {
		host = host[:idx]
	}
	return "ip:" + host
}

// RecordViewer records an active playback heartbeat for a channel
func (s *SeedServer) RecordViewer(channelID, viewerKey string) {
	if channelID == "" || viewerKey == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.viewers[channelID] == nil {
		s.viewers[channelID] = make(map[string]time.Time)
	}
	s.viewers[channelID][viewerKey] = time.Now()
}

// RemoveViewer removes a viewer immediately when they switch away or close the player
func (s *SeedServer) RemoveViewer(channelID, viewerKey string) {
	if channelID == "" || viewerKey == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.viewers[channelID]; m != nil {
		delete(m, viewerKey)
		if len(m) == 0 {
			delete(s.viewers, channelID)
		}
	}
}

// GetActiveViewers returns active viewer counts per channel within a 25-second sliding window
func (s *SeedServer) GetActiveViewers() map[string]int {
	cutoff := time.Now().Add(-25 * time.Second)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int)
	for chID, m := range s.viewers {
		for vk, ts := range m {
			if ts.Before(cutoff) {
				delete(m, vk)
			}
		}
		if len(m) == 0 {
			delete(s.viewers, chID)
			continue
		}
		out[chID] = len(m)
	}
	return out
}

func (s *SeedServer) SetAceStreamManager(mgr *acestream.Manager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aceManager = mgr
}

// RegisterChannelConfig saves channel source info for on-demand streaming and proxy playback
func (s *SeedServer) RegisterChannelConfig(channelID, tier string, sourceURLs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tier == "" {
		tier = auth.TierFree
	}
	cleaned := make([]string, 0, len(sourceURLs))
	for _, u := range sourceURLs {
		u = strings.TrimSpace(u)
		if u != "" {
			cleaned = append(cleaned, u)
		}
	}
	s.configs[channelID] = ChannelConfig{
		ID:         channelID,
		Tier:       tier,
		SourceURLs: cleaned,
	}
}

// StopChannel stops an active FFmpeg pipeline and releases its chunker
func (s *SeedServer) StopChannel(channelID string) {
	s.mu.Lock()
	act, exists := s.active[channelID]
	if exists {
		delete(s.active, channelID)
	}
	s.mu.Unlock()

	if exists && act != nil {
		if act.pipeline != nil {
			act.pipeline.Stop()
		}
		if act.chunker != nil {
			act.chunker.Close()
		}
	}
	if s.aceManager != nil {
		s.aceManager.ReleaseStream(channelID)
	}
}

// UnregisterChannel stops any running pipeline and deletes channel metadata
func (s *SeedServer) UnregisterChannel(channelID string) {
	s.StopChannel(channelID)
	s.mu.Lock()
	delete(s.configs, channelID)
	s.mu.Unlock()
}

// GetChannelTier returns the tier of a registered channel
func (s *SeedServer) GetChannelTier(channelID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cfg, ok := s.configs[channelID]; ok {
		return cfg.Tier
	}
	return ""
}

// GetActiveChannelIDs returns channel IDs currently running FFmpeg transcoding
func (s *SeedServer) GetActiveChannelIDs() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]bool, len(s.active))
	for id := range s.active {
		res[id] = true
	}
	return res
}

func (s *SeedServer) idleReaperLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var toStop []string
		now := time.Now()

		s.mu.RLock()
		for id, act := range s.active {
			if now.Sub(act.lastAccess) > 90*time.Second {
				toStop = append(toStop, id)
			}
		}
		s.mu.RUnlock()

		for _, id := range toStop {
			log.Printf("[SEEDER] Stopping idle FFmpeg pipeline for channel %s (no viewers for 90s)", id)
			s.StopChannel(id)
		}
	}
}

func (s *SeedServer) ensureActiveStream(channelID string) (*activeStream, ChannelConfig, bool) {
	s.mu.Lock()
	cfg, cfgExists := s.configs[channelID]
	if !cfgExists {
		s.mu.Unlock()
		return nil, ChannelConfig{}, false
	}

	if act, ok := s.active[channelID]; ok {
		act.lastAccess = time.Now()
		s.mu.Unlock()
		return act, cfg, true
	}

	if len(cfg.SourceURLs) == 0 {
		s.mu.Unlock()
		return nil, cfg, false
	}

	isPaid := cfg.Tier == auth.TierPaid
	codec := "AVC"
	if strings.Contains(s.encoderType, "hevc") || strings.Contains(s.encoderType, "265") {
		codec = "HEVC"
	}
	chunker, err := packager.NewChunker(channelID, codec, "1080p", isPaid, s.masterKey)
	if err != nil {
		s.mu.Unlock()
		log.Printf("⚠️ Failed to create chunker for %s: %v", channelID, err)
		return nil, cfg, false
	}

	act := &activeStream{
		chunker:    chunker,
		lastAccess: time.Now(),
	}
	s.active[channelID] = act
	aceMgr := s.aceManager
	s.mu.Unlock()

	go func(chID string, urls []string, c *packager.Chunker, expectedAct *activeStream) {
		defer func() {
			c.Close()
			if aceMgr != nil {
				aceMgr.ReleaseStream(chID)
			}
			s.mu.Lock()
			if cur, ok := s.active[chID]; ok && cur == expectedAct {
				delete(s.active, chID)
			}
			s.mu.Unlock()
		}()

		for idx, srcURL := range urls {
			s.mu.RLock()
			cur, stillActive := s.active[chID]
			s.mu.RUnlock()
			if !stillActive || cur != expectedAct {
				return
			}

			effectiveURL := srcURL
			isAce := acestream.IsAceStreamURL(srcURL)
			if isAce {
				if aceMgr == nil || !aceMgr.IsEnabled() {
					log.Printf("⚠️ AceStream source #%d skipped for %s (AceStream disabled)", idx+1, chID)
					continue
				}
				sess, aceErr := aceMgr.AcquireStream(context.Background(), chID, srcURL, false)
				if aceErr != nil {
					log.Printf("⚠️ AceStream acquire failed for %s (%s): %v", chID, srcURL, aceErr)
					continue
				}
				effectiveURL = sess.PlaybackURL
			}

			enc := s.encoderType
			preset := s.preset
			p := ingester.NewStreamPipeline(chID, effectiveURL, enc, s.fallbackEncoder, preset, s.targetFPS, s.gopSeconds)
			s.mu.Lock()
			if cur, ok := s.active[chID]; ok && cur == expectedAct {
				cur.pipeline = p
			} else {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()

			reader, startErr := p.Start()
			emitted := 0
			if startErr != nil {
				log.Printf("⚠️ FFmpeg source #%d failed for %s (%s): %v", idx+1, chID, effectiveURL, startErr)
				if isAce && aceMgr != nil {
					aceMgr.ReleaseStream(chID)
				}
				continue
			}

			emitted = c.ProcessStream(reader)
			p.Stop()

			// If hardware encoder (e.g. hevc_nvenc/hevc_qsv) exited with 0 chunks, retry once with software fallback
			if emitted == 0 && s.fallbackEncoder != "" && enc != s.fallbackEncoder {
				s.mu.RLock()
				cur, ok := s.active[chID]
				s.mu.RUnlock()
				if !ok || cur != expectedAct {
					return
				}
				log.Printf("⚠️ [SEEDER-%s] Encoder %s emitted 0 chunks on %s; retrying with fallback %s", chID, enc, effectiveURL, s.fallbackEncoder)
				if strings.Contains(s.fallbackEncoder, "hevc") || strings.Contains(s.fallbackEncoder, "265") {
					c.SetCodec("HEVC")
				} else {
					c.SetCodec("AVC")
				}

				fbURL := effectiveURL
				if isAce && aceMgr != nil {
					if sess, aceErr := aceMgr.AcquireStream(context.Background(), chID, srcURL, false); aceErr == nil {
						fbURL = sess.PlaybackURL
					}
				}

				fbPipe := ingester.NewStreamPipeline(chID, fbURL, s.fallbackEncoder, s.fallbackEncoder, "ultrafast", s.targetFPS, s.gopSeconds)
				s.mu.Lock()
				if cur, ok := s.active[chID]; ok && cur == expectedAct {
					cur.pipeline = fbPipe
				} else {
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()

				if fbReader, fbErr := fbPipe.Start(); fbErr == nil {
					emitted = c.ProcessStream(fbReader)
					fbPipe.Stop()
				}
			}

			if isAce && aceMgr != nil {
				aceMgr.ReleaseStream(chID)
			}

			if emitted > 0 {
				// Stream ran and later ended; check if intentionally stopped before trying next backup URL
				s.mu.RLock()
				cur, ok := s.active[chID]
				s.mu.RUnlock()
				if !ok || cur != expectedAct {
					return
				}
			}
		}
		log.Printf("⚠️ All %d source URLs exhausted for %s", len(urls), chID)
	}(channelID, cfg.SourceURLs, chunker, act)

	return act, cfg, true
}

// ServeHTTP handles /live/:channelID/init.mp4, /live/:channelID/latest, /live/:channelID/chunk/:seq
func (s *SeedServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Expose-Headers", "X-Chunk-Seq, X-Chunk-Codec, X-Chunk-Checksum")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 3 || pathParts[0] != "live" {
		http.Error(w, "Invalid stream URL path", http.StatusBadRequest)
		return
	}

	channelID := pathParts[1]
	endpoint := pathParts[2]

	s.mu.RLock()
	cfg, exists := s.configs[channelID]
	s.mu.RUnlock()
	if !exists {
		http.Error(w, "Channel not found", http.StatusNotFound)
		return
	}

	tokenStr := extractToken(r)
	claims, err := s.authManager.ValidateToken(tokenStr)
	if err != nil || !s.authManager.CanAccessChannel(claims.Tier, cfg.Tier) {
		http.Error(w, "Unauthorized: Subscription required for paid channel", http.StatusForbidden)
		return
	}

	act, _, ok := s.ensureActiveStream(channelID)
	if !ok {
		http.Error(w, "Stream unavailable", http.StatusServiceUnavailable)
		return
	}
	s.RecordViewer(channelID, viewerKeyFromRequest(r))

	if endpoint == "init.mp4" {
		initSeg := act.chunker.GetInitSegment()
		if len(initSeg) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"status":"initializing"}`))
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("X-Chunk-Codec", act.chunker.GetCodec())
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(initSeg)))
		_, _ = w.Write(initSeg)
		return
	}

	ringBuffer := act.chunker.GetRingBuffer()
	var targetSeq uint64

	if endpoint == "latest" {
		targetSeq = ringBuffer.GetLatestSequence()
		if targetSeq == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	} else if endpoint == "chunk" && len(pathParts) >= 4 {
		seq, parseErr := strconv.ParseUint(pathParts[3], 10, 64)
		if parseErr != nil {
			http.Error(w, "Invalid sequence number", http.StatusBadRequest)
			return
		}
		targetSeq = seq
	} else {
		http.Error(w, "Invalid endpoint", http.StatusBadRequest)
		return
	}

	msg, found := ringBuffer.Get(targetSeq)
	if !found {
		latestSeq := ringBuffer.GetLatestSequence()
		// If client is waiting for the very next live chunk, long-poll briefly on chunker subscriber
		if targetSeq > latestSeq && targetSeq <= latestSeq+2 {
			subCh := act.chunker.Subscribe()
			timer := time.NewTimer(1800 * time.Millisecond)
		waitLoop:
			for {
				select {
				case m, ok := <-subCh:
					if !ok {
						break waitLoop
					}
					if m != nil && m.Header != nil && m.Header.Sequence == targetSeq {
						msg = m
						found = true
						break waitLoop
					}
				case <-timer.C:
					break waitLoop
				case <-r.Context().Done():
					break waitLoop
				}
			}
			timer.Stop()
			act.chunker.Unsubscribe(subCh)
		}
		if !found {
			if targetSeq > ringBuffer.GetLatestSequence() {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			http.Error(w, "Chunk evicted from buffer", http.StatusNotFound)
			return
		}
	}

	payload := msg.Payload
	if msg.Header.IsEncrypted && len(act.chunker.AESMasterKey) > 0 {
		if dec, decErr := packager.DecryptAESGCM(act.chunker.AESMasterKey, payload); decErr == nil {
			payload = dec
		}
	}

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("X-Chunk-Seq", fmt.Sprintf("%d", msg.Header.Sequence))
	w.Header().Set("X-Chunk-Codec", msg.Header.Codec)
	w.Header().Set("X-Chunk-Checksum", msg.Header.ChecksumSHA256)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))

	_, _ = w.Write(payload)
}

// ServeHLSProxy handles /api/stream/:channelID/master.m3u8, /api/stream/:channelID/playlist, /api/stream/:channelID/seg
// Provides CORS-free proxying, paid tier enforcement, HMAC-signed URLs (anti-SSRF), and automatic source_urls failover.
func (s *SeedServer) ServeHLSProxy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Expose-Headers", "X-Source-Index")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// Expected: /api/stream/{channelID}/{action}
	if len(pathParts) < 4 || pathParts[0] != "api" || pathParts[1] != "stream" {
		http.Error(w, "Invalid proxy path", http.StatusBadRequest)
		return
	}

	channelID := pathParts[2]
	action := pathParts[3]

	s.mu.RLock()
	cfg, exists := s.configs[channelID]
	s.mu.RUnlock()
	if !exists {
		http.Error(w, "Channel not found", http.StatusNotFound)
		return
	}

	tokenStr := extractToken(r)
	claims, err := s.authManager.ValidateToken(tokenStr)
	if err != nil || !s.authManager.CanAccessChannel(claims.Tier, cfg.Tier) {
		http.Error(w, "Unauthorized: Subscription required for paid channel", http.StatusForbidden)
		return
	}
	s.RecordViewer(channelID, viewerKeyFromRequest(r))

	switch action {
	case "master.m3u8":
		if len(cfg.SourceURLs) == 0 {
			http.Error(w, "No source URLs configured for channel", http.StatusNotFound)
			return
		}
		var lastErr error
		for idx, srcURL := range cfg.SourceURLs {
			if acestream.IsAceStreamURL(srcURL) {
				if s.aceManager == nil || !s.aceManager.IsEnabled() {
					lastErr = fmt.Errorf("acestream is disabled")
					continue
				}
				if s.aceManager.GetStreamMode() == "hls_first" {
					sess, aceErr := s.aceManager.AcquireStream(r.Context(), channelID, srcURL, true)
					if aceErr != nil {
						lastErr = aceErr
						log.Printf("[HLS-PROXY-%s] AceStream HLS failed (%s): %v", channelID, srcURL, aceErr)
						continue
					}
					body, _, finalURL, isPlaylist, fetchErr := s.fetchUpstreamPlaylist(sess.PlaybackURL)
					if fetchErr == nil && isPlaylist {
						rewritten := s.rewriteM3U8(string(body), finalURL, channelID, tokenStr)
						w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
						w.Header().Set("X-Source-Index", fmt.Sprintf("%d", idx))
						_, _ = w.Write([]byte(rewritten))
						return
					}
					s.aceManager.ReleaseStream(channelID)
				}
				// Default ffmpeg_fmp4 mode (or HLS fallback): return JSON mode hint so browser switches cleanly to /live/:channelID/init.mp4 without HTTP error noise
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"transport":"mse_fmp4","init_url":"/live/` + url.PathEscape(channelID) + `/init.mp4"}`))
				return
			}

			body, _, finalURL, isPlaylist, fetchErr := s.fetchUpstreamPlaylist(srcURL)
			if fetchErr != nil {
				lastErr = fetchErr
				log.Printf("[HLS-PROXY-%s] Failover: source #%d failed (%s): %v", channelID, idx+1, srcURL, fetchErr)
				continue
			}

			if isPlaylist {
				rewritten := s.rewriteM3U8(string(body), finalURL, channelID, tokenStr)
				w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
				w.Header().Set("X-Source-Index", fmt.Sprintf("%d", idx))
				_, _ = w.Write([]byte(rewritten))
				return
			}

			// Direct non-HLS live stream (e.g. raw MPEG-TS / RTSP): hand back MSE hint
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"transport":"mse_fmp4","init_url":"/live/` + url.PathEscape(channelID) + `/init.mp4"}`))
			return
		}
		http.Error(w, fmt.Sprintf("All %d upstream sources failed: %v", len(cfg.SourceURLs), lastErr), http.StatusBadGateway)

	case "playlist", "seg":
		targetURL := r.URL.Query().Get("u")
		sig := r.URL.Query().Get("sig")
		if targetURL == "" || !s.verifyProxySig(channelID, targetURL, sig) {
			http.Error(w, "Invalid or unsigned proxy target", http.StatusForbidden)
			return
		}

		req, reqErr := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL, nil)
		if reqErr != nil {
			http.Error(w, "Invalid target URL", http.StatusBadRequest)
			return
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
		if rng := r.Header.Get("Range"); rng != "" {
			req.Header.Set("Range", rng)
		}

		resp, doErr := s.httpClient.Do(req)
		if doErr != nil {
			http.Error(w, "Upstream fetch error", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			http.Error(w, fmt.Sprintf("Upstream status %d", resp.StatusCode), http.StatusBadGateway)
			return
		}

		if action == "playlist" {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
			if readErr != nil {
				http.Error(w, "Failed reading playlist", http.StatusBadGateway)
				return
			}
			rewritten := s.rewriteM3U8(string(body), resp.Request.URL.String(), channelID, tokenStr)
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte(rewritten))
			return
		}

		ct := resp.Header.Get("Content-Type")
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		} else {
			w.Header().Set("Content-Type", "video/mp2t")
		}
		_, _ = io.Copy(w, resp.Body)

	default:
		http.Error(w, "Unknown stream action", http.StatusNotFound)
	}
}

func (s *SeedServer) fetchUpstreamPlaylist(rawURL string) ([]byte, string, string, bool, error) {
	if strings.HasPrefix(strings.ToLower(rawURL), "rtsp://") {
		return nil, "", rawURL, false, nil
	}

	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", rawURL, false, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, "", rawURL, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return nil, "", rawURL, false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	lowerCT := strings.ToLower(ct)
	finalURL := resp.Request.URL.String()

	// If Content-Type explicitly indicates raw video/TS stream, don't read body
	if strings.Contains(lowerCT, "video/mp2t") || strings.Contains(lowerCT, "video/mp4") {
		return nil, ct, finalURL, false, nil
	}

	// Peek first 512 bytes to confirm whether payload is an M3U8 playlist before reading full body
	peekBuf := make([]byte, 512)
	n, _ := io.ReadAtLeast(resp.Body, peekBuf, 1)
	if n == 0 {
		return nil, ct, finalURL, false, fmt.Errorf("empty upstream response")
	}

	head := strings.TrimSpace(string(peekBuf[:n]))
	looksPlaylist := strings.Contains(lowerCT, "mpegurl") ||
		strings.Contains(strings.ToLower(finalURL), ".m3u8") ||
		strings.HasPrefix(head, "#EXTM3U")

	if !looksPlaylist {
		return nil, ct, finalURL, false, nil
	}

	rest, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, ct, finalURL, false, err
	}
	full := make([]byte, n+len(rest))
	copy(full, peekBuf[:n])
	copy(full[n:], rest)
	return full, ct, finalURL, true, nil
}

func (s *SeedServer) signProxyTarget(channelID, targetURL string) string {
	mac := hmac.New(sha256.New, s.proxySecret)
	mac.Write([]byte(channelID + "|" + targetURL))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *SeedServer) verifyProxySig(channelID, targetURL, sig string) bool {
	expected := s.signProxyTarget(channelID, targetURL)
	return hmac.Equal([]byte(expected), []byte(sig))
}

func (s *SeedServer) rewriteM3U8(content, baseURLStr, channelID, tokenStr string) string {
	baseURL, err := url.Parse(baseURLStr)
	if err != nil {
		return content
	}

	var out strings.Builder
	scanner := bufio.NewScanner(strings.NewReader(content))
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			// Rewrite URI="..." inside #EXT-X-MAP or #EXT-X-KEY if present
			if strings.Contains(trimmed, `URI="`) {
				line = s.rewriteTagURI(line, baseURL, channelID, tokenStr)
			}
			out.WriteString(line + "\n")
			continue
		}

		refURL, parseErr := url.Parse(trimmed)
		if parseErr != nil {
			out.WriteString(line + "\n")
			continue
		}
		resolved := baseURL.ResolveReference(refURL).String()
		action := "seg"
		if strings.Contains(strings.ToLower(resolved), ".m3u8") {
			action = "playlist"
		}
		sig := s.signProxyTarget(channelID, resolved)
		proxied := fmt.Sprintf("/api/stream/%s/%s?u=%s&sig=%s", url.PathEscape(channelID), action, url.QueryEscape(resolved), sig)
		if tokenStr != "" {
			proxied += "&token=" + url.QueryEscape(tokenStr)
		}
		out.WriteString(proxied + "\n")
	}

	return out.String()
}

func (s *SeedServer) rewriteTagURI(line string, baseURL *url.URL, channelID, tokenStr string) string {
	idx := strings.Index(line, `URI="`)
	if idx == -1 {
		return line
	}
	start := idx + 5
	end := strings.Index(line[start:], `"`)
	if end == -1 {
		return line
	}
	rawURI := line[start : start+end]
	refURL, err := url.Parse(rawURI)
	if err != nil {
		return line
	}
	resolved := baseURL.ResolveReference(refURL).String()
	action := "seg"
	if strings.Contains(strings.ToLower(resolved), ".m3u8") {
		action = "playlist"
	}
	sig := s.signProxyTarget(channelID, resolved)
	proxied := fmt.Sprintf("/api/stream/%s/%s?u=%s&sig=%s", url.PathEscape(channelID), action, url.QueryEscape(resolved), sig)
	if tokenStr != "" {
		proxied += "&token=" + url.QueryEscape(tokenStr)
	}
	return line[:start] + proxied + line[start+end:]
}

func extractToken(r *http.Request) string {
	if tok := r.URL.Query().Get("token"); tok != "" {
		return tok
	}
	authHdr := r.Header.Get("Authorization")
	if strings.HasPrefix(authHdr, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(authHdr, "Bearer "))
	}
	return ""
}
