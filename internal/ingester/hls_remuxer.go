package ingester

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HLSRemuxerOptions configures live .ts segment duration, buffer window, and fallback encoding quality.
type HLSRemuxerOptions struct {
	SegmentSeconds    int
	ListSize          int
	SmoothProfile     string
	VideoBitrate      string
	MaxRate           string
	BufSize           string
	PreserveSourceFPS bool
}

// LiveHLSRemuxer converts non-HLS continuous streams (raw MPEG-TS, RTSP, AceStream)
// into lightweight live .ts + .m3u8 segments using stream copy (-c:v copy) whenever possible,
// keeping a smooth rolling live edge window on the server while clients hold DVR history.
type LiveHLSRemuxer struct {
	ChannelID       string
	SourceURL       string
	FallbackEncoder string
	TargetFPS       int
	SessionID       string
	Options         HLSRemuxerOptions

	dir        string
	ctx        context.Context
	cancelFunc context.CancelFunc
	cmd        *exec.Cmd

	mu        sync.RWMutex
	IsRunning bool
	stopped   bool
	doneCh    chan struct{}
}

func sanitizeChannelDirName(id string) string {
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	res := b.String()
	if res == "" {
		return "ch"
	}
	return res
}

func normalizeHLSOptions(opts HLSRemuxerOptions) HLSRemuxerOptions {
	if opts.SegmentSeconds <= 0 {
		opts.SegmentSeconds = 3
	}
	if opts.ListSize <= 0 {
		opts.ListSize = 8
	}
	if opts.SmoothProfile == "" {
		opts.SmoothProfile = "vlc_smooth"
	}
	if opts.VideoBitrate == "" {
		opts.VideoBitrate = "4500k"
	}
	if opts.MaxRate == "" {
		opts.MaxRate = "6500k"
	}
	if opts.BufSize == "" {
		opts.BufSize = "13000k"
	}
	return opts
}

// NewLiveHLSRemuxer initializes a live .ts HLS remuxer with VLC-smooth defaults.
func NewLiveHLSRemuxer(channelID, sourceURL, fallbackEncoder string, fps int) *LiveHLSRemuxer {
	return NewLiveHLSRemuxerWithOptions(channelID, sourceURL, fallbackEncoder, fps, HLSRemuxerOptions{
		SegmentSeconds:    3,
		ListSize:          8,
		SmoothProfile:     "vlc_smooth",
		VideoBitrate:      "4500k",
		MaxRate:           "6500k",
		BufSize:           "13000k",
		PreserveSourceFPS: true,
	})
}

// NewLiveHLSRemuxerWithOptions initializes a live .ts HLS remuxer with explicit segment/rate options.
func NewLiveHLSRemuxerWithOptions(channelID, sourceURL, fallbackEncoder string, fps int, opts HLSRemuxerOptions) *LiveHLSRemuxer {
	ctx, cancel := context.WithCancel(context.Background())
	if fps <= 0 {
		fps = 30
	}
	if fallbackEncoder == "" {
		fallbackEncoder = "libx264"
	}
	opts = normalizeHLSOptions(opts)
	sessionID := fmt.Sprintf("%x", time.Now().UnixNano()&0xffffffff)
	dir := filepath.Join(os.TempDir(), "iptv_hls_live", sanitizeChannelDirName(channelID)+"_"+sessionID)
	return &LiveHLSRemuxer{
		ChannelID:       channelID,
		SourceURL:       sourceURL,
		FallbackEncoder: fallbackEncoder,
		TargetFPS:       fps,
		SessionID:       sessionID,
		Options:         opts,
		dir:             dir,
		ctx:             ctx,
		cancelFunc:      cancel,
		doneCh:          make(chan struct{}),
	}
}

func buildHLSInputArgs(sourceURL string) []string {
	lowerURL := strings.ToLower(sourceURL)
	isHTTP := strings.HasPrefix(lowerURL, "http://") || strings.HasPrefix(lowerURL, "https://")
	isRTSP := strings.HasPrefix(lowerURL, "rtsp://")
	isAceHTTP := isHTTP && strings.Contains(lowerURL, "/ace/")

	// Avoid +discardcorrupt so FFmpeg does not drop reference P/B-frames and smear macroblocks;
	// keep +genpts and large input queues for smooth VLC-like buffering.
	if isAceHTTP {
		return []string{
			"-fflags", "+genpts",
			"-thread_queue_size", "1024",
			"-analyzeduration", "8000000",
			"-probesize", "8000000",
			"-rw_timeout", "35000000",
			"-reconnect", "1",
			"-reconnect_streamed", "1",
			"-reconnect_delay_max", "3",
			"-i", sourceURL,
		}
	}
	if isHTTP {
		return []string{
			"-fflags", "+genpts",
			"-thread_queue_size", "1024",
			"-analyzeduration", "5000000",
			"-probesize", "5000000",
			"-user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
			"-rw_timeout", "20000000",
			"-reconnect", "1",
			"-reconnect_streamed", "1",
			"-reconnect_delay_max", "4",
			"-i", sourceURL,
		}
	}
	if isRTSP {
		return []string{
			"-fflags", "+genpts",
			"-thread_queue_size", "1024",
			"-rtsp_transport", "tcp",
			"-i", sourceURL,
		}
	}
	return []string{"-re", "-thread_queue_size", "1024", "-i", sourceURL}
}

func (r *LiveHLSRemuxer) buildArgs(copyVideo bool, startNumber int) []string {
	args := buildHLSInputArgs(r.SourceURL)
	segSec := r.Options.SegmentSeconds
	if segSec <= 0 {
		segSec = 3
	}
	listSize := r.Options.ListSize
	if listSize <= 0 {
		listSize = 8
	}

	if copyVideo {
		args = append(args,
			"-map", "0:v:0?",
			"-map", "0:a:0?",
			"-c:v", "copy",
		)
	} else {
		gopSize := strconv.Itoa(r.TargetFPS * segSec)
		enc := r.FallbackEncoder
		if enc == "" {
			enc = "libx264"
		}
		preset := "veryfast"
		if r.Options.SmoothProfile == "low_latency" {
			preset = "superfast"
		}
		args = append(args,
			"-map", "0:v:0?",
			"-map", "0:a:0?",
			"-c:v", enc,
			"-preset", preset,
			"-tune", "zerolatency",
			"-pix_fmt", "yuv420p",
			"-crf", "21",
			"-b:v", r.Options.VideoBitrate,
			"-maxrate", r.Options.MaxRate,
			"-bufsize", r.Options.BufSize,
			"-g", gopSize,
			"-keyint_min", gopSize,
			"-sc_threshold", "0",
		)
		if !r.Options.PreserveSourceFPS && r.TargetFPS > 0 {
			args = append(args, "-r", strconv.Itoa(r.TargetFPS))
		}
	}

	segPattern := fmt.Sprintf("seg_%s_%%06d.ts", r.SessionID)
	args = append(args,
		"-c:a", "aac",
		"-ac", "2",
		"-ar", "44100",
		"-b:a", "128k",
		"-af", "aresample=async=1:min_hard_comp=0.100000:first_pts=0",
		"-avoid_negative_ts", "make_zero",
		"-max_interleave_delta", "0",
		"-muxdelay", "0",
		"-muxpreload", "0",
		"-f", "hls",
		"-hls_time", strconv.Itoa(segSec),
		"-hls_list_size", strconv.Itoa(listSize),
		"-start_number", strconv.Itoa(startNumber),
		"-hls_flags", "independent_segments+delete_segments+temp_file+omit_endlist",
		"-hls_segment_filename", segPattern,
		"live.m3u8",
	)
	return args
}

func resolveFFmpegPath() string {
	bin := EnsureFFmpeg()
	if strings.ContainsRune(bin, os.PathSeparator) || strings.Contains(bin, "/") || strings.Contains(bin, `\`) {
		if abs, err := filepath.Abs(bin); err == nil {
			return abs
		}
	}
	return bin
}

// Start launches FFmpeg HLS remuxing in a background goroutine (tries zero-transcode -c:v copy first,
// auto-recovers monotonic segment numbers on upstream drops, and falls back to bitrate-controlled encoding if needed).
func (r *LiveHLSRemuxer) Start() error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return fmt.Errorf("remuxer already stopped")
	}
	if err := os.MkdirAll(r.dir, 0755); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("failed to create temp HLS dir: %w", err)
	}
	r.IsRunning = true
	r.mu.Unlock()

	ffmpegBin := resolveFFmpegPath()

	go func() {
		defer func() {
			r.mu.Lock()
			r.IsRunning = false
			r.mu.Unlock()
			close(r.doneCh)
		}()

		copyVideo := true
		startNum := 0

		// Pass 1: Zero-transcode video copy (-c:v copy)
		emitted := r.runPass(ffmpegBin, copyVideo, startNum)
		if r.ctx.Err() != nil {
			return
		}
		if !emitted {
			copyVideo = false
			log.Printf("[HLS-REMUX-%s] Stream copy emitted 0 segments, retrying with fallback encoder %s (%ds HLS .ts, bitrate=%s)...",
				r.ChannelID, r.FallbackEncoder, r.Options.SegmentSeconds, r.Options.VideoBitrate)
			emitted = r.runPass(ffmpegBin, copyVideo, startNum)
		}

		// Auto-reconnect upstream transparently if stream produced segments before dropping
		for retry := 1; retry <= 5 && emitted && r.ctx.Err() == nil; retry++ {
			nextNum := r.nextSegmentNumber()
			log.Printf("[HLS-REMUX-%s] Upstream closed cleanly/dropped; auto-reconnecting pass #%d at seg #%d...", r.ChannelID, retry, nextNum)
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(1200 * time.Millisecond):
			}
			emitted = r.runPass(ffmpegBin, copyVideo, nextNum)
		}
	}()

	return nil
}

func (r *LiveHLSRemuxer) runPass(ffmpegBin string, copyVideo bool, startNumber int) bool {
	args := r.buildArgs(copyVideo, startNumber)
	cmd := exec.CommandContext(r.ctx, ffmpegBin, args...)
	cmd.Dir = r.dir
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return false
	}

	if err := cmd.Start(); err != nil {
		log.Printf("[HLS-REMUX-%s] Start failed (copy=%v): %v", r.ChannelID, copyVideo, err)
		return false
	}

	r.mu.Lock()
	r.cmd = cmd
	r.mu.Unlock()

	go func(rc io.ReadCloser) {
		scanner := bufio.NewScanner(rc)
		for scanner.Scan() {
			line := scanner.Text()
			lower := strings.ToLower(line)
			if strings.Contains(lower, "error") || strings.Contains(lower, "invalid") {
				log.Printf("[HLS-REMUX-%s] %s", r.ChannelID, line)
			}
		}
	}(stderr)

	_ = cmd.Wait()
	return r.hasAnySegment()
}

func (r *LiveHLSRemuxer) nextSegmentNumber() int {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return 0
	}
	prefix := fmt.Sprintf("seg_%s_", r.SessionID)
	maxIdx := -1
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".ts") {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".ts")
		if n, err := strconv.Atoi(numStr); err == nil && n > maxIdx {
			maxIdx = n
		}
	}
	if maxIdx < 0 {
		return 0
	}
	return maxIdx + 1
}

func (r *LiveHLSRemuxer) hasAnySegment() bool {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".ts") {
			return true
		}
	}
	return false
}

// Done returns a channel closed when the remuxer process exits.
func (r *LiveHLSRemuxer) Done() <-chan struct{} {
	return r.doneCh
}

// WaitReady blocks until live.m3u8 and at least one .ts segment are ready, or timeout expires.
func (r *LiveHLSRemuxer) WaitReady(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	playlistPath := filepath.Join(r.dir, "live.m3u8")
	for {
		if data, err := os.ReadFile(playlistPath); err == nil {
			txt := string(data)
			if strings.Contains(txt, "#EXTINF:") && strings.Contains(txt, ".ts") {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-r.doneCh:
			return false
		case <-ctx.Done():
			return false
		case <-r.ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// ReadPlaylist returns the current rolling live.m3u8 generated by FFmpeg.
func (r *LiveHLSRemuxer) ReadPlaylist() (string, error) {
	data, err := os.ReadFile(filepath.Join(r.dir, "live.m3u8"))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadSegment reads a generated .ts segment safely by base filename.
func (r *LiveHLSRemuxer) ReadSegment(name string) ([]byte, error) {
	base := filepath.Base(strings.TrimSpace(name))
	if base == "" || base == "." || base == ".." || !strings.HasSuffix(strings.ToLower(base), ".ts") {
		return nil, fmt.Errorf("invalid segment name")
	}
	if strings.Contains(base, "/") || strings.Contains(base, `\`) {
		return nil, fmt.Errorf("invalid segment path")
	}
	return os.ReadFile(filepath.Join(r.dir, base))
}

// Stop terminates FFmpeg and deletes the temporary live .ts directory.
func (r *LiveHLSRemuxer) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	cmd := r.cmd
	r.IsRunning = false
	r.mu.Unlock()

	r.cancelFunc()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	_ = os.RemoveAll(r.dir)
	log.Printf("[HLS-REMUX-%s] Stopped and cleaned temporary HLS dir", r.ChannelID)
}
