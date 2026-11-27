package ingester

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

// StreamPipelineOptions configures bitrate control, frame rate preservation, and buffering profile for fMP4 transcoding.
type StreamPipelineOptions struct {
	SmoothProfile     string
	PreserveSourceFPS bool
	VideoBitrate      string
	MaxRate           string
	BufSize           string
}

// StreamPipeline represents a running FFmpeg live ingestion process
type StreamPipeline struct {
	ChannelID     string
	SourceURL     string
	EncoderType   string // hevc_nvenc, hevc_qsv, libx265, libx264
	FallbackCodec string // libx264
	Preset        string
	TargetFPS     int
	GOPSeconds    int
	Options       StreamPipelineOptions

	ctx        context.Context
	cancelFunc context.CancelFunc
	cmd        *exec.Cmd
	stdout     io.ReadCloser
	stderr     io.ReadCloser

	mu        sync.RWMutex
	IsRunning bool
}

func normalizePipelineOptions(opts StreamPipelineOptions) StreamPipelineOptions {
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

// NewStreamPipeline initializes a new FFmpeg ingestion pipeline with VLC-smooth defaults
func NewStreamPipeline(channelID, sourceURL, encoderType, fallbackCodec, preset string, fps, gopSec int) *StreamPipeline {
	return NewStreamPipelineWithOptions(channelID, sourceURL, encoderType, fallbackCodec, preset, fps, gopSec, StreamPipelineOptions{
		SmoothProfile:     "vlc_smooth",
		PreserveSourceFPS: true,
		VideoBitrate:      "4500k",
		MaxRate:           "6500k",
		BufSize:           "13000k",
	})
}

// NewStreamPipelineWithOptions initializes a new FFmpeg ingestion pipeline with explicit smoothness/bitrate options
func NewStreamPipelineWithOptions(channelID, sourceURL, encoderType, fallbackCodec, preset string, fps, gopSec int, opts StreamPipelineOptions) *StreamPipeline {
	ctx, cancel := context.WithCancel(context.Background())
	if fps <= 0 {
		fps = 30
	}
	if gopSec <= 0 {
		gopSec = 2
	}
	if fallbackCodec == "" {
		fallbackCodec = "libx264"
	}
	if preset == "" {
		preset = "veryfast"
	}
	opts = normalizePipelineOptions(opts)
	return &StreamPipeline{
		ChannelID:     channelID,
		SourceURL:     sourceURL,
		EncoderType:   encoderType,
		FallbackCodec: fallbackCodec,
		Preset:        preset,
		TargetFPS:     fps,
		GOPSeconds:    gopSec,
		Options:       opts,
		ctx:           ctx,
		cancelFunc:    cancel,
	}
}

func buildFFmpegArgs(sourceURL, encoder, preset string, fps, gopSec int, opts StreamPipelineOptions) []string {
	opts = normalizePipelineOptions(opts)
	if fps <= 0 {
		fps = 30
	}
	if gopSec <= 0 {
		gopSec = 2
	}
	gopSize := strconv.Itoa(fps * gopSec)

	lowerURL := strings.ToLower(sourceURL)
	isHTTP := strings.HasPrefix(lowerURL, "http://") || strings.HasPrefix(lowerURL, "https://")
	isRTSP := strings.HasPrefix(lowerURL, "rtsp://")
	isAceHTTP := isHTTP && strings.Contains(lowerURL, "/ace/")

	var inputArgs []string
	if isAceHTTP {
		inputArgs = append(inputArgs,
			"-fflags", "+genpts",
			"-thread_queue_size", "1024",
			"-analyzeduration", "8000000",
			"-probesize", "8000000",
			"-rw_timeout", "35000000",
			"-reconnect", "1",
			"-reconnect_streamed", "1",
			"-reconnect_delay_max", "3",
			"-i", sourceURL,
		)
	} else if isHTTP {
		inputArgs = append(inputArgs,
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
		)
	} else if isRTSP {
		inputArgs = append(inputArgs,
			"-fflags", "+genpts",
			"-thread_queue_size", "1024",
			"-rtsp_transport", "tcp",
			"-i", sourceURL,
		)
	} else {
		inputArgs = append(inputArgs, "-re", "-thread_queue_size", "1024", "-i", sourceURL)
	}

	if encoder == "" {
		encoder = "libx264"
	}

	effectivePreset := preset
	isSoftware := encoder == "libx264" || encoder == "libx265"
	isNVENC := strings.Contains(encoder, "nvenc")
	isQSV := strings.Contains(encoder, "qsv")

	if isSoftware {
		// NVENC/QSV presets like llhq/p1 are invalid for software x264/x265;
		// avoid ultrafast because disabling CABAC/deblocking causes severe motion pixelation.
		if effectivePreset == "" || effectivePreset == "llhq" || effectivePreset == "ultrafast" || strings.HasPrefix(effectivePreset, "p") {
			effectivePreset = "veryfast"
		}
	}

	args := append(inputArgs,
		"-c:v", encoder,
		"-preset", effectivePreset,
		"-pix_fmt", "yuv420p",
	)

	if isNVENC {
		args = append(args,
			"-rc", "vbr",
			"-cq", "22",
			"-b:v", opts.VideoBitrate,
			"-maxrate", opts.MaxRate,
			"-bufsize", opts.BufSize,
		)
	} else if isQSV {
		args = append(args,
			"-global_quality", "22",
			"-b:v", opts.VideoBitrate,
			"-maxrate", opts.MaxRate,
			"-bufsize", opts.BufSize,
		)
	} else {
		args = append(args,
			"-tune", "zerolatency",
			"-crf", "21",
			"-b:v", opts.VideoBitrate,
			"-maxrate", opts.MaxRate,
			"-bufsize", opts.BufSize,
		)
	}

	args = append(args,
		"-g", gopSize,
		"-keyint_min", gopSize,
		"-sc_threshold", "0",
	)
	if !opts.PreserveSourceFPS && fps > 0 {
		args = append(args, "-r", strconv.Itoa(fps))
	}

	args = append(args,
		"-c:a", "aac",
		"-ac", "2",
		"-ar", "44100",
		"-b:a", "128k",
		"-af", "aresample=async=1:min_hard_comp=0.100000:first_pts=0",
		"-avoid_negative_ts", "make_zero",
		"-max_interleave_delta", "0",
	)
	if strings.Contains(encoder, "hevc") || strings.Contains(encoder, "265") {
		args = append(args, "-tag:v", "hvc1")
	}
	args = append(args,
		"-f", "mp4",
		"-movflags", "frag_keyframe+empty_moov+default_base_moof+omit_tfhd_offset",
		"pipe:1",
	)
	return args
}

// Start launches the FFmpeg process with strict GOP alignment for fMP4 output
func (p *StreamPipeline) Start() (io.Reader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.SourceURL == "" {
		return nil, fmt.Errorf("empty source URL for channel %s", p.ChannelID)
	}

	ffmpegBin := EnsureFFmpeg()
	encoder := p.EncoderType
	if encoder == "" {
		encoder = p.FallbackCodec
	}

	args := buildFFmpegArgs(p.SourceURL, encoder, p.Preset, p.TargetFPS, p.GOPSeconds, p.Options)
	cmd := exec.CommandContext(p.ctx, ffmpegBin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil && encoder != p.FallbackCodec {
		log.Printf("[FFMPEG-%s] Encoder %s failed (%v), falling back to %s...", p.ChannelID, encoder, err, p.FallbackCodec)
		encoder = p.FallbackCodec
		args = buildFFmpegArgs(p.SourceURL, encoder, "veryfast", p.TargetFPS, p.GOPSeconds, p.Options)
		cmd = exec.CommandContext(p.ctx, ffmpegBin, args...)
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		stderr, err = cmd.StderrPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("ffmpeg fallback start failed: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("ffmpeg start failed: %w", err)
	}

	p.cmd = cmd
	p.stdout = stdout
	p.stderr = stderr
	p.IsRunning = true

	go p.monitorStderr()

	log.Printf("[INGESTER] Started FFmpeg stream pipeline for channel %s (Source: %s, Codec: %s, Bitrate: %s)", p.ChannelID, p.SourceURL, encoder, p.Options.VideoBitrate)
	return stdout, nil
}

func (p *StreamPipeline) monitorStderr() {
	if p.stderr == nil {
		return
	}
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(strings.ToLower(line), "error") {
			log.Printf("[FFmpeg-%s] %s", p.ChannelID, line)
		}
	}
}

// Stop terminates the FFmpeg process gracefully
func (p *StreamPipeline) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cancelFunc()
	if p.IsRunning {
		if p.cmd != nil && p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
		p.IsRunning = false
		log.Printf("[INGESTER] Stopped stream pipeline for channel %s", p.ChannelID)
	}
}
