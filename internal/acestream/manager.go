package acestream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Enabled           bool   `yaml:"enabled" json:"enabled"`
	EnginePath        string `yaml:"engine_path" json:"engine_path"`
	APIHost           string `yaml:"api_host" json:"api_host"`
	BaseHTTPPort      int    `yaml:"base_http_port" json:"base_http_port"`
	BaseP2PPort       int    `yaml:"base_p2p_port" json:"base_p2p_port"`
	MaxInstances      int    `yaml:"max_instances" json:"max_instances"`
	MultiPort         bool   `yaml:"multi_port" json:"multi_port"`
	StateBaseDir      string `yaml:"state_base_dir" json:"state_base_dir"`
	StartupTimeoutSec int    `yaml:"startup_timeout_sec" json:"startup_timeout_sec"`
	StreamMode        string `yaml:"stream_mode" json:"stream_mode"` // ffmpeg_fmp4 or hls_first
}

type EngineSlot struct {
	SlotIndex   int        `json:"slot_index"`
	HTTPPort    int        `json:"http_port"`
	P2PPort     int        `json:"p2p_port"`
	StateDir    string     `json:"state_dir"`
	Running     bool       `json:"running"`
	ManagedByUs bool       `json:"managed_by_us"`
	Busy        bool       `json:"busy"`
	ChannelID   string     `json:"channel_id,omitempty"`
	SourceRef   string     `json:"source_ref,omitempty"`
	PID         string     `json:"pid,omitempty"`
	PlaybackURL string     `json:"playback_url,omitempty"`
	StatURL     string     `json:"stat_url,omitempty"`
	CommandURL  string     `json:"command_url,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	cmd         *exec.Cmd
}

type ActiveSession struct {
	ChannelID   string `json:"channel_id"`
	SlotIndex   int    `json:"slot_index"`
	HTTPPort    int    `json:"http_port"`
	PID         string `json:"pid"`
	PlaybackURL string `json:"playback_url"`
	StatURL     string `json:"stat_url"`
	CommandURL  string `json:"command_url"`
	IsHLS       bool   `json:"is_hls"`
}

type SessionStats struct {
	Status    string `json:"status"`
	Peers     int    `json:"peers"`
	SpeedDown int    `json:"speed_down"`
	SpeedUp   int    `json:"speed_up"`
	Progress  int    `json:"progress"`
}

type SessionStatusView struct {
	ChannelID string        `json:"channel_id"`
	SlotIndex int           `json:"slot_index"`
	HTTPPort  int           `json:"http_port"`
	PID       string        `json:"pid"`
	IsHLS     bool          `json:"is_hls"`
	Stats     *SessionStats `json:"stats,omitempty"`
}

type SlotStatusView struct {
	SlotIndex   int           `json:"slot_index"`
	HTTPPort    int           `json:"http_port"`
	P2PPort     int           `json:"p2p_port"`
	Running     bool          `json:"running"`
	ManagedByUs bool          `json:"managed_by_us"`
	Busy        bool          `json:"busy"`
	ChannelID   string        `json:"channel_id,omitempty"`
	PID         string        `json:"pid,omitempty"`
	Stats       *SessionStats `json:"stats,omitempty"`
}

type EngineStatus struct {
	Enabled        bool                `json:"enabled"`
	EnginePath     string              `json:"engine_path"`
	BinaryExists   bool                `json:"binary_exists"`
	APIHost        string              `json:"api_host"`
	MultiPort      bool                `json:"multi_port"`
	StreamMode     string              `json:"stream_mode"`
	PrimaryReady   bool                `json:"primary_ready"`
	Version        string              `json:"version,omitempty"`
	ActiveCount    int                 `json:"active_count"`
	Slots          []SlotStatusView    `json:"slots"`
	ActiveSessions []SessionStatusView `json:"active_sessions"`
}

type Manager struct {
	cfg        Config
	httpClient *http.Client

	mu       sync.Mutex
	slots    []*EngineSlot
	sessions map[string]*ActiveSession // channelID -> session
}

func NewManager(cfg Config) *Manager {
	if cfg.APIHost == "" {
		cfg.APIHost = "127.0.0.1"
	}
	if cfg.BaseHTTPPort <= 0 {
		cfg.BaseHTTPPort = 6878
	}
	if cfg.BaseP2PPort <= 0 {
		cfg.BaseP2PPort = 8621
	}
	if !cfg.MultiPort {
		cfg.MaxInstances = 1
	} else if cfg.MaxInstances <= 0 {
		cfg.MaxInstances = 6
	}
	if cfg.StateBaseDir == "" {
		cfg.StateBaseDir = filepath.Join("data", "acestream_states")
	}
	if cfg.StartupTimeoutSec <= 0 {
		cfg.StartupTimeoutSec = 20
	}
	if cfg.StreamMode == "" {
		cfg.StreamMode = "ffmpeg_fmp4"
	}

	slots := make([]*EngineSlot, cfg.MaxInstances)
	for i := 0; i < cfg.MaxInstances; i++ {
		slots[i] = &EngineSlot{
			SlotIndex: i,
			HTTPPort:  cfg.BaseHTTPPort + i,
			P2PPort:   cfg.BaseP2PPort + i,
			StateDir:  filepath.Join(cfg.StateBaseDir, fmt.Sprintf("slot_%d", i)),
		}
	}

	return &Manager{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 35 * time.Second,
		},
		slots:    slots,
		sessions: make(map[string]*ActiveSession),
	}
}

// IsAceStreamURL returns true for acestream:// URLs or 40-char hex content IDs.
func IsAceStreamURL(raw string) bool {
	s := strings.TrimSpace(raw)
	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "acestream://") {
		return true
	}
	if len(s) == 40 && isHexString(s) {
		return true
	}
	return false
}

func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(s) > 0
}

// ParseSource extracts query parameter type ("id", "infohash", or "url") and value from an AceStream reference.
func ParseSource(raw string) (paramKey, paramVal string, err error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", "", fmt.Errorf("empty acestream source")
	}
	if len(s) == 40 && isHexString(s) {
		return "id", s, nil
	}
	lower := strings.ToLower(s)
	if !strings.HasPrefix(lower, "acestream://") {
		return "", "", fmt.Errorf("not an acestream URL: %s", raw)
	}
	rest := strings.TrimSpace(s[len("acestream://"):])
	rest = strings.TrimPrefix(rest, "/")
	if strings.HasPrefix(strings.ToLower(rest), "infohash/") {
		hash := strings.TrimPrefix(rest[len("infohash/"):], "/")
		if hash == "" {
			return "", "", fmt.Errorf("empty infohash in acestream URL")
		}
		return "infohash", hash, nil
	}
	if strings.HasPrefix(strings.ToLower(rest), "infohash:") {
		hash := strings.TrimSpace(rest[len("infohash:"):])
		if hash == "" {
			return "", "", fmt.Errorf("empty infohash in acestream URL")
		}
		return "infohash", hash, nil
	}
	if strings.HasPrefix(strings.ToLower(rest), "http://") || strings.HasPrefix(strings.ToLower(rest), "https://") {
		return "url", rest, nil
	}
	if idx := strings.IndexAny(rest, "?/"); idx != -1 {
		rest = rest[:idx]
	}
	if rest == "" {
		return "", "", fmt.Errorf("missing content ID in acestream URL")
	}
	return "id", rest, nil
}

func (m *Manager) GetStreamMode() string {
	return m.cfg.StreamMode
}

func (m *Manager) IsEnabled() bool {
	return m.cfg.Enabled
}

func (m *Manager) checkVersion(port int) (string, bool) {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	u := fmt.Sprintf("http://%s:%d/webui/api/service?method=get_version", m.cfg.APIHost, port)
	resp, err := client.Get(u)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false
	}
	var payload struct {
		Result struct {
			Version string `json:"version"`
		} `json:"result"`
		Error interface{} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", true
	}
	return payload.Result.Version, true
}

// EnsurePrimaryRunning ensures slot 0 (base_http_port, e.g. 6878) is running for search and playback.
func (m *Manager) EnsurePrimaryRunning() error {
	if !m.cfg.Enabled {
		return fmt.Errorf("acestream is disabled in config.yaml")
	}
	m.mu.Lock()
	slot := m.slots[0]
	m.mu.Unlock()
	return m.ensureSlotRunning(slot)
}

func (m *Manager) ensureSlotRunning(slot *EngineSlot) error {
	if _, ok := m.checkVersion(slot.HTTPPort); ok {
		m.mu.Lock()
		slot.Running = true
		m.mu.Unlock()
		return nil
	}

	if m.cfg.EnginePath == "" {
		return fmt.Errorf("acestream engine_path is not configured")
	}
	if _, err := os.Stat(m.cfg.EnginePath); err != nil {
		return fmt.Errorf("ace_engine.exe not found at %s: %w", m.cfg.EnginePath, err)
	}

	baseExe := strings.ToLower(filepath.Base(m.cfg.EnginePath))
	isConsoleExe := strings.Contains(baseExe, "console")

	var args []string
	if isConsoleExe {
		// Only ace_console.exe (or Linux acestreamengine) accepts --client-console; ace_engine.exe injects --client-wx internally!
		args = append(args, "--client-console")
	}

	if m.cfg.MultiPort && slot.SlotIndex > 0 {
		absStateDir, err := filepath.Abs(slot.StateDir)
		if err != nil {
			absStateDir = slot.StateDir
		}
		absCacheDir := filepath.Join(absStateDir, "cache")
		_ = os.MkdirAll(absCacheDir, 0755)
		args = append(args,
			"--http-port", fmt.Sprintf("%d", slot.HTTPPort),
			"--port", fmt.Sprintf("%d", slot.P2PPort),
			"--state-dir", absStateDir,
			"--cache-dir", absCacheDir,
		)
	}

	cmd := exec.Command(m.cfg.EnginePath, args...)
	cmd.Dir = filepath.Dir(m.cfg.EnginePath)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start %s on port %d: %w", baseExe, slot.HTTPPort, err)
	}

	m.mu.Lock()
	slot.cmd = cmd
	slot.ManagedByUs = true
	slot.Running = true
	m.mu.Unlock()

	exitedCh := make(chan error, 1)
	go func(s *EngineSlot, c *exec.Cmd) {
		err := c.Wait()
		exitedCh <- err
		m.mu.Lock()
		if s.cmd == c {
			s.cmd = nil
			s.Running = false
			s.Busy = false
			s.ManagedByUs = false
		}
		m.mu.Unlock()
	}(slot, cmd)

	deadline := time.Now().Add(time.Duration(m.cfg.StartupTimeoutSec) * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.checkVersion(slot.HTTPPort); ok {
			log.Printf("[ACESTREAM] Engine slot #%d ready on http://%s:%d (P2P port %d)", slot.SlotIndex, m.cfg.APIHost, slot.HTTPPort, slot.P2PPort)
			return nil
		}
		select {
		case waitErr := <-exitedCh:
			// Check if another instance is actually already responding on that port (ace_engine single-instance lock)
			if _, ok := m.checkVersion(slot.HTTPPort); ok {
				return nil
			}
			return fmt.Errorf("%s exited prematurely (err=%v); check ace_engine.log", baseExe, waitErr)
		case <-time.After(400 * time.Millisecond):
		}
	}

	return fmt.Errorf("%s started on port %d but API did not become ready within %ds", baseExe, slot.HTTPPort, m.cfg.StartupTimeoutSec)
}

// AcquireStream allocates a free port slot (when multi_port=true) or uses slot 0 with a unique pid (multi_port=false),
// opens the AceStream playback session via format=json, and returns the local HTTP stream URL.
func (m *Manager) AcquireStream(ctx context.Context, channelID, rawSource string, wantHLS bool) (*ActiveSession, error) {
	if !m.cfg.Enabled {
		return nil, fmt.Errorf("acestream is disabled in config.yaml")
	}

	paramKey, paramVal, err := ParseSource(rawSource)
	if err != nil {
		return nil, err
	}

	m.ReleaseStream(channelID)

	m.mu.Lock()
	var chosen *EngineSlot
	if m.cfg.MultiPort {
		for _, s := range m.slots {
			if !s.Busy {
				chosen = s
				s.Busy = true
				s.ChannelID = channelID
				s.SourceRef = rawSource
				now := time.Now()
				s.StartedAt = &now
				break
			}
		}
	}
	if chosen == nil {
		chosen = m.slots[0]
		if !m.cfg.MultiPort {
			chosen.Busy = true
		}
	}
	m.mu.Unlock()

	if err := m.ensureSlotRunning(chosen); err != nil {
		if chosen.SlotIndex != 0 {
			m.freeSlot(chosen, channelID)
			primary := m.slots[0]
			if err0 := m.ensureSlotRunning(primary); err0 == nil {
				chosen = primary
			} else {
				return nil, err
			}
		} else {
			m.freeSlot(chosen, channelID)
			return nil, err
		}
	}

	pid := fmt.Sprintf("p2p_%s_%d", sanitizePID(channelID), time.Now().UnixNano())
	endpoint := "getstream"
	if wantHLS {
		endpoint = "manifest.m3u8"
	}

	q := url.Values{}
	q.Set(paramKey, paramVal)
	q.Set("pid", pid)
	q.Set("format", "json")
	if wantHLS {
		q.Set("transcode_audio", "1")
		q.Set("transcode_ac3", "1")
	}

	reqURL := fmt.Sprintf("http://%s:%d/ace/%s?%s", m.cfg.APIHost, chosen.HTTPPort, endpoint, q.Encode())
	log.Printf("[ACESTREAM-%s] Requesting stream on slot #%d (port %d, pid=%s): %s=%s", channelID, chosen.SlotIndex, chosen.HTTPPort, pid, paramKey, paramVal)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		m.freeSlot(chosen, channelID)
		return nil, err
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		m.freeSlot(chosen, channelID)
		return nil, fmt.Errorf("acestream engine request failed on port %d: %w", chosen.HTTPPort, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		m.freeSlot(chosen, channelID)
		return nil, err
	}

	var aceResp struct {
		Response struct {
			PlaybackURL string `json:"playback_url"`
			StatURL     string `json:"stat_url"`
			CommandURL  string `json:"command_url"`
		} `json:"response"`
		Error *string `json:"error"`
	}

	if jsonErr := json.Unmarshal(body, &aceResp); jsonErr != nil {
		m.freeSlot(chosen, channelID)
		return nil, fmt.Errorf("invalid JSON from acestream engine: %w (body: %s)", jsonErr, string(body))
	}
	if aceResp.Error != nil && *aceResp.Error != "" {
		m.freeSlot(chosen, channelID)
		return nil, fmt.Errorf("acestream engine error: %s", *aceResp.Error)
	}
	if aceResp.Response.PlaybackURL == "" {
		m.freeSlot(chosen, channelID)
		return nil, fmt.Errorf("acestream engine returned empty playback_url")
	}

	playbackURL := normalizeLocalURL(aceResp.Response.PlaybackURL, m.cfg.APIHost, chosen.HTTPPort)
	statURL := normalizeLocalURL(aceResp.Response.StatURL, m.cfg.APIHost, chosen.HTTPPort)
	commandURL := normalizeLocalURL(aceResp.Response.CommandURL, m.cfg.APIHost, chosen.HTTPPort)

	sess := &ActiveSession{
		ChannelID:   channelID,
		SlotIndex:   chosen.SlotIndex,
		HTTPPort:    chosen.HTTPPort,
		PID:         pid,
		PlaybackURL: playbackURL,
		StatURL:     statURL,
		CommandURL:  commandURL,
		IsHLS:       wantHLS,
	}

	m.mu.Lock()
	if m.cfg.MultiPort {
		chosen.PID = pid
		chosen.PlaybackURL = playbackURL
		chosen.StatURL = statURL
		chosen.CommandURL = commandURL
	}
	m.sessions[channelID] = sess
	m.mu.Unlock()

	log.Printf("[ACESTREAM-%s] Ready on port %d (pid=%s) -> %s", channelID, chosen.HTTPPort, pid, playbackURL)
	return sess, nil
}

func (m *Manager) freeSlot(slot *EngineSlot, channelID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.cfg.MultiPort && slot.SlotIndex == 0 {
		slot.Busy = len(m.sessions) > 0
		return
	}
	if slot.ChannelID == channelID {
		slot.Busy = false
		slot.ChannelID = ""
		slot.SourceRef = ""
		slot.PID = ""
		slot.PlaybackURL = ""
		slot.StatURL = ""
		slot.CommandURL = ""
		slot.StartedAt = nil
	}
}

// ReleaseStream sends method=stop to the AceStream command_url and frees the port slot.
func (m *Manager) ReleaseStream(channelID string) {
	m.mu.Lock()
	sess, ok := m.sessions[channelID]
	if ok {
		delete(m.sessions, channelID)
	}
	var matchingSlot *EngineSlot
	for _, s := range m.slots {
		if s.ChannelID == channelID {
			matchingSlot = s
			break
		}
	}
	if !m.cfg.MultiPort && len(m.slots) > 0 {
		m.slots[0].Busy = len(m.sessions) > 0
	}
	m.mu.Unlock()

	if ok && sess.CommandURL != "" {
		stopURL := sess.CommandURL
		if strings.Contains(stopURL, "?") {
			stopURL += "&method=stop"
		} else {
			stopURL += "?method=stop"
		}
		client := &http.Client{Timeout: 3 * time.Second}
		if resp, err := client.Get(stopURL); err == nil {
			_ = resp.Body.Close()
		}
		log.Printf("[ACESTREAM-%s] Stopped session on port %d (pid=%s)", channelID, sess.HTTPPort, sess.PID)
	}

	if matchingSlot != nil {
		m.freeSlot(matchingSlot, channelID)
	}
}

// GetSession returns the currently active AceStream session for a channel, if any.
func (m *Manager) GetSession(channelID string) (*ActiveSession, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.sessions[channelID]
	return sess, ok
}

// StopAll stops all active AceStream sessions and terminates engine processes started by this server.
func (m *Manager) StopAll() {
	m.mu.Lock()
	chIDs := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		chIDs = append(chIDs, id)
	}
	m.mu.Unlock()

	for _, id := range chIDs {
		m.ReleaseStream(id)
	}

	m.mu.Lock()
	for _, s := range m.slots {
		if s.ManagedByUs && s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
			s.cmd = nil
			s.Running = false
			s.ManagedByUs = false
			s.Busy = false
		}
	}
	m.mu.Unlock()
}

// GetStatus returns engine availability, version, active concurrent sessions, and per-slot statistics.
func (m *Manager) GetStatus() EngineStatus {
	_, statErr := os.Stat(m.cfg.EnginePath)
	version, primaryReady := m.checkVersion(m.cfg.BaseHTTPPort)

	m.mu.Lock()
	slotsCopy := make([]EngineSlot, len(m.slots))
	for i, s := range m.slots {
		slotsCopy[i] = *s
	}
	sessCopy := make([]ActiveSession, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessCopy = append(sessCopy, *sess)
	}
	m.mu.Unlock()

	sessionViews := make([]SessionStatusView, 0, len(sessCopy))
	for _, sess := range sessCopy {
		var st *SessionStats
		if sess.StatURL != "" {
			st = m.fetchSessionStats(sess.StatURL)
		}
		sessionViews = append(sessionViews, SessionStatusView{
			ChannelID: sess.ChannelID,
			SlotIndex: sess.SlotIndex,
			HTTPPort:  sess.HTTPPort,
			PID:       sess.PID,
			IsHLS:     sess.IsHLS,
			Stats:     st,
		})
	}

	views := make([]SlotStatusView, len(slotsCopy))
	for i, s := range slotsCopy {
		running := s.Running
		if i == 0 {
			running = primaryReady
		} else if !running {
			_, running = m.checkVersion(s.HTTPPort)
		}
		view := SlotStatusView{
			SlotIndex:   s.SlotIndex,
			HTTPPort:    s.HTTPPort,
			P2PPort:     s.P2PPort,
			Running:     running,
			ManagedByUs: s.ManagedByUs,
			Busy:        s.Busy,
			ChannelID:   s.ChannelID,
			PID:         s.PID,
		}
		if s.Busy && s.StatURL != "" {
			view.Stats = m.fetchSessionStats(s.StatURL)
		}
		views[i] = view
	}

	return EngineStatus{
		Enabled:        m.cfg.Enabled,
		EnginePath:     m.cfg.EnginePath,
		BinaryExists:   statErr == nil,
		APIHost:        m.cfg.APIHost,
		MultiPort:      m.cfg.MultiPort,
		StreamMode:     m.cfg.StreamMode,
		PrimaryReady:   primaryReady,
		Version:        version,
		ActiveCount:    len(sessionViews),
		Slots:          views,
		ActiveSessions: sessionViews,
	}
}

func (m *Manager) fetchSessionStats(statURL string) *SessionStats {
	client := &http.Client{Timeout: 1200 * time.Millisecond}
	resp, err := client.Get(statURL)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var payload struct {
		Response *SessionStats `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil
	}
	return payload.Response
}

func normalizeLocalURL(raw, fallbackHost string, port int) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	if strings.HasPrefix(u.Host, "0.0.0.0") || strings.HasPrefix(u.Host, "127.0.0.1") || strings.HasPrefix(u.Host, "localhost") {
		u.Host = fmt.Sprintf("%s:%d", fallbackHost, port)
	}
	return u.String()
}

func sanitizePID(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "ch"
	}
	return b.String()
}
