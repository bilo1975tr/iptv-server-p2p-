package mesh

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

var (
	tailscaleCGNAT4 = netip.MustParsePrefix("100.64.0.0/10")
	tailscaleULA6   = netip.MustParsePrefix("fd7a:115c:a1e0::/48")
)

// Config holds YAML / env configuration for embedded Tailscale Mesh (tsnet).
type Config struct {
	Enabled           bool     `yaml:"enabled" json:"enabled"`
	Hostname          string   `yaml:"hostname" json:"hostname"`
	AuthKey           string   `yaml:"auth_key" json:"-"`
	OAuthClientSecret string   `yaml:"oauth_client_secret" json:"-"`
	AdvertiseTags     []string `yaml:"advertise_tags" json:"advertise_tags"`
	StateDir          string   `yaml:"state_dir" json:"state_dir"`
	MeshRole          string   `yaml:"mesh_role" json:"mesh_role"` // origin, seeder, hybrid
	MeshPort          int      `yaml:"mesh_port" json:"mesh_port"`
	MeshSharedSecret  string   `yaml:"mesh_shared_secret" json:"-"`
	DiscoveryTag      string   `yaml:"discovery_tag" json:"discovery_tag"`
	StaticPeers       []string `yaml:"static_peers" json:"static_peers"`
	PreferTailscale   bool     `yaml:"prefer_tailscale_ice" json:"prefer_tailscale_ice"`
}

// PeerInfo summarizes a discovered Tailscale mesh node and its active channels.
type PeerInfo struct {
	Hostname       string   `json:"hostname"`
	DNSName        string   `json:"dns_name,omitempty"`
	TailscaleIP    string   `json:"tailscale_ip"`
	BaseURL        string   `json:"base_url"`
	Role           string   `json:"role"`
	Online         bool     `json:"online"`
	DirectP2P      bool     `json:"direct_p2p"`
	Relay          string   `json:"relay,omitempty"`
	RTTMs          int64    `json:"rtt_ms"`
	ActiveChannels []string `json:"active_channels,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	LastSeenUnix   int64    `json:"last_seen_unix"`
}

// NodeInfoPayload is exchanged on /api/mesh/info between Tailscale Mesh nodes.
type NodeInfoPayload struct {
	Hostname       string   `json:"hostname"`
	Role           string   `json:"role"`
	MeshPort       int      `json:"mesh_port"`
	TailscaleIP    string   `json:"tailscale_ip"`
	ActiveChannels []string `json:"active_channels"`
}

// MeshStatus is the sanitized status returned to Admin API (secrets are never exposed).
type MeshStatus struct {
	Enabled         bool       `json:"enabled"`
	Connected       bool       `json:"connected"`
	BackendState    string     `json:"backend_state"`
	AuthMode        string     `json:"auth_mode"`
	AuthConfigured  bool       `json:"auth_configured"`
	SecretMask      string     `json:"secret_mask,omitempty"`
	Hostname        string     `json:"hostname"`
	MagicDNS        string     `json:"magic_dns,omitempty"`
	TailscaleIPv4   string     `json:"tailscale_ipv4,omitempty"`
	TailscaleIPv6   string     `json:"tailscale_ipv6,omitempty"`
	MeshRole        string     `json:"mesh_role"`
	MeshPort        int        `json:"mesh_port"`
	DiscoveryTag    string     `json:"discovery_tag"`
	PreferTailscale bool       `json:"prefer_tailscale_ice"`
	LastError       string     `json:"last_error,omitempty"`
	BytesPulled     int64      `json:"bytes_pulled"`
	BytesServed     int64      `json:"bytes_served"`
	Peers           []PeerInfo `json:"peers"`
}

// Manager runs the embedded Tailscale tsnet server, peer discovery, and WireGuard HTTP mesh transport.
type Manager struct {
	cfg           Config
	authKey       string
	oauthSecret   string
	sharedSecret  string
	authMode      string
	activeChFn    func() map[string]bool
	meshHandler   http.Handler
	fallbackHTTP  *http.Client

	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.RWMutex
	srv          *tsnet.Server
	tsHTTPClient *http.Client
	meshListener net.Listener
	meshHTTP     *http.Server
	connected    bool
	backendState string
	magicDNS     string
	ipv4         string
	ipv6         string
	lastErr      string
	peers        map[string]PeerInfo // key: tailscaleIP or baseURL

	bytesPulled int64
	bytesServed int64
	started     bool
	stopped     bool
}

// IsTailscaleIP reports whether an IP or host:port belongs to Tailscale's CGNAT (100.64.0.0/10) or ULA IPv6 range.
func IsTailscaleIP(rawAddr string) bool {
	s := strings.TrimSpace(rawAddr)
	if s == "" {
		return false
	}
	if ap, err := netip.ParseAddrPort(s); err == nil {
		a := ap.Addr().Unmap()
		return tailscaleCGNAT4.Contains(a) || tailscaleULA6.Contains(a)
	}
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if a, err := netip.ParseAddr(s); err == nil {
		a = a.Unmap()
		return tailscaleCGNAT4.Contains(a) || tailscaleULA6.Contains(a)
	}
	return false
}

func maskKey(val string) string {
	v := strings.TrimSpace(val)
	if v == "" {
		return ""
	}
	if len(v) <= 10 {
		return "****"
	}
	return v[:8] + "..." + v[len(v)-4:]
}

func normalizeConfig(cfg Config) Config {
	if strings.TrimSpace(cfg.Hostname) == "" {
		cfg.Hostname = "iptv-p2p-node"
	}
	if strings.TrimSpace(cfg.StateDir) == "" {
		cfg.StateDir = filepath.Join("data", "tailscale_state")
	}
	role := strings.ToLower(strings.TrimSpace(cfg.MeshRole))
	if role != "origin" && role != "seeder" && role != "hybrid" {
		role = "hybrid"
	}
	cfg.MeshRole = role
	if cfg.MeshPort <= 0 || cfg.MeshPort > 65535 {
		cfg.MeshPort = 8448
	}
	if strings.TrimSpace(cfg.DiscoveryTag) == "" {
		cfg.DiscoveryTag = "tag:iptv-mesh"
	}
	if len(cfg.AdvertiseTags) == 0 && cfg.DiscoveryTag != "" {
		cfg.AdvertiseTags = []string{cfg.DiscoveryTag}
	}
	return cfg
}

// NewManager initializes a Tailscale Mesh manager without blocking server boot.
func NewManager(cfg Config) *Manager {
	cfg = normalizeConfig(cfg)
	authKey := strings.TrimSpace(cfg.AuthKey)
	if authKey == "" {
		authKey = strings.TrimSpace(os.Getenv("TS_AUTHKEY"))
	}
	oauthSecret := strings.TrimSpace(cfg.OAuthClientSecret)
	if oauthSecret == "" {
		oauthSecret = strings.TrimSpace(os.Getenv("TS_CLIENT_SECRET"))
	}
	sharedSecret := strings.TrimSpace(cfg.MeshSharedSecret)
	if sharedSecret == "" {
		sharedSecret = strings.TrimSpace(os.Getenv("IPTV_MESH_SECRET"))
	}

	authMode := "none"
	if authKey != "" {
		authMode = "static_auth_key"
	} else if oauthSecret != "" {
		authMode = "oauth_client_secret"
	} else if cfg.Enabled {
		authMode = "persisted_state"
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg:          cfg,
		authKey:      authKey,
		oauthSecret:  oauthSecret,
		sharedSecret: sharedSecret,
		authMode:     authMode,
		backendState: "Disabled",
		ctx:          ctx,
		cancel:       cancel,
		peers:        make(map[string]PeerInfo),
		fallbackHTTP: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

func (m *Manager) SetActiveChannelProvider(fn func() map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeChFn = fn
}

func (m *Manager) SetMeshHTTPHandler(h http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meshHandler = h
}

func (m *Manager) IsEnabled() bool {
	return m.cfg.Enabled
}

func (m *Manager) PreferTailscaleICE() bool {
	return m.cfg.Enabled && m.cfg.PreferTailscale
}

func (m *Manager) GetRole() string {
	return m.cfg.MeshRole
}

func (m *Manager) GetMeshPort() int {
	return m.cfg.MeshPort
}

func (m *Manager) SharedSecret() string {
	return m.sharedSecret
}

func (m *Manager) AddBytesPulled(n int64) {
	if n > 0 {
		atomic.AddInt64(&m.bytesPulled, n)
	}
}

func (m *Manager) AddBytesServed(n int64) {
	if n > 0 {
		atomic.AddInt64(&m.bytesServed, n)
	}
}

func (m *Manager) HTTPClient() *http.Client {
	m.mu.RLock()
	c := m.tsHTTPClient
	m.mu.RUnlock()
	if c != nil {
		return c
	}
	return m.fallbackHTTP
}

// Start boots the embedded tsnet node asynchronously so startup never blocks HTTP serving.
func (m *Manager) Start() error {
	m.mu.Lock()
	if !m.cfg.Enabled || m.started || m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.started = true
	m.backendState = "Starting"
	m.mu.Unlock()

	if err := os.MkdirAll(m.cfg.StateDir, 0700); err != nil {
		m.mu.Lock()
		m.lastErr = fmt.Sprintf("state_dir error: %v", err)
		m.backendState = "Error"
		m.mu.Unlock()
		return err
	}

	go m.runLifecycle()
	return nil
}

func (m *Manager) runLifecycle() {
	srv := &tsnet.Server{
		Hostname: m.cfg.Hostname,
		Dir:      m.cfg.StateDir,
		Logf:     func(string, ...any) {},
	}
	authKey := m.authKey
	if authKey == "" && m.oauthSecret != "" {
		minted, mintErr := m.mintAuthKeyFromOAuth()
		if mintErr != nil {
			m.mu.Lock()
			m.lastErr = fmt.Sprintf("oauth key mint error: %v", mintErr)
			m.mu.Unlock()
			log.Printf("⚠️ [TAILSCALE-MESH] OAuth auth key mint failed: %v", mintErr)
		} else {
			authKey = minted
		}
	}
	if authKey != "" {
		srv.AuthKey = authKey
	}

	m.mu.Lock()
	m.srv = srv
	m.mu.Unlock()

	log.Printf("🕸️ [TAILSCALE-MESH] Starting embedded node '%s' (Role: %s, Auth: %s, MeshPort: %d)...",
		m.cfg.Hostname, m.cfg.MeshRole, m.authMode, m.cfg.MeshPort)

	upCtx, cancelUp := context.WithTimeout(m.ctx, 45*time.Second)
	status, err := srv.Up(upCtx)
	cancelUp()
	if err != nil {
		m.mu.Lock()
		m.lastErr = err.Error()
		m.backendState = "NeedsAuthOrRetry"
		m.mu.Unlock()
		log.Printf("⚠️ [TAILSCALE-MESH] Initial Up() did not complete immediately: %v", err)
		// Still start polling static/mesh peers in background in case Up finishes asynchronously
	}

	ip4, ip6 := srv.TailscaleIPs()
	httpCli := srv.HTTPClient()
	if httpCli != nil {
		httpCli.Timeout = 12 * time.Second
	}

	m.mu.Lock()
	m.tsHTTPClient = httpCli
	if ip4.IsValid() {
		m.ipv4 = ip4.String()
	}
	if ip6.IsValid() {
		m.ipv6 = ip6.String()
	}
	if status != nil {
		m.backendState = status.BackendState
		if status.Self != nil {
			m.magicDNS = strings.TrimSuffix(status.Self.DNSName, ".")
		}
		if status.BackendState == "Running" {
			m.connected = true
			m.lastErr = ""
		}
	}
	handler := m.meshHandler
	m.mu.Unlock()

	if handler != nil {
		if ln, lnErr := srv.Listen("tcp", fmt.Sprintf(":%d", m.cfg.MeshPort)); lnErr == nil {
			httpSrv := &http.Server{
				Handler:           handler,
				ReadHeaderTimeout: 10 * time.Second,
			}
			m.mu.Lock()
			m.meshListener = ln
			m.meshHTTP = httpSrv
			m.mu.Unlock()
			log.Printf("🌐 [TAILSCALE-MESH] Listening over WireGuard on %s:%d (%s)", m.ipv4, m.cfg.MeshPort, m.magicDNS)
			go func() {
				if serveErr := httpSrv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
					log.Printf("⚠️ [TAILSCALE-MESH] Mesh HTTP server stopped: %v", serveErr)
				}
			}()
		} else {
			log.Printf("⚠️ [TAILSCALE-MESH] Failed to open tsnet listener :%d: %v", m.cfg.MeshPort, lnErr)
		}
	}

	m.discoveryLoop()
}

func (m *Manager) mintAuthKeyFromOAuth() (string, error) {
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_secret", m.oauthSecret)

	tokReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.tailscale.com/api/v2/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	tokResp, err := m.fallbackHTTP.Do(tokReq)
	if err != nil {
		return "", err
	}
	tokBody, _ := io.ReadAll(io.LimitReader(tokResp.Body, 64*1024))
	_ = tokResp.Body.Close()
	if tokResp.StatusCode < 200 || tokResp.StatusCode >= 300 {
		return "", fmt.Errorf("oauth/token HTTP %d: %s", tokResp.StatusCode, strings.TrimSpace(string(tokBody)))
	}

	var tokData struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(tokBody, &tokData); err != nil || tokData.AccessToken == "" {
		return "", fmt.Errorf("oauth/token invalid response")
	}

	tags := m.cfg.AdvertiseTags
	if len(tags) == 0 {
		tags = []string{"tag:iptv-mesh"}
	}
	keyReqBody, _ := json.Marshal(map[string]any{
		"capabilities": map[string]any{
			"devices": map[string]any{
				"create": map[string]any{
					"reusable":      false,
					"ephemeral":     false,
					"preauthorized": true,
					"tags":          tags,
				},
			},
		},
		"expirySeconds": 3600,
		"description":   "iptv-p2p-mesh-" + m.cfg.Hostname,
	})

	keyReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.tailscale.com/api/v2/tailnet/-/keys", bytes.NewReader(keyReqBody))
	if err != nil {
		return "", err
	}
	keyReq.Header.Set("Authorization", "Bearer "+tokData.AccessToken)
	keyReq.Header.Set("Content-Type", "application/json")

	keyResp, err := m.fallbackHTTP.Do(keyReq)
	if err != nil {
		return "", err
	}
	keyBody, _ := io.ReadAll(io.LimitReader(keyResp.Body, 64*1024))
	_ = keyResp.Body.Close()
	if keyResp.StatusCode < 200 || keyResp.StatusCode >= 300 {
		return "", fmt.Errorf("tailnet/-/keys HTTP %d: %s", keyResp.StatusCode, strings.TrimSpace(string(keyBody)))
	}

	var keyData struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(keyBody, &keyData); err != nil || keyData.Key == "" {
		return "", fmt.Errorf("tailnet/-/keys invalid response")
	}
	return keyData.Key, nil
}

func (m *Manager) discoveryLoop() {
	m.refreshPeers()
	ticker := time.NewTicker(12 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.refreshPeers()
		}
	}
}

func (m *Manager) refreshPeers() {
	m.mu.RLock()
	srv := m.srv
	m.mu.RUnlock()
	if srv == nil {
		return
	}

	lc, err := srv.LocalClient()
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, 8*time.Second)
	st, err := lc.Status(ctx)
	cancel()
	if err != nil || st == nil {
		return
	}

	ip4, ip6 := srv.TailscaleIPs()
	m.mu.Lock()
	if ip4.IsValid() {
		m.ipv4 = ip4.String()
	}
	if ip6.IsValid() {
		m.ipv6 = ip6.String()
	}
	m.backendState = st.BackendState
	m.connected = st.BackendState == "Running"
	if m.connected {
		m.lastErr = ""
	}
	if st.Self != nil {
		m.magicDNS = strings.TrimSuffix(st.Self.DNSName, ".")
	}
	m.mu.Unlock()

	candidates := make(map[string]PeerInfo)

	// 1. Discover Tailnet peers matching discovery_tag or iptv hostname prefix
	for _, ps := range st.Peer {
		if ps == nil {
			continue
		}
		var tags []string
		hasTag := false
		if ps.Tags != nil {
			for i := 0; i < ps.Tags.Len(); i++ {
				t := ps.Tags.At(i)
				tags = append(tags, t)
				if m.cfg.DiscoveryTag != "" && strings.EqualFold(t, m.cfg.DiscoveryTag) {
					hasTag = true
				}
			}
		}
		nameMatch := strings.HasPrefix(strings.ToLower(ps.HostName), "iptv")
		if !hasTag && !nameMatch && m.cfg.DiscoveryTag != "" {
			continue
		}

		peerIP := pickIPv4FromPeer(ps)
		if peerIP == "" {
			continue
		}
		baseURL := fmt.Sprintf("http://%s:%d", peerIP, m.cfg.MeshPort)
		candidates[baseURL] = PeerInfo{
			Hostname:    ps.HostName,
			DNSName:     strings.TrimSuffix(ps.DNSName, "."),
			TailscaleIP: peerIP,
			BaseURL:     baseURL,
			Role:        "unknown",
			Online:      ps.Online,
			DirectP2P:   strings.TrimSpace(ps.CurAddr) != "",
			Relay:       ps.Relay,
			Tags:        tags,
		}
	}

	// 2. Include statically configured peer URLs
	for _, raw := range m.cfg.StaticPeers {
		u := strings.TrimRight(strings.TrimSpace(raw), "/")
		if u == "" {
			continue
		}
		if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
			u = fmt.Sprintf("http://%s:%d", u, m.cfg.MeshPort)
		}
		if _, exists := candidates[u]; !exists {
			candidates[u] = PeerInfo{
				Hostname:    u,
				TailscaleIP: u,
				BaseURL:     u,
				Role:        "static",
				Online:      true,
			}
		}
	}

	// 3. Probe each candidate's /api/mesh/info over Tailscale WireGuard tunnel
	cli := m.HTTPClient()
	updated := make(map[string]PeerInfo, len(candidates))
	var wg sync.WaitGroup
	var updMu sync.Mutex

	for key, info := range candidates {
		wg.Add(1)
		go func(k string, p PeerInfo) {
			defer wg.Done()
			if !p.Online {
				updMu.Lock()
				updated[k] = p
				updMu.Unlock()
				return
			}
			probeCtx, pCancel := context.WithTimeout(m.ctx, 3*time.Second)
			defer pCancel()

			req, reqErr := http.NewRequestWithContext(probeCtx, http.MethodGet, p.BaseURL+"/api/mesh/info", nil)
			if reqErr == nil {
				if m.sharedSecret != "" {
					req.Header.Set("X-Mesh-Secret", m.sharedSecret)
				}
				t0 := time.Now()
				resp, doErr := cli.Do(req)
				if doErr == nil {
					rtt := time.Since(t0).Milliseconds()
					body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
					_ = resp.Body.Close()
					if readErr == nil && resp.StatusCode == http.StatusOK {
						var payload NodeInfoPayload
						if json.Unmarshal(body, &payload) == nil {
							if payload.Hostname != "" {
								p.Hostname = payload.Hostname
							}
							if payload.Role != "" {
								p.Role = payload.Role
							}
							p.ActiveChannels = payload.ActiveChannels
							p.RTTMs = rtt
							p.Online = true
							p.LastSeenUnix = time.Now().Unix()
						}
					}
				}
			}
			updMu.Lock()
			updated[k] = p
			updMu.Unlock()
		}(key, info)
	}
	wg.Wait()

	m.mu.Lock()
	m.peers = updated
	m.mu.Unlock()
}

func pickIPv4FromPeer(ps *ipnstate.PeerStatus) string {
	for _, a := range ps.TailscaleIPs {
		if a.Is4() {
			return a.String()
		}
	}
	if len(ps.TailscaleIPs) > 0 {
		return ps.TailscaleIPs[0].String()
	}
	return ""
}

// PickPeerForChannel chooses the fastest online Mesh peer that already has channelID active.
// Only seeder or hybrid nodes pull from peers (origin nodes always ingest directly from upstream).
func (m *Manager) PickPeerForChannel(channelID string) (baseURL, hostname string, ok bool) {
	if !m.cfg.Enabled || m.cfg.MeshRole == "origin" || channelID == "" {
		return "", "", false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var best PeerInfo
	found := false
	for _, p := range m.peers {
		if !p.Online || len(p.ActiveChannels) == 0 {
			continue
		}
		hasCh := false
		for _, ch := range p.ActiveChannels {
			if ch == channelID {
				hasCh = true
				break
			}
		}
		if !hasCh {
			continue
		}
		if !found {
			best = p
			found = true
			continue
		}
		// Prefer direct P2P WireGuard over DERP relay, then lowest RTT
		if p.DirectP2P && !best.DirectP2P {
			best = p
		} else if p.DirectP2P == best.DirectP2P && p.RTTMs > 0 && (best.RTTMs == 0 || p.RTTMs < best.RTTMs) {
			best = p
		}
	}

	if !found {
		return "", "", false
	}
	return best.BaseURL, best.Hostname, true
}

// AuthenticateMeshRequest verifies shared secret and Tailscale peer origin on /api/mesh/* requests.
func (m *Manager) AuthenticateMeshRequest(r *http.Request) bool {
	if !m.cfg.Enabled {
		return false
	}
	if m.sharedSecret != "" {
		got := strings.TrimSpace(r.Header.Get("X-Mesh-Secret"))
		if len(got) != len(m.sharedSecret) || subtle.ConstantTimeCompare([]byte(got), []byte(m.sharedSecret)) != 1 {
			return false
		}
	}
	if IsTailscaleIP(r.RemoteAddr) {
		return true
	}
	m.mu.RLock()
	srv := m.srv
	m.mu.RUnlock()
	if srv != nil {
		if lc, err := srv.LocalClient(); err == nil {
			if who, wErr := lc.WhoIs(r.Context(), r.RemoteAddr); wErr == nil && who != nil && who.Node != nil {
				return true
			}
		}
	}
	return false
}

// LocalNodeInfo returns this node's Mesh identity and currently active channel IDs.
func (m *Manager) LocalNodeInfo() NodeInfoPayload {
	m.mu.RLock()
	activeFn := m.activeChFn
	ip4 := m.ipv4
	m.mu.RUnlock()

	var active []string
	if activeFn != nil {
		for id, on := range activeFn() {
			if on {
				active = append(active, id)
			}
		}
		sort.Strings(active)
	}
	return NodeInfoPayload{
		Hostname:       m.cfg.Hostname,
		Role:           m.cfg.MeshRole,
		MeshPort:       m.cfg.MeshPort,
		TailscaleIP:    ip4,
		ActiveChannels: active,
	}
}

// GetStatus returns sanitized Mesh status and discovered peers for the Admin dashboard.
func (m *Manager) GetStatus() MeshStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Hostname < peers[j].Hostname
	})

	secretMask := maskKey(m.oauthSecret)
	if secretMask == "" {
		secretMask = maskKey(m.authKey)
	}

	return MeshStatus{
		Enabled:         m.cfg.Enabled,
		Connected:       m.connected,
		BackendState:    m.backendState,
		AuthMode:        m.authMode,
		AuthConfigured:  m.authKey != "" || m.oauthSecret != "",
		SecretMask:      secretMask,
		Hostname:        m.cfg.Hostname,
		MagicDNS:        m.magicDNS,
		TailscaleIPv4:   m.ipv4,
		TailscaleIPv6:   m.ipv6,
		MeshRole:        m.cfg.MeshRole,
		MeshPort:        m.cfg.MeshPort,
		DiscoveryTag:    m.cfg.DiscoveryTag,
		PreferTailscale: m.cfg.PreferTailscale,
		LastError:       m.lastErr,
		BytesPulled:     atomic.LoadInt64(&m.bytesPulled),
		BytesServed:     atomic.LoadInt64(&m.bytesServed),
		Peers:           peers,
	}
}

// Stop shuts down the Mesh HTTP server and embedded tsnet server cleanly.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	httpSrv := m.meshHTTP
	ln := m.meshListener
	srv := m.srv
	m.mu.Unlock()

	m.cancel()
	if httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = httpSrv.Shutdown(ctx)
		cancel()
	}
	if ln != nil {
		_ = ln.Close()
	}
	if srv != nil {
		_ = srv.Close()
	}
}
