package tracker

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"iptv-p2p-server/internal/auth"
	"iptv-p2p-server/internal/mesh"
	"iptv-p2p-server/pkg/protocol"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var peerCounter uint64

func generatePeerID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	n := atomic.AddUint64(&peerCounter, 1)
	return fmt.Sprintf("peer_%d_%s", n, hex.EncodeToString(b[:]))
}

// PeerClient represents a connected web browser or native P2P client
type PeerClient struct {
	ID              string
	ChannelID       string
	UserTier        string
	IP              string
	IsIPv6          bool
	IsTailscale     bool
	BinaryZstd      bool
	Conn            *websocket.Conn
	P2PBytesIn      int64
	HTTPBytesIn     int64
	P2POutFreeBytes int64
	P2POutPaidBytes int64
	mu              sync.Mutex
}

func (p *PeerClient) SendMessage(msg *protocol.P2PMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.BinaryZstd {
		compressed, err := protocol.EncodeCompressed(msg)
		if err != nil {
			return err
		}
		return p.Conn.WriteMessage(websocket.BinaryMessage, compressed)
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return p.Conn.WriteMessage(websocket.TextMessage, jsonBytes)
}

// SwarmTracker manages P2P peer swarms per channel
type SwarmTracker struct {
	authManager *auth.AuthManager
	mu          sync.RWMutex
	swarms      map[string]map[string]*PeerClient // channel_id -> peer_id -> PeerClient
	webrtcAPI   *webrtc.API
	getTierFn   func(channelID string) string
}

func NewSwarmTracker(authMgr *auth.AuthManager, minPort, maxPort uint16) *SwarmTracker {
	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetNetworkTypes([]webrtc.NetworkType{
		webrtc.NetworkTypeUDP6,
		webrtc.NetworkTypeUDP4,
		webrtc.NetworkTypeTCP6,
		webrtc.NetworkTypeTCP4,
	})
	if minPort > 0 && maxPort >= minPort {
		_ = settingEngine.SetEphemeralUDPPortRange(minPort, maxPort)
	}

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	return &SwarmTracker{
		authManager: authMgr,
		swarms:      make(map[string]map[string]*PeerClient),
		webrtcAPI:   api,
	}
}

// SetChannelTierResolver sets a lookup callback to enforce paid tier access on WebSocket join
func (st *SwarmTracker) SetChannelTierResolver(fn func(channelID string) string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.getTierFn = fn
}

// GetWebRTCAPI exposes the configured Pion WebRTC API instance
func (st *SwarmTracker) GetWebRTCAPI() *webrtc.API {
	return st.webrtcAPI
}

// HandleWebSocket handles incoming client WebSocket connections for WebRTC P2P signaling
func (st *SwarmTracker) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[TRACKER] WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	channelID := r.URL.Query().Get("channel_id")
	if channelID == "" {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"error": "channel_id required"}`))
		return
	}

	tokenStr := strings.TrimSpace(r.URL.Query().Get("token"))
	if tokenStr == "" {
		if c, cErr := r.Cookie("iptv_session"); cErr == nil {
			tokenStr = strings.TrimSpace(c.Value)
		}
	}
	peerID := r.URL.Query().Get("peer_id")
	if peerID == "" {
		peerID = generatePeerID()
	}
	useZstd := r.URL.Query().Get("codec") == "zstd"

	claims, err := st.authManager.ValidateToken(tokenStr)
	if err != nil {
		log.Printf("[TRACKER] Auth failed for peer %s: %v", peerID, err)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"error": "unauthorized"}`))
		return
	}

	st.mu.RLock()
	tierFn := st.getTierFn
	st.mu.RUnlock()
	if tierFn != nil {
		chTier := tierFn(channelID)
		if chTier != "" && !st.authManager.CanAccessChannel(claims.Tier, chTier) {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"error": "subscription required for paid channel"}`))
			return
		}
	}

	remoteAddr := r.RemoteAddr
	isIPv6 := len(remoteAddr) > 0 && remoteAddr[0] == '['
	isTailscale := mesh.IsTailscaleIP(remoteAddr)

	peer := &PeerClient{
		ID:          peerID,
		ChannelID:   channelID,
		UserTier:    claims.Tier,
		IP:          remoteAddr,
		IsIPv6:      isIPv6,
		IsTailscale: isTailscale,
		BinaryZstd:  useZstd,
		Conn:        conn,
	}

	st.registerPeer(peer)
	defer func() {
		st.unregisterPeer(peer)
		st.broadcastPeerListToChannel(peer.ChannelID)
	}()

	log.Printf("[TRACKER] Peer %s joined swarm for channel %s (Tier: %s, IPv6: %v, Tailscale: %v)", peerID, channelID, claims.Tier, isIPv6, isTailscale)

	// Send peer its assigned ID + server-verified tier, then notify all swarm members
	_ = peer.SendMessage(&protocol.P2PMessage{
		Type:     "WELCOME",
		SenderID: "server",
		TargetID: peer.ID,
		SelfTier: peer.UserTier,
	})
	st.broadcastPeerListToChannel(peer.ChannelID)

	for {
		_, messageBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}

		msg, err := protocol.DecodeCompressed(messageBytes)
		if err != nil {
			var rawMsg protocol.P2PMessage
			if jsonErr := json.Unmarshal(messageBytes, &rawMsg); jsonErr == nil {
				msg = &rawMsg
			} else {
				continue
			}
		}

		if msg.Type == protocol.MessageTypeP2PStats && msg.Stats != nil {
			st.recordPeerStats(peer, msg.Stats)
			continue
		}

		st.routeSignalingMessage(peer, msg)
	}
}

func (st *SwarmTracker) recordPeerStats(peer *PeerClient, rep *protocol.P2PStatsReport) {
	if rep == nil {
		return
	}
	clamp := func(v int64) int64 {
		if v < 0 {
			return 0
		}
		return v
	}
	peer.mu.Lock()
	peer.P2PBytesIn = clamp(rep.P2PBytesIn)
	peer.HTTPBytesIn = clamp(rep.HTTPBytesIn)
	peer.P2POutFreeBytes = clamp(rep.P2POutFreeBytes)
	peer.P2POutPaidBytes = clamp(rep.P2POutPaidBytes)
	peer.mu.Unlock()
}

func (st *SwarmTracker) registerPeer(peer *PeerClient) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if _, exists := st.swarms[peer.ChannelID]; !exists {
		st.swarms[peer.ChannelID] = make(map[string]*PeerClient)
	}
	st.swarms[peer.ChannelID][peer.ID] = peer
}

func (st *SwarmTracker) unregisterPeer(peer *PeerClient) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if swarm, exists := st.swarms[peer.ChannelID]; exists {
		delete(swarm, peer.ID)
		if len(swarm) == 0 {
			delete(st.swarms, peer.ChannelID)
		}
	}
	log.Printf("[TRACKER] Peer %s left swarm for channel %s", peer.ID, peer.ChannelID)
}

func (st *SwarmTracker) broadcastPeerListToChannel(channelID string) {
	st.mu.RLock()
	swarm := st.swarms[channelID]
	if len(swarm) == 0 {
		st.mu.RUnlock()
		return
	}
	peers := make([]*PeerClient, 0, len(swarm))
	allInfos := make([]protocol.PeerInfo, 0, len(swarm))
	for _, p := range swarm {
		peers = append(peers, p)
		allInfos = append(allInfos, protocol.PeerInfo{
			ID:          p.ID,
			Tier:        p.UserTier,
			IsIPv6:      p.IsIPv6,
			IsTailscale: p.IsTailscale,
		})
	}
	st.mu.RUnlock()

	for _, recipient := range peers {
		otherIDs := make([]string, 0, len(allInfos))
		otherInfos := make([]protocol.PeerInfo, 0, len(allInfos))
		for _, info := range allInfos {
			if info.ID != recipient.ID {
				otherIDs = append(otherIDs, info.ID)
				otherInfos = append(otherInfos, info)
			}
		}
		_ = recipient.SendMessage(&protocol.P2PMessage{
			Type:       "SWARM_PEERS",
			SenderID:   "server",
			TargetID:   recipient.ID,
			SelfTier:   recipient.UserTier,
			Peers:      otherIDs,
			PeerInfos:  otherInfos,
			TotalPeers: len(allInfos),
		})
	}
}

func (st *SwarmTracker) routeSignalingMessage(sender *PeerClient, msg *protocol.P2PMessage) {
	if msg.TargetID == "" {
		return
	}

	st.mu.RLock()
	var targetPeer *PeerClient
	var found bool
	if swarm, ok := st.swarms[sender.ChannelID]; ok {
		targetPeer, found = swarm[msg.TargetID]
	}
	st.mu.RUnlock()

	if found {
		msg.SenderID = sender.ID
		_ = targetPeer.SendMessage(msg)
	}
}

// GetSwarmStats returns peer counts, IPv6 ratios, and real P2P/HTTP offload KB per channel
func (st *SwarmTracker) GetSwarmStats() map[string]map[string]int {
	st.mu.RLock()
	defer st.mu.RUnlock()

	stats := make(map[string]map[string]int, len(st.swarms))
	for chID, swarm := range st.swarms {
		total := len(swarm)
		ipv6Count := 0
		tsCount := 0
		var p2pIn, httpIn, outFree, outPaid int64
		for _, peer := range swarm {
			if peer.IsIPv6 {
				ipv6Count++
			}
			if peer.IsTailscale {
				tsCount++
			}
			peer.mu.Lock()
			p2pIn += peer.P2PBytesIn
			httpIn += peer.HTTPBytesIn
			outFree += peer.P2POutFreeBytes
			outPaid += peer.P2POutPaidBytes
			peer.mu.Unlock()
		}
		stats[chID] = map[string]int{
			"total_peers":     total,
			"ipv6_peers":      ipv6Count,
			"ipv4_peers":      total - ipv6Count,
			"tailscale_peers": tsCount,
			"p2p_kb_in":       int(p2pIn / 1024),
			"http_kb_in":      int(httpIn / 1024),
			"p2p_out_free_kb": int(outFree / 1024),
			"p2p_out_paid_kb": int(outPaid / 1024),
		}
	}
	return stats
}
