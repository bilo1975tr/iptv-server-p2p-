package api

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"iptv-p2p-server/internal/acestream"
	"iptv-p2p-server/internal/auth"
	"iptv-p2p-server/internal/epg"
	"iptv-p2p-server/internal/metrics"
	"iptv-p2p-server/internal/playlist"
	"iptv-p2p-server/internal/storage"
	"iptv-p2p-server/internal/tracker"
)

type StreamRequest struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Tier        string   `json:"tier"` // free or paid
	SourceURL   string   `json:"source_url"`
	SourceURLs  []string `json:"source_urls"` // Alternative URLs
	LogoURL     string   `json:"logo_url"`
	EPGID       string   `json:"epg_id"`
	IsStreaming bool     `json:"is_streaming,omitempty"`
	IsAceStream bool     `json:"is_acestream,omitempty"`
}

type PublicChannel struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Tier        string `json:"tier"`
	LogoURL     string `json:"logo_url"`
	EPGID       string `json:"epg_id"`
	IsAceStream bool   `json:"is_acestream,omitempty"`
	ViewerCount int    `json:"viewer_count"`
}

type M3UImportRequest struct {
	M3UContent  string `json:"m3u_content"`
	AutoPrune   bool   `json:"auto_prune"`
	InsecureTLS bool   `json:"insecure_tls"`
	Tier        string `json:"tier"`
	Language    string `json:"language"`
	ContentType string `json:"content_type"`
}

type EPGSourceRequest struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	TimeOffset int    `json:"time_offset"`
}

type EPGMapRequest struct {
	InternalID string `json:"internal_id"`
	EPGID      string `json:"epg_id"`
}

type UserRequest struct {
	UserID string `json:"user_id"`
	Tier   string `json:"tier"` // free or paid
}

type AdminLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type StreamLifecycleHooks struct {
	OnUpsert       func(id, tier string, sourceURLs []string)
	OnDelete       func(id string)
	OnStop         func(id string)
	GetActiveIDs   func() map[string]bool
	GetHTTPViewers func() map[string]int
	RecordViewer   func(channelID, viewerKey string)
	RemoveViewer   func(channelID, viewerKey string)
}

type AdminAPI struct {
	mu                sync.RWMutex
	authManager       *auth.AuthManager
	tracker           *tracker.SwarmTracker
	aceManager        *acestream.Manager
	streams           map[string]StreamRequest
	normalizedMap     map[string]string // normalized_name -> stream_id
	m3uSources        []storage.M3USource
	deletedChannelIDs map[string]bool
	users             map[string]storage.UserAccount
	billing           storage.BillingSettings
	paymentOrders     map[string]storage.PaymentOrder
	usedTxHashes      map[string]string
	lastCheckByOrder  map[string]int64
	hooks             StreamLifecycleHooks
}

func NewAdminAPI(authMgr *auth.AuthManager, tr *tracker.SwarmTracker, hooks StreamLifecycleHooks) *AdminAPI {
	return &AdminAPI{
		authManager:       authMgr,
		tracker:           tr,
		streams:           make(map[string]StreamRequest),
		normalizedMap:     make(map[string]string),
		m3uSources:        []storage.M3USource{},
		deletedChannelIDs: make(map[string]bool),
		users:             make(map[string]storage.UserAccount),
		billing:           storage.DefaultBillingSettings(),
		paymentOrders:     make(map[string]storage.PaymentOrder),
		usedTxHashes:      make(map[string]string),
		lastCheckByOrder:  make(map[string]int64),
		hooks:             hooks,
	}
}

func (api *AdminAPI) SetAceStreamManager(mgr *acestream.Manager) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.aceManager = mgr
}

func isStreamAce(s StreamRequest) bool {
	if acestream.IsAceStreamURL(s.SourceURL) {
		return true
	}
	for _, u := range s.SourceURLs {
		if acestream.IsAceStreamURL(u) {
			return true
		}
	}
	return false
}

func (api *AdminAPI) RestoreState(data *storage.StorageData) {
	if data == nil {
		return
	}
	api.mu.Lock()
	defer api.mu.Unlock()

	if data.M3USources != nil {
		api.m3uSources = data.M3USources
	}
	if data.DeletedChannelIDs != nil {
		api.deletedChannelIDs = data.DeletedChannelIDs
	}
	if data.Users != nil {
		api.users = data.Users
	}
	if data.Billing != nil {
		api.billing = *data.Billing
	}
	if data.PaymentOrders != nil {
		api.paymentOrders = data.PaymentOrders
	}
	if data.UsedTxHashes != nil {
		api.usedTxHashes = data.UsedTxHashes
	}
}

func (api *AdminAPI) RestoreStream(s StreamRequest) {
	api.mu.Lock()
	if api.deletedChannelIDs[s.ID] {
		api.mu.Unlock()
		return
	}
	if s.SourceURL == "" && len(s.SourceURLs) > 0 {
		s.SourceURL = s.SourceURLs[0]
	}
	if s.SourceURL != "" {
		s.SourceURLs = uniqueStrings(append([]string{s.SourceURL}, s.SourceURLs...))
	} else {
		s.SourceURLs = uniqueStrings(s.SourceURLs)
	}
	if len(s.SourceURLs) > 0 {
		s.SourceURL = s.SourceURLs[0]
	}
	if s.Tier == "" {
		s.Tier = auth.TierFree
	}
	api.streams[s.ID] = s
	api.normalizedMap[normalizeName(s.Name)] = s.ID
	api.mu.Unlock()

	if api.hooks.OnUpsert != nil {
		api.hooks.OnUpsert(s.ID, s.Tier, s.SourceURLs)
	}
}

func (api *AdminAPI) saveStateLocked() {
	streamsCopy := make(map[string]StreamRequest, len(api.streams))
	for k, v := range api.streams {
		v.IsStreaming = false
		streamsCopy[k] = v
	}
	m3uCopy := append([]storage.M3USource(nil), api.m3uSources...)
	delCopy := make(map[string]bool, len(api.deletedChannelIDs))
	for k, v := range api.deletedChannelIDs {
		delCopy[k] = v
	}
	usersCopy := make(map[string]storage.UserAccount, len(api.users))
	for k, v := range api.users {
		usersCopy[k] = v
	}
	billingCopy := api.billing
	ordersCopy := make(map[string]storage.PaymentOrder, len(api.paymentOrders))
	for k, v := range api.paymentOrders {
		ordersCopy[k] = v
	}
	txCopy := make(map[string]string, len(api.usedTxHashes))
	for k, v := range api.usedTxHashes {
		txCopy[k] = v
	}

	storage.SaveStateFull(
		streamsCopy,
		epg.GlobalEPG.GetSources(),
		epg.GlobalEPG.GetMappings(),
		m3uCopy,
		delCopy,
		usersCopy,
		&billingCopy,
		ordersCopy,
		txCopy,
	)
}

func normalizeName(name string) string {
	s := strings.ToLower(name)
	rMap := map[rune]rune{
		'î': 'i', 'í': 'i', 'ì': 'i', 'ï': 'i', 'İ': 'i', 'ı': 'i',
		'ş': 's', 'Ş': 's',
		'ğ': 'g', 'Ğ': 'g',
		'ü': 'u', 'Ü': 'u',
		'ö': 'o', 'Ö': 'o',
		'ç': 'c', 'Ç': 'c',
		'â': 'a', 'ä': 'a', 'à': 'a', 'á': 'a', 'ã': 'a',
		'ê': 'e', 'ë': 'e', 'è': 'e', 'é': 'e',
		'ô': 'o', 'ó': 'o', 'ò': 'o', 'õ': 'o',
		'û': 'u', 'ú': 'u', 'ù': 'u',
		'ñ': 'n',
	}

	var sb strings.Builder
	for _, r := range s {
		if mapped, ok := rMap[r]; ok {
			r = mapped
		}
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		}
	}
	s = sb.String()
	s = strings.ReplaceAll(s, "hd", "")
	s = strings.ReplaceAll(s, "fhd", "")
	s = strings.ReplaceAll(s, "sd", "")
	s = strings.ReplaceAll(s, "4k", "")
	return s
}

func normalizeEPGKey(raw string) string {
	s := normalizeName(raw)
	for _, tld := range []string{"tr", "uk", "de", "az", "com"} {
		if strings.HasSuffix(s, tld) && len(s) > len(tld)+2 {
			s = strings.TrimSuffix(s, tld)
		}
	}
	return s
}

func canonicalStreamURL(raw string) string {
	u := strings.TrimSpace(raw)
	if u == "" {
		return ""
	}
	low := strings.ToLower(u)
	if strings.HasPrefix(low, "acestream://") {
		cid := strings.Trim(strings.TrimPrefix(low, "acestream://"), "/")
		return "acestream://" + cid
	}
	if parsed, err := url.Parse(u); err == nil {
		if strings.Contains(strings.ToLower(parsed.Path), "/ace/getstream") {
			if cid := strings.TrimSpace(parsed.Query().Get("id")); cid != "" {
				return "acestream://" + strings.ToLower(cid)
			}
		}
		if parsed.Scheme != "" && parsed.Host != "" {
			scheme := strings.ToLower(parsed.Scheme)
			host := strings.ToLower(parsed.Host)
			if scheme == "http" {
				host = strings.TrimSuffix(host, ":80")
			} else if scheme == "https" {
				host = strings.TrimSuffix(host, ":443")
			}
			parsed.Scheme = scheme
			parsed.Host = host
			if parsed.Path != "/" {
				parsed.Path = strings.TrimRight(parsed.Path, "/")
			}
			return parsed.String()
		}
	}
	return strings.TrimRight(u, "/")
}

func uniqueStrings(input []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, u := range input {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		key := canonicalStreamURL(u)
		if key == "" {
			key = u
		}
		if !seen[key] {
			seen[key] = true
			result = append(result, u)
		}
	}
	return result
}

func (api *AdminAPI) findDuplicateURLOwnerLocked(urls []string, excludeID string) (string, string, string) {
	checkSet := make(map[string]string, len(urls))
	for _, u := range urls {
		if c := canonicalStreamURL(u); c != "" {
			checkSet[c] = u
		}
	}
	if len(checkSet) == 0 {
		return "", "", ""
	}
	for id, s := range api.streams {
		if id == excludeID {
			continue
		}
		allURLs := append([]string{s.SourceURL}, s.SourceURLs...)
		for _, existingU := range allURLs {
			c := canonicalStreamURL(existingU)
			if rawDup, hit := checkSet[c]; hit {
				return id, s.Name, rawDup
			}
		}
	}
	return "", "", ""
}

func (api *AdminAPI) rebuildNormalizedMapLocked() {
	api.normalizedMap = make(map[string]string, len(api.streams))
	ids := make([]string, 0, len(api.streams))
	for id := range api.streams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		s := api.streams[id]
		if norm := normalizeName(s.Name); norm != "" {
			if _, exists := api.normalizedMap[norm]; !exists {
				api.normalizedMap[norm] = id
			}
		}
	}
}

func (api *AdminAPI) mergeChannelIntoLocked(targetID, srcID string) bool {
	if targetID == "" || srcID == "" || targetID == srcID {
		return false
	}
	target, okT := api.streams[targetID]
	src, okS := api.streams[srcID]
	if !okT || !okS {
		return false
	}

	combined := make([]string, 0, 2+len(target.SourceURLs)+len(src.SourceURLs))
	if target.SourceURL != "" {
		combined = append(combined, target.SourceURL)
	}
	combined = append(combined, target.SourceURLs...)
	if src.SourceURL != "" {
		combined = append(combined, src.SourceURL)
	}
	combined = append(combined, src.SourceURLs...)
	target.SourceURLs = uniqueStrings(combined)
	if len(target.SourceURLs) > 0 {
		target.SourceURL = target.SourceURLs[0]
	}

	if target.LogoURL == "" && src.LogoURL != "" {
		target.LogoURL = src.LogoURL
	}
	if target.EPGID == "" && src.EPGID != "" {
		target.EPGID = src.EPGID
		epg.GlobalEPG.MapChannel(target.ID, target.EPGID)
	}

	// Migrate user favorites from merged-away srcID to targetID
	for uid, u := range api.users {
		if len(u.Favorites) == 0 {
			continue
		}
		changed := false
		hasTarget := false
		for _, f := range u.Favorites {
			if f == targetID {
				hasTarget = true
				break
			}
		}
		var updatedFavs []string
		for _, f := range u.Favorites {
			if f == srcID {
				changed = true
				if !hasTarget {
					updatedFavs = append(updatedFavs, targetID)
					hasTarget = true
				}
			} else {
				updatedFavs = append(updatedFavs, f)
			}
		}
		if changed {
			u.Favorites = updatedFavs
			api.users[uid] = u
		}
	}

	api.streams[targetID] = target
	delete(api.streams, srcID)
	return true
}

// DeduplicateByURL merges any separate channels that share the exact same canonical stream URL.
func (api *AdminAPI) DeduplicateByURL() int {
	type pair struct {
		target StreamRequest
		srcID  string
	}
	var removedIDs []string
	var updatedTargets []StreamRequest

	api.mu.Lock()
	ids := make([]string, 0, len(api.streams))
	for id := range api.streams {
		ids = append(ids, id)
	}
	score := func(s StreamRequest) int {
		sc := len(s.SourceURLs) * 10
		if s.EPGID != "" {
			sc += 100
		}
		if s.LogoURL != "" {
			sc += 20
		}
		sc += len(strings.TrimSpace(s.Name))
		return sc
	}
	sort.Slice(ids, func(i, j int) bool {
		si, sj := api.streams[ids[i]], api.streams[ids[j]]
		sci, scj := score(si), score(sj)
		if sci != scj {
			return sci > scj
		}
		return ids[i] < ids[j]
	})

	urlOwner := make(map[string]string, len(ids)*2)
	touchedTargets := make(map[string]bool)

	for _, id := range ids {
		s, exists := api.streams[id]
		if !exists {
			continue
		}
		allURLs := uniqueStrings(append([]string{s.SourceURL}, s.SourceURLs...))
		matchedTarget := ""
		for _, u := range allURLs {
			c := canonicalStreamURL(u)
			if owner, ok := urlOwner[c]; ok && owner != id {
				matchedTarget = owner
				break
			}
		}
		if matchedTarget != "" {
			if api.mergeChannelIntoLocked(matchedTarget, id) {
				removedIDs = append(removedIDs, id)
				touchedTargets[matchedTarget] = true
				t := api.streams[matchedTarget]
				for _, tu := range append([]string{t.SourceURL}, t.SourceURLs...) {
					if c := canonicalStreamURL(tu); c != "" {
						urlOwner[c] = matchedTarget
					}
				}
			}
			continue
		}
		for _, u := range allURLs {
			if c := canonicalStreamURL(u); c != "" {
				urlOwner[c] = id
			}
		}
	}

	if len(removedIDs) > 0 {
		api.rebuildNormalizedMapLocked()
		for tid := range touchedTargets {
			if t, ok := api.streams[tid]; ok {
				updatedTargets = append(updatedTargets, t)
			}
		}
		api.saveStateLocked()
	}
	api.mu.Unlock()

	for _, rid := range removedIDs {
		if api.hooks.OnDelete != nil {
			api.hooks.OnDelete(rid)
		}
	}
	for _, t := range updatedTargets {
		if api.hooks.OnStop != nil {
			api.hooks.OnStop(t.ID)
		}
		if api.hooks.OnUpsert != nil {
			api.hooks.OnUpsert(t.ID, t.Tier, t.SourceURLs)
		}
	}

	return len(removedIDs)
}

func (api *AdminAPI) RegisterRoutes(mux *http.ServeMux) {
	// Public endpoints
	mux.HandleFunc("/api/admin/login", api.handleAdminLogin)
	mux.HandleFunc("/api/channels", api.handlePublicChannels)
	mux.HandleFunc("/api/epg/client.xml", api.handleClientEPGXML)
	mux.HandleFunc("/api/epg/json", api.handleClientEPGJson)

	// Protected Admin endpoints
	mux.HandleFunc("/api/admin/metrics", api.requireAdmin(api.handleMetrics))
	mux.HandleFunc("/api/admin/streams", api.requireAdmin(api.handleStreams))
	mux.HandleFunc("/api/admin/streams/update", api.requireAdmin(api.handleUpdateStream))
	mux.HandleFunc("/api/admin/streams/merge", api.requireAdmin(api.handleMergeStreams))
	mux.HandleFunc("/api/admin/streams/dedupe", api.requireAdmin(api.handleDedupeStreams))
	mux.HandleFunc("/api/admin/streams/delete", api.requireAdmin(api.handleDeleteStream))
	mux.HandleFunc("/api/admin/streams/stop", api.requireAdmin(api.handleStopStream))
	mux.HandleFunc("/api/admin/swarms", api.requireAdmin(api.handleSwarms))
	mux.HandleFunc("/api/admin/auth/generate-token", api.requireAdmin(api.handleGenerateToken))
	mux.HandleFunc("/api/admin/m3u/import", api.requireAdmin(api.handleM3UImport))
	mux.HandleFunc("/api/admin/m3u/sources", api.requireAdmin(api.handleM3USources))
	mux.HandleFunc("/api/admin/users", api.requireAdmin(api.handleUsers))
	mux.HandleFunc("/api/admin/epg/source", api.requireAdmin(api.handleEPGSource))
	mux.HandleFunc("/api/admin/epg/sources", api.requireAdmin(api.handleEPGSources))
	mux.HandleFunc("/api/admin/epg/map", api.requireAdmin(api.handleEPGMap))
	mux.HandleFunc("/api/admin/epg/channels", api.requireAdmin(api.handleGetEPGChannels))
	mux.HandleFunc("/api/admin/epg/auto-map", api.requireAdmin(api.handleAutoMapEPG))
	mux.HandleFunc("/api/admin/backup/export", api.requireAdmin(api.handleExportBackup))
	mux.HandleFunc("/api/admin/backup/import", api.requireAdmin(api.handleImportBackup))

	// AceStream Engine, Search & Channel Add endpoints
	mux.HandleFunc("/api/admin/acestream/status", api.requireAdmin(api.handleAceStreamStatus))
	mux.HandleFunc("/api/admin/acestream/start", api.requireAdmin(api.handleAceStreamStart))
	mux.HandleFunc("/api/admin/acestream/stop", api.requireAdmin(api.handleAceStreamStop))
	mux.HandleFunc("/api/admin/acestream/search", api.requireAdmin(api.handleAceStreamSearch))
	mux.HandleFunc("/api/admin/acestream/add", api.requireAdmin(api.handleAceStreamAdd))

	// User Auth, USDT Auto-Billing, Favorites & Live Viewers
	api.registerUserBillingRoutes(mux)
}

func (api *AdminAPI) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// 1. Check Bearer JWT or X-Admin-Token or query param
		tokenStr := ""
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			tokenStr = strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		} else if tok := r.Header.Get("X-Admin-Token"); tok != "" {
			tokenStr = tok
		} else if tok := r.URL.Query().Get("admin_token"); tok != "" {
			tokenStr = tok
		}

		if tokenStr != "" {
			if _, err := api.authManager.ValidateAdminToken(tokenStr); err == nil {
				next(w, r)
				return
			}
		}

		// 2. Check HTTP Basic Auth fallback
		if strings.HasPrefix(authHeader, "Basic ") {
			raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader, "Basic "))
			if err == nil {
				parts := strings.SplitN(string(raw), ":", 2)
				if len(parts) == 2 && api.authManager.CheckAdminCredentials(parts[0], parts[1]) {
					next(w, r)
					return
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "Admin authentication required",
		})
	}
}

func (api *AdminAPI) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req AdminLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if !api.authManager.CheckAdminCredentials(req.Username, req.Password) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "Geçersiz kullanıcı adı veya şifre",
		})
		return
	}

	token, err := api.authManager.GenerateToken(req.Username, auth.TierAdmin, 24)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"token":  token,
		"user":   req.Username,
	})
}

// handlePublicChannels serves sanitized channel list without exposing upstream source URLs
func (api *AdminAPI) handlePublicChannels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	viewerCounts := api.getViewerCounts()

	api.mu.RLock()
	list := make([]PublicChannel, 0, len(api.streams))
	for _, s := range api.streams {
		list = append(list, PublicChannel{
			ID:          s.ID,
			Name:        s.Name,
			Tier:        s.Tier,
			LogoURL:     s.LogoURL,
			EPGID:       s.EPGID,
			IsAceStream: isStreamAce(s),
			ViewerCount: viewerCounts[s.ID],
		})
	}
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(list)
}

func (api *AdminAPI) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	m := metrics.CollectMetrics()
	activeCount := 0
	if api.hooks.GetActiveIDs != nil {
		activeCount = len(api.hooks.GetActiveIDs())
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"cpu_usage_pct":      m.CPUUsagePct,
		"ram_used_mb":        m.RAMUsedMB,
		"ram_total_mb":       m.RAMTotalMB,
		"gpu_available":      m.GPUAvailable,
		"gpu_encoder_pct":    m.GPUEncoderPct,
		"gpu_memory_used_mb": m.GPUMemoryUsedMB,
		"gpu_temp_c":         m.GPUTemperatureC,
		"goroutines_count":   m.GoroutinesCount,
		"uptime_seconds":     m.UptimeSeconds,
		"active_streams":     activeCount,
	})
}

func (api *AdminAPI) handleStreams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodPost {
		var req StreamRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid stream JSON payload", http.StatusBadRequest)
			return
		}

		req.Name = strings.TrimSpace(req.Name)
		req.SourceURL = strings.TrimSpace(req.SourceURL)
		if req.Name == "" {
			http.Error(w, "Channel name is required", http.StatusBadRequest)
			return
		}
		if req.Tier == "" {
			req.Tier = auth.TierFree
		}

		req.SourceURLs = uniqueStrings(append([]string{req.SourceURL}, req.SourceURLs...))
		if len(req.SourceURLs) > 0 {
			req.SourceURL = req.SourceURLs[0]
		}
		if req.SourceURL == "" {
			http.Error(w, "Source URL is required", http.StatusBadRequest)
			return
		}

		if req.ID == "" {
			sum := sha256.Sum256([]byte(canonicalStreamURL(req.SourceURL) + "|" + req.Name))
			req.ID = "ch_" + hex.EncodeToString(sum[:5])
		}

		api.mu.Lock()
		if ownerID, ownerName, _ := api.findDuplicateURLOwnerLocked(req.SourceURLs, req.ID); ownerID != "" {
			api.mu.Unlock()
			http.Error(w, fmt.Sprintf("Bu yayın adresi zaten '%s' (%s) kanalında kayıtlı. Mükerrer adres eklemek yerine kanalları 'Birleştir' butonuyla birleştirebilirsiniz.", ownerName, ownerID), http.StatusConflict)
			return
		}
		delete(api.deletedChannelIDs, req.ID)
		api.streams[req.ID] = req
		api.normalizedMap[normalizeName(req.Name)] = req.ID
		api.saveStateLocked()
		api.mu.Unlock()

		if req.EPGID != "" {
			epg.GlobalEPG.MapChannel(req.ID, req.EPGID)
		}
		if api.hooks.OnUpsert != nil {
			api.hooks.OnUpsert(req.ID, req.Tier, req.SourceURLs)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "success",
			"message": "Stream added",
			"channel": req,
		})
		return
	}

	var activeMap map[string]bool
	if api.hooks.GetActiveIDs != nil {
		activeMap = api.hooks.GetActiveIDs()
	}

	api.mu.RLock()
	out := make(map[string]StreamRequest, len(api.streams))
	for id, s := range api.streams {
		if activeMap != nil && activeMap[id] {
			s.IsStreaming = true
		}
		s.IsAceStream = isStreamAce(s)
		out[id] = s
	}
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(out)
}

func (api *AdminAPI) handleStopStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Channel ID required", http.StatusBadRequest)
		return
	}
	if api.hooks.OnStop != nil {
		api.hooks.OnStop(id)
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Stream pipeline stopped"})
}

func (api *AdminAPI) handleDeleteStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "POST/DELETE required", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		var req struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		id = req.ID
	}

	if id == "" {
		http.Error(w, "Channel ID required", http.StatusBadRequest)
		return
	}

	api.mu.Lock()
	if existing, ok := api.streams[id]; ok {
		delete(api.normalizedMap, normalizeName(existing.Name))
	}
	delete(api.streams, id)
	api.deletedChannelIDs[id] = true
	api.saveStateLocked()
	api.mu.Unlock()

	if api.hooks.OnDelete != nil {
		api.hooks.OnDelete(id)
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Channel deleted and blacklisted"})
}

func (api *AdminAPI) handleUpdateStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "POST/PUT required", http.StatusMethodNotAllowed)
		return
	}

	var req StreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "Invalid stream update request", http.StatusBadRequest)
		return
	}

	api.mu.Lock()
	existing, exists := api.streams[req.ID]
	if !exists {
		api.mu.Unlock()
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}

	if req.Name != "" {
		delete(api.normalizedMap, normalizeName(existing.Name))
		existing.Name = req.Name
		api.normalizedMap[normalizeName(existing.Name)] = existing.ID
	}
	if req.Tier != "" {
		existing.Tier = req.Tier
	}
	if req.SourceURL != "" {
		existing.SourceURL = req.SourceURL
	}
	if len(req.SourceURLs) > 0 {
		if req.SourceURL != "" {
			existing.SourceURLs = uniqueStrings(append([]string{req.SourceURL}, req.SourceURLs...))
		} else {
			existing.SourceURLs = uniqueStrings(req.SourceURLs)
		}
		if len(existing.SourceURLs) > 0 {
			existing.SourceURL = existing.SourceURLs[0]
		}
	} else if existing.SourceURL != "" {
		existing.SourceURLs = uniqueStrings(append([]string{existing.SourceURL}, existing.SourceURLs...))
		if len(existing.SourceURLs) > 0 {
			existing.SourceURL = existing.SourceURLs[0]
		}
	}
	if ownerID, ownerName, _ := api.findDuplicateURLOwnerLocked(existing.SourceURLs, existing.ID); ownerID != "" {
		api.mu.Unlock()
		http.Error(w, fmt.Sprintf("Bu yayın adresi zaten '%s' (%s) kanalında kayıtlı. Mükerrer adres eklemek yerine kanalları 'Birleştir' ile birleştirin.", ownerName, ownerID), http.StatusConflict)
		return
	}
	if req.LogoURL != "" {
		existing.LogoURL = req.LogoURL
	}
	if req.EPGID != "" {
		existing.EPGID = req.EPGID
		epg.GlobalEPG.MapChannel(existing.ID, req.EPGID)
	}

	api.streams[req.ID] = existing
	api.saveStateLocked()
	api.mu.Unlock()

	// Stop old pipeline so new source/tier takes effect immediately on next playback
	if api.hooks.OnStop != nil {
		api.hooks.OnStop(existing.ID)
	}
	if api.hooks.OnUpsert != nil {
		api.hooks.OnUpsert(existing.ID, existing.Tier, existing.SourceURLs)
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Stream updated successfully"})
}

func (api *AdminAPI) handleM3UImport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 32*1024*1024))
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req M3UImportRequest
	if err := json.Unmarshal(body, &req); err != nil || req.M3UContent == "" {
		req.M3UContent = strings.TrimSpace(string(body))
		req.AutoPrune = true
		req.Tier = auth.TierFree
	}

	if req.Tier == "" {
		req.Tier = auth.TierFree
	}

	m3uSourceURL := ""
	m3uText := strings.TrimSpace(req.M3UContent)
	if strings.HasPrefix(m3uText, "http://") || strings.HasPrefix(m3uText, "https://") {
		m3uSourceURL = m3uText
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: req.InsecureTLS},
		}
		client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
		resp, fetchErr := client.Get(m3uText)
		if fetchErr != nil {
			http.Error(w, fmt.Sprintf("Failed to download M3U URL: %v", fetchErr), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			http.Error(w, fmt.Sprintf("M3U URL returned HTTP %d", resp.StatusCode), http.StatusBadGateway)
			return
		}
		b, readErr := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
		if readErr != nil {
			http.Error(w, "Failed to read M3U response body", http.StatusBadGateway)
			return
		}
		m3uText = string(b)
	}

	liveChannels, prunedCount := playlist.ImportAndPruneM3U(m3uText, req.AutoPrune)

	addedCount := 0
	mergedCount := 0
	skippedDeletedCount := 0
	var upserts []StreamRequest

	api.mu.Lock()
	if m3uSourceURL != "" {
		found := false
		for _, src := range api.m3uSources {
			if src.URL == m3uSourceURL {
				found = true
				break
			}
		}
		if !found {
			api.m3uSources = append(api.m3uSources, storage.M3USource{
				ID:          fmt.Sprintf("m3u_%d", time.Now().UnixNano()),
				URL:         m3uSourceURL,
				Language:    req.Language,
				ContentType: req.ContentType,
				AutoPrune:   req.AutoPrune,
			})
		}
	}

	// Build canonical URL ownership map so identical stream URLs never create duplicate channels
	urlOwner := make(map[string]string, len(api.streams)*2)
	for id, s := range api.streams {
		for _, u := range append([]string{s.SourceURL}, s.SourceURLs...) {
			if c := canonicalStreamURL(u); c != "" {
				urlOwner[c] = id
			}
		}
	}

	dedupedURLCount := 0
	for _, ch := range liveChannels {
		if api.deletedChannelIDs[ch.ID] {
			skippedDeletedCount++
			continue
		}

		normName := normalizeName(ch.Name)
		canonURL := canonicalStreamURL(ch.SourceURL)

		if existingID, urlDup := urlOwner[canonURL]; urlDup && canonURL != "" {
			existingStream := api.streams[existingID]
			changed := false
			if existingStream.LogoURL == "" && ch.Logo != "" {
				existingStream.LogoURL = ch.Logo
				changed = true
			}
			if changed {
				api.streams[existingID] = existingStream
				upserts = append(upserts, existingStream)
			}
			dedupedURLCount++
			mergedCount++
		} else if existingID, found := api.normalizedMap[normName]; found {
			existingStream := api.streams[existingID]
			existingStream.SourceURLs = uniqueStrings(append([]string{existingStream.SourceURL}, append(existingStream.SourceURLs, ch.SourceURL)...))
			if len(existingStream.SourceURLs) > 0 {
				existingStream.SourceURL = existingStream.SourceURLs[0]
			}
			if existingStream.LogoURL == "" && ch.Logo != "" {
				existingStream.LogoURL = ch.Logo
			}
			api.streams[existingID] = existingStream
			if canonURL != "" {
				urlOwner[canonURL] = existingID
			}
			upserts = append(upserts, existingStream)
			mergedCount++
		} else {
			streamReq := StreamRequest{
				ID:         ch.ID,
				Name:       ch.Name,
				Tier:       req.Tier,
				SourceURL:  ch.SourceURL,
				SourceURLs: uniqueStrings([]string{ch.SourceURL}),
				LogoURL:    ch.Logo,
			}
			api.streams[ch.ID] = streamReq
			api.normalizedMap[normName] = ch.ID
			if canonURL != "" {
				urlOwner[canonURL] = ch.ID
			}
			upserts = append(upserts, streamReq)
			addedCount++
		}
	}

	api.saveStateLocked()
	api.mu.Unlock()

	if api.hooks.OnUpsert != nil {
		for _, s := range upserts {
			api.hooks.OnUpsert(s.ID, s.Tier, s.SourceURLs)
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":            "success",
		"added_count":       addedCount,
		"merged_count":      mergedCount,
		"deduped_url_count": dedupedURLCount,
		"pruned_count":      prunedCount,
		"skipped_deleted":   skippedDeletedCount,
		"total_parsed":      addedCount + mergedCount + prunedCount,
		"live_channels":     liveChannels,
	})
}

func (api *AdminAPI) handleMergeStreams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TargetID  string   `json:"target_id"`
		SourceIDs []string `json:"source_ids"`
		NewName   string   `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.TargetID) == "" || len(req.SourceIDs) == 0 {
		http.Error(w, "Geçersiz birleştirme isteği (hedef ve kaynak kanallar gerekli)", http.StatusBadRequest)
		return
	}

	targetID := strings.TrimSpace(req.TargetID)
	var mergedIDs []string
	var targetAfter StreamRequest

	api.mu.Lock()
	target, exists := api.streams[targetID]
	if !exists {
		api.mu.Unlock()
		http.Error(w, "Hedef kanal bulunamadı", http.StatusNotFound)
		return
	}
	if newName := strings.TrimSpace(req.NewName); newName != "" {
		target.Name = newName
		api.streams[targetID] = target
	}
	for _, rawSrcID := range req.SourceIDs {
		srcID := strings.TrimSpace(rawSrcID)
		if srcID == "" || srcID == targetID {
			continue
		}
		if api.mergeChannelIntoLocked(targetID, srcID) {
			mergedIDs = append(mergedIDs, srcID)
		}
	}
	if len(mergedIDs) == 0 {
		api.mu.Unlock()
		http.Error(w, "Birleştirilecek geçerli kaynak kanal bulunamadı", http.StatusBadRequest)
		return
	}
	api.rebuildNormalizedMapLocked()
	targetAfter = api.streams[targetID]
	api.saveStateLocked()
	api.mu.Unlock()

	for _, sid := range mergedIDs {
		if api.hooks.OnDelete != nil {
			api.hooks.OnDelete(sid)
		}
	}
	if api.hooks.OnStop != nil {
		api.hooks.OnStop(targetAfter.ID)
	}
	if api.hooks.OnUpsert != nil {
		api.hooks.OnUpsert(targetAfter.ID, targetAfter.Tier, targetAfter.SourceURLs)
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "success",
		"merged_count":   len(mergedIDs),
		"merged_ids":     mergedIDs,
		"target_channel": targetAfter,
	})
}

func (api *AdminAPI) handleDedupeStreams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	merged := api.DeduplicateByURL()
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "success",
		"merged_count": merged,
	})
}

func (api *AdminAPI) handleM3USources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodDelete {
		id := r.URL.Query().Get("id")
		api.mu.Lock()
		var newSources []storage.M3USource
		for _, s := range api.m3uSources {
			if s.ID != id {
				newSources = append(newSources, s)
			}
		}
		api.m3uSources = newSources
		api.saveStateLocked()
		api.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "M3U source removed"})
		return
	}

	api.mu.RLock()
	sourcesCopy := append([]storage.M3USource(nil), api.m3uSources...)
	api.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(sourcesCopy)
}

func (api *AdminAPI) handleUsers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodPost {
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			http.Error(w, "Invalid user payload", http.StatusBadRequest)
			return
		}
		req.UserID = strings.TrimSpace(req.UserID)

		if req.Tier == "" {
			req.Tier = auth.TierPaid
		}

		token, err := api.authManager.GenerateToken(req.UserID, req.Tier, 24*30)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		userAcc := storage.UserAccount{
			UserID:    req.UserID,
			Tier:      req.Tier,
			Token:     token,
			CreatedAt: time.Now().Format("2006-01-02 15:04:05"),
		}

		api.mu.Lock()
		api.users[req.UserID] = userAcc
		api.saveStateLocked()
		api.mu.Unlock()

		_ = json.NewEncoder(w).Encode(userAcc)
		return
	}

	if r.Method == http.MethodDelete {
		uid := r.URL.Query().Get("user_id")
		api.mu.Lock()
		delete(api.users, uid)
		api.saveStateLocked()
		api.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "User deleted"})
		return
	}

	api.mu.RLock()
	usersCopy := make(map[string]storage.UserAccount, len(api.users))
	for k, v := range api.users {
		v.PasswordHash = ""
		usersCopy[k] = v
	}
	api.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(usersCopy)
}

func (api *AdminAPI) handleEPGSource(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req EPGSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, "Invalid EPG source payload", http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		req.ID = fmt.Sprintf("epg_%d", time.Now().UnixNano())
	}

	chCount, progCount, err := epg.GlobalEPG.AddSource(req.ID, req.URL, req.TimeOffset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	api.mu.Lock()
	api.saveStateLocked()
	api.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "success",
		"message":         "EPG source downloaded and synchronized successfully",
		"channel_count":   chCount,
		"programme_count": progCount,
	})
}

func (api *AdminAPI) handleEPGSources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodDelete {
		id := r.URL.Query().Get("id")
		epg.GlobalEPG.RemoveSource(id)
		api.mu.Lock()
		api.saveStateLocked()
		api.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "EPG source removed"})
		return
	}
	stats := epg.GlobalEPG.GetSourceStats()
	_ = json.NewEncoder(w).Encode(stats)
}

func (api *AdminAPI) handleGetEPGChannels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	channels := epg.GlobalEPG.GetAllUpstreamChannels()
	_ = json.NewEncoder(w).Encode(channels)
}

func (api *AdminAPI) handleAutoMapEPG(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	epgChannels := epg.GlobalEPG.GetAllUpstreamChannels()

	// Build O(M) normalized lookup map once outside api.mu lock.
	// Prefer .tr channels when multiple countries share a normalized channel name.
	normIndex := make(map[string]string, len(epgChannels)*2)
	setIndex := func(normKey, epgID string) {
		if normKey == "" || epgID == "" {
			return
		}
		existing, exists := normIndex[normKey]
		if !exists {
			normIndex[normKey] = epgID
			return
		}
		isNewTR := strings.HasSuffix(strings.ToLower(epgID), ".tr")
		isOldTR := strings.HasSuffix(strings.ToLower(existing), ".tr")
		if isNewTR && !isOldTR {
			normIndex[normKey] = epgID
		}
	}

	for _, epgCh := range epgChannels {
		setIndex(normalizeEPGKey(epgCh.ID), epgCh.ID)
		for _, disp := range epgCh.DisplayNames {
			setIndex(normalizeEPGKey(disp.Value), epgCh.ID)
		}
	}

	mappedCount := 0
	api.mu.Lock()
	for id, stream := range api.streams {
		streamNorm := normalizeName(stream.Name)
		if epgID, ok := normIndex[streamNorm]; ok {
			epg.GlobalEPG.MapChannel(id, epgID)
			stream.EPGID = epgID
			api.streams[id] = stream
			mappedCount++
		}
	}
	api.saveStateLocked()
	api.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "success",
		"mapped_count": mappedCount,
	})
}

func (api *AdminAPI) handleEPGMap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req EPGMapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.InternalID == "" || req.EPGID == "" {
		http.Error(w, "Invalid EPG mapping payload", http.StatusBadRequest)
		return
	}

	epg.GlobalEPG.MapChannel(req.InternalID, req.EPGID)
	api.mu.Lock()
	if s, ok := api.streams[req.InternalID]; ok {
		s.EPGID = req.EPGID
		api.streams[req.InternalID] = s
	}
	api.saveStateLocked()
	api.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Channel EPG mapped successfully"})
}

func (api *AdminAPI) handleClientEPGXML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	cleanXML, err := epg.GlobalEPG.GenerateCleanXML()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(cleanXML)
}

func (api *AdminAPI) handleClientEPGJson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	guide := epg.GlobalEPG.GetClientEPGJson()
	_ = json.NewEncoder(w).Encode(guide)
}

func (api *AdminAPI) handleExportBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=iptv_p2p_backup.json")

	api.mu.RLock()
	streamsCopy := make(map[string]StreamRequest, len(api.streams))
	for k, v := range api.streams {
		v.IsStreaming = false
		streamsCopy[k] = v
	}
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(streamsCopy)
}

func (api *AdminAPI) handleImportBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 32*1024*1024))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	var importedStreams map[string]StreamRequest
	if err := json.Unmarshal(body, &importedStreams); err != nil {
		http.Error(w, "Invalid backup JSON", http.StatusBadRequest)
		return
	}

	var upserts []StreamRequest

	api.mu.Lock()
	for id, s := range importedStreams {
		if api.deletedChannelIDs[id] {
			continue
		}
		if s.ID == "" {
			s.ID = id
		}
		if s.SourceURL == "" && len(s.SourceURLs) > 0 {
			s.SourceURL = s.SourceURLs[0]
		}
		if len(s.SourceURLs) == 0 && s.SourceURL != "" {
			s.SourceURLs = []string{s.SourceURL}
		}
		s.SourceURLs = uniqueStrings(s.SourceURLs)
		if s.Tier == "" {
			s.Tier = auth.TierFree
		}
		api.streams[id] = s
		api.normalizedMap[normalizeName(s.Name)] = id
		upserts = append(upserts, s)
	}
	api.saveStateLocked()
	api.mu.Unlock()

	if api.hooks.OnUpsert != nil {
		for _, s := range upserts {
			api.hooks.OnUpsert(s.ID, s.Tier, s.SourceURLs)
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Backup restored successfully"})
}

func (api *AdminAPI) handleSwarms(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Project", "IPTV-P2P")
	w.Header().Set("Content-Type", "application/json")
	stats := api.tracker.GetSwarmStats()
	_ = json.NewEncoder(w).Encode(stats)
}

func (api *AdminAPI) handleGenerateToken(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	userID := r.URL.Query().Get("user_id")
	tier := r.URL.Query().Get("tier")
	if userID == "" {
		userID = "user_demo"
	}
	if tier == "" || tier == auth.TierAdmin {
		tier = auth.TierPaid
	}

	token, err := api.authManager.GenerateToken(userID, tier, 24*30)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{
		"user_id": userID,
		"tier":    tier,
		"token":   token,
	})
}

func (api *AdminAPI) getAceManager() *acestream.Manager {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.aceManager
}

func (api *AdminAPI) handleAceStreamStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mgr := api.getAceManager()
	if mgr == nil {
		_ = json.NewEncoder(w).Encode(acestream.EngineStatus{Enabled: false})
		return
	}
	_ = json.NewEncoder(w).Encode(mgr.GetStatus())
}

func (api *AdminAPI) handleAceStreamStart(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	mgr := api.getAceManager()
	if mgr == nil {
		http.Error(w, "AceStream manager not initialized", http.StatusServiceUnavailable)
		return
	}
	if err := mgr.EnsurePrimaryRunning(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"engine": mgr.GetStatus(),
	})
}

func (api *AdminAPI) handleAceStreamStop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	mgr := api.getAceManager()
	if mgr != nil {
		mgr.StopAll()
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "AceStream sessions and managed engines stopped"})
}

func (api *AdminAPI) handleAceStreamSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mgr := api.getAceManager()
	if mgr == nil || !mgr.IsEnabled() {
		http.Error(w, "AceStream engine is disabled", http.StatusServiceUnavailable)
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		q = strings.TrimSpace(r.URL.Query().Get("query"))
	}
	if q == "" {
		http.Error(w, "Search query (q) is required", http.StatusBadRequest)
		return
	}
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	res, err := mgr.Search(r.Context(), q, category, 0, 50)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(res)
}

func (api *AdminAPI) handleAceStreamAdd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Name      string `json:"name"`
		SourceURL string `json:"source_url"`
		Tier      string `json:"tier"`
		LogoURL   string `json:"logo_url"`
		EPGID     string `json:"epg_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.SourceURL = strings.TrimSpace(req.SourceURL)
	if req.Name == "" || req.SourceURL == "" {
		http.Error(w, "name and source_url are required", http.StatusBadRequest)
		return
	}
	if !acestream.IsAceStreamURL(req.SourceURL) {
		http.Error(w, "Invalid AceStream source_url", http.StatusBadRequest)
		return
	}
	if _, _, parseErr := acestream.ParseSource(req.SourceURL); parseErr != nil {
		http.Error(w, parseErr.Error(), http.StatusBadRequest)
		return
	}
	if len(req.SourceURL) == 40 && !strings.HasPrefix(strings.ToLower(req.SourceURL), "acestream://") {
		req.SourceURL = "acestream://" + req.SourceURL
	}
	if req.Tier == "" {
		req.Tier = auth.TierFree
	}

	sum := sha256.Sum256([]byte(req.SourceURL + "|" + req.Name))
	chID := "ace_" + hex.EncodeToString(sum[:5])

	streamReq := StreamRequest{
		ID:          chID,
		Name:        req.Name,
		Tier:        req.Tier,
		SourceURL:   req.SourceURL,
		SourceURLs:  []string{req.SourceURL},
		LogoURL:     strings.TrimSpace(req.LogoURL),
		EPGID:       strings.TrimSpace(req.EPGID),
		IsAceStream: true,
	}

	api.mu.Lock()
	delete(api.deletedChannelIDs, streamReq.ID)
	api.streams[streamReq.ID] = streamReq
	api.normalizedMap[normalizeName(streamReq.Name)] = streamReq.ID
	api.saveStateLocked()
	api.mu.Unlock()

	if streamReq.EPGID != "" {
		epg.GlobalEPG.MapChannel(streamReq.ID, streamReq.EPGID)
	}
	if api.hooks.OnUpsert != nil {
		api.hooks.OnUpsert(streamReq.ID, streamReq.Tier, streamReq.SourceURLs)
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"channel": streamReq,
	})
}
