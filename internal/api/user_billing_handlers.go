package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"iptv-p2p-server/internal/auth"
	"iptv-p2p-server/internal/billing"
	"iptv-p2p-server/internal/storage"
)

const userSessionCookieName = "iptv_session"

var usernameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,32}$`)

type PublicUserDTO struct {
	UserID    string   `json:"user_id"`
	Tier      string   `json:"tier"`
	PaidUntil string   `json:"paid_until,omitempty"`
	Favorites []string `json:"favorites"`
}

type PublicOrderDTO struct {
	OrderID         string  `json:"order_id"`
	Username        string  `json:"username"`
	PlanID          string  `json:"plan_id"`
	DurationDays    int     `json:"duration_days"`
	BaseAmountUSDT  float64 `json:"base_amount_usdt"`
	ExactAmountUSDT string  `json:"exact_amount_usdt"`
	USDTAddress     string  `json:"usdt_address"`
	USDTNetwork     string  `json:"usdt_network"`
	Status          string  `json:"status"`
	TxHash          string  `json:"tx_hash,omitempty"`
	ExpiresAtUnix   int64   `json:"expires_at_unix"`
	ConfirmedAt     string  `json:"confirmed_at,omitempty"`
}

func toPublicOrderDTO(o storage.PaymentOrder) PublicOrderDTO {
	return PublicOrderDTO{
		OrderID:         o.OrderID,
		Username:        o.Username,
		PlanID:          o.PlanID,
		DurationDays:    o.DurationDays,
		BaseAmountUSDT:  o.BaseAmountUSDT,
		ExactAmountUSDT: o.ExactAmountUSDT,
		USDTAddress:     o.USDTAddress,
		USDTNetwork:     o.USDTNetwork,
		Status:          o.Status,
		TxHash:          o.TxHash,
		ExpiresAtUnix:   o.ExpiresAtUnix,
		ConfirmedAt:     o.ConfirmedAt,
	}
}

func (api *AdminAPI) registerUserBillingRoutes(mux *http.ServeMux) {
	// Public Auth, Registration, USDT Payment & Plans
	mux.HandleFunc("/api/billing/plans", api.handlePublicBillingPlans)
	mux.HandleFunc("/api/auth/register", api.handleUserRegister)
	mux.HandleFunc("/api/auth/login", api.handleUserLogin)
	mux.HandleFunc("/api/auth/logout", api.handleUserLogout)
	mux.HandleFunc("/api/auth/me", api.handleUserMe)
	mux.HandleFunc("/api/auth/upgrade", api.handleUserUpgrade)
	mux.HandleFunc("/api/auth/payment-status", api.handlePaymentStatus)
	mux.HandleFunc("/api/auth/verify-tx", api.handleVerifyTxHash)

	// Registered User Favorites
	mux.HandleFunc("/api/user/favorites", api.handleUserFavorites)
	mux.HandleFunc("/api/user/favorites/toggle", api.handleToggleFavorite)

	// Live Channel Viewers
	mux.HandleFunc("/api/channels/viewers", api.handleChannelViewers)
	mux.HandleFunc("/api/channels/heartbeat", api.handleChannelHeartbeat)

	// Admin Billing Settings & Orders
	mux.HandleFunc("/api/admin/billing/settings", api.requireAdmin(api.handleAdminBillingSettings))
	mux.HandleFunc("/api/admin/billing/orders", api.requireAdmin(api.handleAdminBillingOrders))
	mux.HandleFunc("/api/admin/billing/orders/approve", api.requireAdmin(api.handleAdminApproveOrder))
}

// StartBillingPoller polls the blockchain every 10 seconds whenever pending USDT orders exist
func (api *AdminAPI) StartBillingPoller() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			api.pollPendingOrders()
		}
	}()
}

func generateRandomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func isSecureRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func setUserSessionCookie(w http.ResponseWriter, r *http.Request, token string, maxAgeSec int) {
	secure := isSecureRequest(r)
	sameSite := "Lax"
	extra := ""
	if secure {
		sameSite = "None"
		extra = "; Secure; Partitioned"
	}
	cookieVal := fmt.Sprintf("%s=%s; Path=/; HttpOnly; Max-Age=%d; SameSite=%s%s", userSessionCookieName, token, maxAgeSec, sameSite, extra)
	w.Header().Add("Set-Cookie", cookieVal)
}

func checkSameOriginOrHeader(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get("X-Requested-With"), "XMLHttpRequest") {
		return true
	}
	if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		return true
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin != "" {
		host := r.Header.Get("X-Forwarded-Host")
		if host == "" {
			host = r.Host
		}
		if strings.HasSuffix(origin, "://"+host) {
			return true
		}
	}
	return false
}

func extractUserSessionToken(r *http.Request) string {
	authHdr := r.Header.Get("Authorization")
	if strings.HasPrefix(authHdr, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(authHdr, "Bearer "))
	}
	if c, err := r.Cookie(userSessionCookieName); err == nil && strings.TrimSpace(c.Value) != "" {
		return strings.TrimSpace(c.Value)
	}
	if q := strings.TrimSpace(r.URL.Query().Get("token")); q != "" {
		return q
	}
	return ""
}

func (api *AdminAPI) resolvePlan(planID string, b storage.BillingSettings) (float64, int, bool) {
	switch strings.ToLower(strings.TrimSpace(planID)) {
	case "monthly", "paid", "1m":
		if b.MonthlyPriceUSDT <= 0 {
			return 0, 0, false
		}
		return b.MonthlyPriceUSDT, 30, true
	case "quarterly", "3m":
		if b.QuarterlyPriceUSDT <= 0 {
			return 0, 0, false
		}
		return b.QuarterlyPriceUSDT, 90, true
	case "yearly", "12m":
		if b.YearlyPriceUSDT <= 0 {
			return 0, 0, false
		}
		return b.YearlyPriceUSDT, 365, true
	default:
		return 0, 0, false
	}
}

func (api *AdminAPI) effectiveUserTierLocked(u storage.UserAccount) (storage.UserAccount, bool) {
	changed := false
	if u.Tier == auth.TierPaid && u.PaidUntil != "" {
		if exp, err := time.Parse(time.RFC3339, u.PaidUntil); err == nil {
			if time.Now().After(exp) {
				u.Tier = auth.TierFree
				u.PaidUntil = ""
				if tok, tErr := api.authManager.GenerateToken(u.UserID, auth.TierFree, 24*30); tErr == nil {
					u.Token = tok
				}
				api.users[u.UserID] = u
				changed = true
			}
		}
	}
	return u, changed
}

func (api *AdminAPI) allocateUniqueExactAmountLocked(base float64) string {
	usedAmounts := make(map[string]bool)
	nowUnix := time.Now().Unix()
	for _, ord := range api.paymentOrders {
		if ord.Status == "pending" && ord.ExpiresAtUnix > nowUnix {
			usedAmounts[ord.ExactAmountUSDT] = true
		}
	}
	// Try random 4-decimal suffix 0.0001..0.0999 to keep invoices distinct on-chain
	for i := 0; i < 200; i++ {
		nBig, _ := rand.Int(rand.Reader, big.NewInt(999))
		suffix := int(nBig.Int64()) + 1
		candidate := fmt.Sprintf("%.4f", base+float64(suffix)/10000.0)
		if !usedAmounts[candidate] {
			return candidate
		}
	}
	return fmt.Sprintf("%.4f", base+float64((time.Now().UnixNano()%999)+1)/10000.0)
}

func (api *AdminAPI) handlePublicBillingPlans(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	api.mu.RLock()
	b := api.billing
	api.mu.RUnlock()

	configured := strings.TrimSpace(b.USDTAddress) != "" &&
		(b.MonthlyPriceUSDT > 0 || b.QuarterlyPriceUSDT > 0 || b.YearlyPriceUSDT > 0)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"configured":           configured,
		"usdt_network":         billing.NormalizeNetwork(b.USDTNetwork),
		"monthly_price_usdt":   b.MonthlyPriceUSDT,
		"quarterly_price_usdt": b.QuarterlyPriceUSDT,
		"yearly_price_usdt":    b.YearlyPriceUSDT,
		"auto_verify_enabled":  b.AutoVerifyEnabled,
	})
}

func (api *AdminAPI) handleUserRegister(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Plan     string `json:"plan"` // free, monthly, quarterly, yearly
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Geçersiz istek formatı"})
		return
	}

	username := strings.TrimSpace(req.Username)
	if !usernameRegex.MatchString(username) || strings.EqualFold(username, "admin") || strings.EqualFold(username, "guest_anon") {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Kullanıcı adı 3-32 karakter olmalı (harf, rakam, _, ., -)"})
		return
	}
	if len(req.Password) < 6 || len(req.Password) > 128 {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Şifre en az 6 karakter olmalıdır"})
		return
	}

	plan := strings.ToLower(strings.TrimSpace(req.Plan))
	if plan == "" {
		plan = "free"
	}

	pwHash, err := api.authManager.HashPassword(req.Password)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Şifre işlenemedi"})
		return
	}

	api.mu.Lock()
	defer api.mu.Unlock()

	for existingName := range api.users {
		if strings.EqualFold(existingName, username) {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Bu kullanıcı adı zaten kayıtlı"})
			return
		}
	}

	if plan == "free" {
		tok, tErr := api.authManager.GenerateToken(username, auth.TierFree, 24*30)
		if tErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Oturum oluşturulamadı"})
			return
		}
		acc := storage.UserAccount{
			UserID:       username,
			Tier:         auth.TierFree,
			Token:        tok,
			PasswordHash: pwHash,
			Favorites:    []string{},
			CreatedAt:    time.Now().Format("2006-01-02 15:04:05"),
		}
		api.users[username] = acc
		api.saveStateLocked()

		setUserSessionCookie(w, r, tok, 24*30*3600)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "active",
			"token":  tok,
			"user": PublicUserDTO{
				UserID:    acc.UserID,
				Tier:      acc.Tier,
				Favorites: acc.Favorites,
			},
		})
		return
	}

	// Paid plan registration -> Create USDT payment order
	if strings.TrimSpace(api.billing.USDTAddress) == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Admin tarafından henüz USDT cüzdan adresi tanımlanmamış"})
		return
	}
	basePrice, days, validPlan := api.resolvePlan(plan, api.billing)
	if !validPlan {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Seçilen ücretli paket şu an aktif değil"})
		return
	}

	now := time.Now()
	order := storage.PaymentOrder{
		OrderID:         "ord_" + generateRandomHex(8),
		Username:        username,
		PasswordHash:    pwHash,
		PlanID:          plan,
		DurationDays:    days,
		BaseAmountUSDT:  basePrice,
		ExactAmountUSDT: api.allocateUniqueExactAmountLocked(basePrice),
		USDTAddress:     strings.TrimSpace(api.billing.USDTAddress),
		USDTNetwork:     billing.NormalizeNetwork(api.billing.USDTNetwork),
		Status:          "pending",
		CreatedAtUnix:   now.Unix(),
		ExpiresAtUnix:   now.Add(45 * time.Minute).Unix(),
	}
	api.paymentOrders[order.OrderID] = order
	api.saveStateLocked()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "payment_required",
		"order":  toPublicOrderDTO(order),
	})
}

func (api *AdminAPI) handleUserUpgrade(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if !checkSameOriginOrHeader(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	tok := extractUserSessionToken(r)
	claims, err := api.authManager.ValidateToken(tok)
	if err != nil || claims.Tier == auth.TierGuest || claims.UserID == "" || claims.UserID == "guest_anon" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Paket yükseltmek için önce giriş yapmalısınız"})
		return
	}

	var req struct {
		Plan string `json:"plan"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Geçersiz istek"})
		return
	}

	api.mu.Lock()
	defer api.mu.Unlock()

	if _, exists := api.users[claims.UserID]; !exists {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Kullanıcı bulunamadı"})
		return
	}
	if strings.TrimSpace(api.billing.USDTAddress) == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Admin tarafından henüz USDT cüzdan adresi tanımlanmamış"})
		return
	}
	basePrice, days, validPlan := api.resolvePlan(req.Plan, api.billing)
	if !validPlan {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Geçersiz ücretli paket"})
		return
	}

	now := time.Now()
	order := storage.PaymentOrder{
		OrderID:         "ord_" + generateRandomHex(8),
		Username:        claims.UserID,
		PlanID:          req.Plan,
		DurationDays:    days,
		BaseAmountUSDT:  basePrice,
		ExactAmountUSDT: api.allocateUniqueExactAmountLocked(basePrice),
		USDTAddress:     strings.TrimSpace(api.billing.USDTAddress),
		USDTNetwork:     billing.NormalizeNetwork(api.billing.USDTNetwork),
		Status:          "pending",
		UpgradeOnly:     true,
		CreatedAtUnix:   now.Unix(),
		ExpiresAtUnix:   now.Add(45 * time.Minute).Unix(),
	}
	api.paymentOrders[order.OrderID] = order
	api.saveStateLocked()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "payment_required",
		"order":  toPublicOrderDTO(order),
	})
}

func (api *AdminAPI) handleUserLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Geçersiz istek"})
		return
	}
	username := strings.TrimSpace(req.Username)

	if api.authManager.CheckAdminCredentials(username, req.Password) {
		tok, err := api.authManager.GenerateToken(username, auth.TierAdmin, 24*30)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Oturum oluşturulamadı"})
			return
		}
		api.mu.RLock()
		var favs []string
		if acc, ok := api.users[username]; ok && len(acc.Favorites) > 0 {
			favs = append([]string(nil), acc.Favorites...)
		}
		api.mu.RUnlock()
		if favs == nil {
			favs = []string{}
		}
		setUserSessionCookie(w, r, tok, 24*30*3600)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"token":  tok,
			"user": PublicUserDTO{
				UserID:    username,
				Tier:      auth.TierAdmin,
				Favorites: favs,
			},
		})
		return
	}

	api.mu.Lock()
	defer api.mu.Unlock()

	var matchedKey string
	var acc storage.UserAccount
	found := false
	for k, u := range api.users {
		if strings.EqualFold(k, username) {
			matchedKey = k
			acc = u
			found = true
			break
		}
	}

	if !found || !api.authManager.CheckPassword(acc.PasswordHash, req.Password) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Kullanıcı adı veya şifre hatalı"})
		return
	}

	acc, expiredChanged := api.effectiveUserTierLocked(acc)
	tok, err := api.authManager.GenerateToken(matchedKey, acc.Tier, 24*30)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Oturum oluşturulamadı"})
		return
	}
	acc.Token = tok
	api.users[matchedKey] = acc
	if expiredChanged {
		api.saveStateLocked()
	} else {
		api.saveStateLocked()
	}

	favs := acc.Favorites
	if favs == nil {
		favs = []string{}
	}

	setUserSessionCookie(w, r, tok, 24*30*3600)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"token":  tok,
		"user": PublicUserDTO{
			UserID:    acc.UserID,
			Tier:      acc.Tier,
			PaidUntil: acc.PaidUntil,
			Favorites: favs,
		},
	})
}

func (api *AdminAPI) handleUserLogout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if !checkSameOriginOrHeader(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	setUserSessionCookie(w, r, "", -1)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func (api *AdminAPI) handleUserMe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tok := extractUserSessionToken(r)
	if tok == "" {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"authenticated": false,
			"tier":          auth.TierGuest,
			"favorites":     []string{},
		})
		return
	}

	claims, err := api.authManager.ValidateToken(tok)
	if err != nil || claims.Tier == auth.TierGuest || claims.UserID == "" || claims.UserID == "guest_anon" {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"authenticated": false,
			"tier":          auth.TierGuest,
			"favorites":     []string{},
		})
		return
	}

	if claims.Tier == auth.TierAdmin {
		api.mu.RLock()
		var favs []string
		if acc, ok := api.users[claims.UserID]; ok && len(acc.Favorites) > 0 {
			favs = append([]string(nil), acc.Favorites...)
		}
		api.mu.RUnlock()
		if favs == nil {
			favs = []string{}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"authenticated": true,
			"token":         tok,
			"user": PublicUserDTO{
				UserID:    claims.UserID,
				Tier:      auth.TierAdmin,
				Favorites: favs,
			},
		})
		return
	}

	api.mu.Lock()
	acc, exists := api.users[claims.UserID]
	if !exists {
		api.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"authenticated": false,
			"tier":          auth.TierGuest,
			"favorites":     []string{},
		})
		return
	}
	acc, changed := api.effectiveUserTierLocked(acc)
	if changed {
		api.saveStateLocked()
		tok = acc.Token
		setUserSessionCookie(w, r, tok, 24*30*3600)
	}
	favs := append([]string(nil), acc.Favorites...)
	if favs == nil {
		favs = []string{}
	}
	api.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"authenticated": true,
		"token":         tok,
		"user": PublicUserDTO{
			UserID:    acc.UserID,
			Tier:      acc.Tier,
			PaidUntil: acc.PaidUntil,
			Favorites: favs,
		},
	})
}

func (api *AdminAPI) completePaymentOrderLocked(orderID, txHash string) (storage.PaymentOrder, storage.UserAccount, error) {
	ord, ok := api.paymentOrders[orderID]
	if !ok {
		return storage.PaymentOrder{}, storage.UserAccount{}, fmt.Errorf("sipariş bulunamadı")
	}
	if ord.Status == "confirmed" {
		u := api.users[ord.Username]
		return ord, u, nil
	}
	if ord.Status == "expired" && !ord.UpgradeOnly && ord.PasswordHash == "" {
		return storage.PaymentOrder{}, storage.UserAccount{}, fmt.Errorf("bu siparişin süresi dolmuş, lütfen yeni kayıt/ödeme başlatın")
	}
	if txHash != "" {
		if prevOrder, used := api.usedTxHashes[strings.ToLower(txHash)]; used && prevOrder != orderID {
			return storage.PaymentOrder{}, storage.UserAccount{}, fmt.Errorf("bu TX Hash başka bir siparişte kullanılmış")
		}
		api.usedTxHashes[strings.ToLower(txHash)] = orderID
	}

	now := time.Now()
	days := ord.DurationDays
	if days <= 0 {
		days = 30
	}

	var acc storage.UserAccount
	if existing, exists := api.users[ord.Username]; exists {
		acc = existing
		if acc.Favorites == nil {
			acc.Favorites = []string{}
		}
		baseTime := now
		if acc.PaidUntil != "" {
			if t, err := time.Parse(time.RFC3339, acc.PaidUntil); err == nil && t.After(now) {
				baseTime = t
			}
		}
		acc.Tier = auth.TierPaid
		acc.PaidUntil = baseTime.AddDate(0, 0, days).Format(time.RFC3339)
		if !ord.UpgradeOnly && ord.PasswordHash != "" {
			acc.PasswordHash = ord.PasswordHash
		}
	} else {
		acc = storage.UserAccount{
			UserID:       ord.Username,
			Tier:         auth.TierPaid,
			PasswordHash: ord.PasswordHash,
			Favorites:    []string{},
			PaidUntil:    now.AddDate(0, 0, days).Format(time.RFC3339),
			CreatedAt:    now.Format("2006-01-02 15:04:05"),
		}
	}

	tok, err := api.authManager.GenerateToken(acc.UserID, auth.TierPaid, 24*days)
	if err != nil {
		return storage.PaymentOrder{}, storage.UserAccount{}, err
	}
	acc.Token = tok
	api.users[acc.UserID] = acc

	ord.Status = "confirmed"
	ord.TxHash = txHash
	ord.ConfirmedAt = now.Format("2006-01-02 15:04:05")
	ord.PasswordHash = "" // wipe order's temporary hash copy once transferred
	api.paymentOrders[orderID] = ord
	api.saveStateLocked()

	log.Printf("💰 [BILLING] Auto-confirmed USDT order %s for user %s (%s %s USDT, TX: %s)", orderID, acc.UserID, ord.ExactAmountUSDT, ord.USDTNetwork, txHash)
	return ord, acc, nil
}

func (api *AdminAPI) pollPendingOrders() {
	api.mu.Lock()
	if !api.billing.AutoVerifyEnabled || strings.TrimSpace(api.billing.USDTAddress) == "" {
		api.mu.Unlock()
		return
	}
	nowUnix := time.Now().Unix()
	type pendingGroupKey struct {
		net  string
		addr string
	}
	groups := make(map[pendingGroupKey][]storage.PaymentOrder)
	expiredAny := false

	for id, ord := range api.paymentOrders {
		if ord.Status != "pending" {
			continue
		}
		if ord.ExpiresAtUnix > 0 && nowUnix > ord.ExpiresAtUnix {
			ord.Status = "expired"
			ord.PasswordHash = ""
			api.paymentOrders[id] = ord
			expiredAny = true
			continue
		}
		key := pendingGroupKey{
			net:  billing.NormalizeNetwork(ord.USDTNetwork),
			addr: strings.TrimSpace(ord.USDTAddress),
		}
		groups[key] = append(groups[key], ord)
	}
	if expiredAny {
		api.saveStateLocked()
	}
	api.mu.Unlock()

	if len(groups) == 0 {
		return
	}

	for key, orders := range groups {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		transfers, err := billing.FetchIncomingUSDTTransfers(ctx, key.net, key.addr)
		cancel()
		if err != nil {
			continue
		}
		decimals := billing.NetworkDecimals(key.net)

		api.mu.Lock()
		for _, ord := range orders {
			curOrd, ok := api.paymentOrders[ord.OrderID]
			if !ok || curOrd.Status != "pending" {
				continue
			}
			expectedUnits, convErr := billing.DecimalToUnits(curOrd.ExactAmountUSDT, decimals)
			if convErr != nil {
				continue
			}
			for _, tr := range transfers {
				if !tr.Confirmed {
					continue
				}
				if tr.TimestampUnix > 0 && tr.TimestampUnix < curOrd.CreatedAtUnix-120 {
					continue
				}
				if _, used := api.usedTxHashes[strings.ToLower(tr.TxHash)]; used {
					continue
				}
				if tr.AmountUnits != nil && tr.AmountUnits.Cmp(expectedUnits) == 0 {
					_, _, _ = api.completePaymentOrderLocked(curOrd.OrderID, tr.TxHash)
					break
				}
			}
		}
		api.mu.Unlock()
	}
}

func (api *AdminAPI) handlePaymentStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	orderID := strings.TrimSpace(r.URL.Query().Get("order_id"))
	if orderID == "" {
		http.Error(w, "order_id required", http.StatusBadRequest)
		return
	}

	api.mu.Lock()
	ord, ok := api.paymentOrders[orderID]
	if !ok {
		api.mu.Unlock()
		http.Error(w, "Sipariş bulunamadı", http.StatusNotFound)
		return
	}

	nowUnix := time.Now().Unix()
	if ord.Status == "pending" && ord.ExpiresAtUnix > 0 && nowUnix > ord.ExpiresAtUnix {
		ord.Status = "expired"
		ord.PasswordHash = ""
		api.paymentOrders[orderID] = ord
		api.saveStateLocked()
	}

	shouldCheckChain := false
	if ord.Status == "pending" && api.billing.AutoVerifyEnabled {
		last := api.lastCheckByOrder[orderID]
		if nowUnix-last >= 5 {
			api.lastCheckByOrder[orderID] = nowUnix
			shouldCheckChain = true
		}
	}
	net := billing.NormalizeNetwork(ord.USDTNetwork)
	addr := ord.USDTAddress
	api.mu.Unlock()

	if shouldCheckChain {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		transfers, err := billing.FetchIncomingUSDTTransfers(ctx, net, addr)
		cancel()
		if err == nil {
			decimals := billing.NetworkDecimals(net)
			api.mu.Lock()
			curOrd, exists := api.paymentOrders[orderID]
			if exists && curOrd.Status == "pending" {
				if expectedUnits, convErr := billing.DecimalToUnits(curOrd.ExactAmountUSDT, decimals); convErr == nil {
					for _, tr := range transfers {
						if !tr.Confirmed || (tr.TimestampUnix > 0 && tr.TimestampUnix < curOrd.CreatedAtUnix-120) {
							continue
						}
						if _, used := api.usedTxHashes[strings.ToLower(tr.TxHash)]; used {
							continue
						}
						if tr.AmountUnits != nil && tr.AmountUnits.Cmp(expectedUnits) == 0 {
							_, _, _ = api.completePaymentOrderLocked(orderID, tr.TxHash)
							break
						}
					}
				}
			}
			api.mu.Unlock()
		}
	}

	api.mu.RLock()
	ord = api.paymentOrders[orderID]
	var acc storage.UserAccount
	if ord.Status == "confirmed" {
		acc = api.users[ord.Username]
	}
	api.mu.RUnlock()

	resp := map[string]interface{}{
		"status": ord.Status,
		"order":  toPublicOrderDTO(ord),
	}
	if ord.Status == "confirmed" && acc.Token != "" {
		setUserSessionCookie(w, r, acc.Token, 24*30*3600)
		favs := acc.Favorites
		if favs == nil {
			favs = []string{}
		}
		resp["token"] = acc.Token
		resp["user"] = PublicUserDTO{
			UserID:    acc.UserID,
			Tier:      acc.Tier,
			PaidUntil: acc.PaidUntil,
			Favorites: favs,
		}
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (api *AdminAPI) handleVerifyTxHash(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		OrderID string `json:"order_id"`
		TxHash  string `json:"tx_hash"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Geçersiz istek"})
		return
	}
	orderID := strings.TrimSpace(req.OrderID)
	txHash := strings.TrimSpace(req.TxHash)

	api.mu.RLock()
	ord, ok := api.paymentOrders[orderID]
	_, alreadyUsed := api.usedTxHashes[strings.ToLower(txHash)]
	api.mu.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Sipariş bulunamadı"})
		return
	}
	if ord.Status == "confirmed" {
		api.mu.RLock()
		acc := api.users[ord.Username]
		api.mu.RUnlock()
		setUserSessionCookie(w, r, acc.Token, 24*30*3600)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "confirmed",
			"token":  acc.Token,
			"user": PublicUserDTO{
				UserID:    acc.UserID,
				Tier:      acc.Tier,
				PaidUntil: acc.PaidUntil,
				Favorites: acc.Favorites,
			},
		})
		return
	}
	if ord.Status == "expired" || (ord.ExpiresAtUnix > 0 && time.Now().Unix() > ord.ExpiresAtUnix) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Bu siparişin süresi dolmuş, lütfen yeni ödeme başlatın"})
		return
	}
	if alreadyUsed {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Bu TX Hash daha önce kullanılmış"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	tr, verifyErr := billing.VerifySpecificTxHash(ctx, ord.USDTNetwork, ord.USDTAddress, txHash, ord.ExactAmountUSDT, ord.CreatedAtUnix)
	cancel()
	if verifyErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": verifyErr.Error()})
		return
	}

	api.mu.Lock()
	_, acc, err := api.completePaymentOrderLocked(orderID, tr.TxHash)
	api.mu.Unlock()
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	setUserSessionCookie(w, r, acc.Token, 24*30*3600)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "confirmed",
		"token":  acc.Token,
		"user": PublicUserDTO{
			UserID:    acc.UserID,
			Tier:      acc.Tier,
			PaidUntil: acc.PaidUntil,
			Favorites: acc.Favorites,
		},
	})
}

func (api *AdminAPI) handleUserFavorites(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tok := extractUserSessionToken(r)
	claims, err := api.authManager.ValidateToken(tok)
	if err != nil || claims.Tier == auth.TierGuest || claims.UserID == "" || claims.UserID == "guest_anon" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Favoriler sadece kayıtlı kullanıcılar içindir"})
		return
	}

	api.mu.RLock()
	acc, exists := api.users[claims.UserID]
	api.mu.RUnlock()
	if !exists && claims.Tier != auth.TierAdmin {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Kullanıcı bulunamadı"})
		return
	}
	favs := acc.Favorites
	if favs == nil {
		favs = []string{}
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"favorites": favs})
}

func (api *AdminAPI) handleToggleFavorite(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if !checkSameOriginOrHeader(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	tok := extractUserSessionToken(r)
	claims, err := api.authManager.ValidateToken(tok)
	if err != nil || claims.Tier == auth.TierGuest || claims.UserID == "" || claims.UserID == "guest_anon" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Favori eklemek için giriş yapmalı veya kaydolmalısınız"})
		return
	}

	var req struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.ChannelID) == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "channel_id gerekli"})
		return
	}
	chID := strings.TrimSpace(req.ChannelID)

	api.mu.Lock()
	defer api.mu.Unlock()

	acc, exists := api.users[claims.UserID]
	if !exists {
		if claims.Tier == auth.TierAdmin {
			acc = storage.UserAccount{
				UserID:    claims.UserID,
				Tier:      auth.TierAdmin,
				Favorites: []string{},
				CreatedAt: time.Now().Format("2006-01-02 15:04:05"),
			}
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Kayıtlı kullanıcı hesabı bulunamadı"})
			return
		}
	}
	if _, chExists := api.streams[chID]; !chExists {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Kanal bulunamadı"})
		return
	}

	isFav := false
	newFavs := make([]string, 0, len(acc.Favorites)+1)
	found := false
	for _, f := range acc.Favorites {
		if f == chID {
			found = true
			continue
		}
		newFavs = append(newFavs, f)
	}
	if !found {
		newFavs = append(newFavs, chID)
		isFav = true
	}
	acc.Favorites = newFavs
	api.users[claims.UserID] = acc
	api.saveStateLocked()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "success",
		"is_favorite": isFav,
		"favorites":   newFavs,
	})
}

func (api *AdminAPI) getViewerCounts() map[string]int {
	counts := make(map[string]int)
	if api.tracker != nil {
		for chID, stats := range api.tracker.GetSwarmStats() {
			if total := stats["total_peers"]; total > 0 {
				counts[chID] = total
			}
		}
	}
	if api.hooks.GetHTTPViewers != nil {
		for chID, httpCnt := range api.hooks.GetHTTPViewers() {
			if httpCnt > counts[chID] {
				counts[chID] = httpCnt
			}
		}
	}
	return counts
}

func (api *AdminAPI) handleChannelViewers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(api.getViewerCounts())
}

func (api *AdminAPI) handleChannelHeartbeat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ChannelID string `json:"channel_id"`
		ViewerID  string `json:"viewer_id"`
		Action    string `json:"action"` // join, ping, leave
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.ChannelID) == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	vk := strings.TrimSpace(req.ViewerID)
	if vk == "" {
		vk = "ip:" + r.RemoteAddr
	} else {
		vk = "v:" + vk
	}
	if strings.EqualFold(req.Action, "leave") {
		if api.hooks.RemoveViewer != nil {
			api.hooks.RemoveViewer(strings.TrimSpace(req.ChannelID), vk)
		}
	} else {
		if api.hooks.RecordViewer != nil {
			api.hooks.RecordViewer(strings.TrimSpace(req.ChannelID), vk)
		}
	}
	counts := api.getViewerCounts()
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"viewer_count": counts[strings.TrimSpace(req.ChannelID)],
	})
}

func (api *AdminAPI) handleAdminBillingSettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		api.mu.RLock()
		b := api.billing
		api.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(b)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "GET/POST required", http.StatusMethodNotAllowed)
		return
	}

	var req storage.BillingSettings
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	req.USDTAddress = strings.TrimSpace(req.USDTAddress)
	req.USDTNetwork = billing.NormalizeNetwork(req.USDTNetwork)

	if req.USDTAddress != "" {
		if err := billing.ValidateUSDTAddress(req.USDTNetwork, req.USDTAddress); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
	}
	if req.MonthlyPriceUSDT < 0 || req.QuarterlyPriceUSDT < 0 || req.YearlyPriceUSDT < 0 {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Ücretler negatif olamaz"})
		return
	}

	api.mu.Lock()
	api.billing = req
	api.saveStateLocked()
	api.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"settings": req,
	})
}

func (api *AdminAPI) handleAdminBillingOrders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	api.mu.RLock()
	list := make([]PublicOrderDTO, 0, len(api.paymentOrders))
	for _, ord := range api.paymentOrders {
		list = append(list, toPublicOrderDTO(ord))
	}
	api.mu.RUnlock()

	sort.Slice(list, func(i, j int) bool {
		return list[i].ExpiresAtUnix > list[j].ExpiresAtUnix
	})
	_ = json.NewEncoder(w).Encode(list)
}

func (api *AdminAPI) handleAdminApproveOrder(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	orderID := strings.TrimSpace(r.URL.Query().Get("order_id"))
	if orderID == "" {
		var req struct {
			OrderID string `json:"order_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		orderID = strings.TrimSpace(req.OrderID)
	}
	if orderID == "" {
		http.Error(w, "order_id required", http.StatusBadRequest)
		return
	}

	api.mu.Lock()
	ord, _, err := api.completePaymentOrderLocked(orderID, "admin_manual_"+generateRandomHex(4))
	api.mu.Unlock()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"order":  toPublicOrderDTO(ord),
	})
}
