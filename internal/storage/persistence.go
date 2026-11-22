package storage

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type M3USource struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	Language    string `json:"language"`    // Türkçe, Almanca, İngilizce, Karışık
	ContentType string `json:"content_type"` // Canlı TV, Filmler, Diziler
	AutoPrune   bool   `json:"auto_prune"`
}

type UserAccount struct {
	UserID       string   `json:"user_id"`
	Tier         string   `json:"tier"` // free or paid
	Token        string   `json:"token"`
	PasswordHash string   `json:"password_hash,omitempty"`
	Favorites    []string `json:"favorites,omitempty"`
	PaidUntil    string   `json:"paid_until,omitempty"`
	CreatedAt    string   `json:"created_at"`
}

type BillingSettings struct {
	USDTAddress        string  `json:"usdt_address"`
	USDTNetwork        string  `json:"usdt_network"` // TRC20, BEP20, POLYGON, ERC20
	MonthlyPriceUSDT   float64 `json:"monthly_price_usdt"`
	QuarterlyPriceUSDT float64 `json:"quarterly_price_usdt"`
	YearlyPriceUSDT    float64 `json:"yearly_price_usdt"`
	AutoVerifyEnabled  bool    `json:"auto_verify_enabled"`
}

type PaymentOrder struct {
	OrderID         string  `json:"order_id"`
	Username        string  `json:"username"`
	PasswordHash    string  `json:"password_hash,omitempty"`
	PlanID          string  `json:"plan_id"`
	DurationDays    int     `json:"duration_days"`
	BaseAmountUSDT  float64 `json:"base_amount_usdt"`
	ExactAmountUSDT string  `json:"exact_amount_usdt"`
	USDTAddress     string  `json:"usdt_address"`
	USDTNetwork     string  `json:"usdt_network"`
	Status          string  `json:"status"` // pending, confirmed, expired
	TxHash          string  `json:"tx_hash,omitempty"`
	UpgradeOnly     bool    `json:"upgrade_only,omitempty"`
	CreatedAtUnix   int64   `json:"created_at_unix"`
	ExpiresAtUnix   int64   `json:"expires_at_unix"`
	ConfirmedAt     string  `json:"confirmed_at,omitempty"`
}

type StorageData struct {
	Streams           map[string]interface{}  `json:"streams"`
	EPGSources        map[string]string       `json:"epg_sources"`
	EPGMappings       map[string]string       `json:"epg_mappings"`
	M3USources        []M3USource             `json:"m3u_sources"`
	DeletedChannelIDs map[string]bool         `json:"deleted_channel_ids"`
	Users             map[string]UserAccount  `json:"users"`
	Billing           *BillingSettings        `json:"billing,omitempty"`
	PaymentOrders     map[string]PaymentOrder `json:"payment_orders,omitempty"`
	UsedTxHashes      map[string]string       `json:"used_tx_hashes,omitempty"`
}

var (
	storageMutex sync.Mutex
	storagePath  = filepath.Join("data", "server_storage.json")
)

func DefaultBillingSettings() BillingSettings {
	return BillingSettings{
		USDTAddress:        "",
		USDTNetwork:        "TRC20",
		MonthlyPriceUSDT:   10,
		QuarterlyPriceUSDT: 25,
		YearlyPriceUSDT:    80,
		AutoVerifyEnabled:  true,
	}
}

func SaveState(streams interface{}, epgSources map[string]string, epgMappings map[string]string, m3uSources []M3USource, deletedIDs map[string]bool, users map[string]UserAccount) {
	SaveStateFull(streams, epgSources, epgMappings, m3uSources, deletedIDs, users, nil, nil, nil)
}

func SaveStateFull(streams interface{}, epgSources map[string]string, epgMappings map[string]string, m3uSources []M3USource, deletedIDs map[string]bool, users map[string]UserAccount, billing *BillingSettings, orders map[string]PaymentOrder, usedTx map[string]string) {
	storageMutex.Lock()
	defer storageMutex.Unlock()

	_ = os.MkdirAll("data", 0755)

	data := StorageData{
		Streams:           StreamsToMap(streams),
		EPGSources:        epgSources,
		EPGMappings:       epgMappings,
		M3USources:        m3uSources,
		DeletedChannelIDs: deletedIDs,
		Users:             users,
		Billing:           billing,
		PaymentOrders:     orders,
		UsedTxHashes:      usedTx,
	}

	fileBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Printf("[STORAGE] Failed to marshal state: %v", err)
		return
	}

	_ = os.WriteFile(storagePath, fileBytes, 0644)
}

func LoadState() (*StorageData, error) {
	storageMutex.Lock()
	defer storageMutex.Unlock()

	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		return nil, err
	}

	fileBytes, err := os.ReadFile(storagePath)
	if err != nil {
		return nil, err
	}

	var data StorageData
	if err := json.Unmarshal(fileBytes, &data); err != nil {
		return nil, err
	}

	return &data, nil
}

func StreamsToMap(streams interface{}) map[string]interface{} {
	b, err := json.Marshal(streams)
	if err != nil {
		return make(map[string]interface{})
	}
	var m map[string]interface{}
	_ = json.Unmarshal(b, &m)
	return m
}
