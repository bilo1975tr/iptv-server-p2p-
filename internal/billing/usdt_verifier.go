package billing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Supported USDT networks
const (
	NetworkTRC20   = "TRC20"
	NetworkBEP20   = "BEP20"
	NetworkPOLYGON = "POLYGON"
	NetworkERC20   = "ERC20"
)

// Official USDT token contracts & RPC endpoints (server-pinned allowlist; never user-supplied)
const (
	trc20USDTContract   = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
	bep20USDTContract   = "0x55d398326f99059fF775485246999027B3197955"
	polygonUSDTContract = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"
	erc20USDTContract   = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

	transferEventTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

type IncomingTransfer struct {
	TxHash        string
	From          string
	To            string
	AmountUnits   *big.Int // raw integer in smallest token unit
	Decimals      int
	TimestampUnix int64
	Confirmed     bool
}

var httpClient = &http.Client{Timeout: 12 * time.Second}

// NormalizeNetwork ensures a valid supported USDT network identifier
func NormalizeNetwork(net string) string {
	switch strings.ToUpper(strings.TrimSpace(net)) {
	case NetworkTRC20, "TRON", "TRX":
		return NetworkTRC20
	case NetworkBEP20, "BSC", "BNB":
		return NetworkBEP20
	case NetworkPOLYGON, "MATIC", "POL":
		return NetworkPOLYGON
	case NetworkERC20, "ETH", "ETHEREUM":
		return NetworkERC20
	default:
		return NetworkTRC20
	}
}

// ValidateUSDTAddress performs strict format validation for the chosen network
func ValidateUSDTAddress(network, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("USDT cüzdan adresi boş olamaz")
	}
	net := NormalizeNetwork(network)
	if net == NetworkTRC20 {
		if !strings.HasPrefix(addr, "T") || len(addr) != 34 {
			return fmt.Errorf("Geçersiz TRC20 (TRON) adresi: 'T' ile başlayan 34 karakter olmalıdır")
		}
		for _, r := range addr {
			if !((r >= '1' && r <= '9') || (r >= 'A' && r <= 'H') || (r >= 'J' && r <= 'N') || (r >= 'P' && r <= 'Z') || (r >= 'a' && r <= 'k') || (r >= 'm' && r <= 'z')) {
				return fmt.Errorf("TRC20 adresi geçersiz karakter içeriyor")
			}
		}
		return nil
	}
	if !strings.HasPrefix(addr, "0x") && !strings.HasPrefix(addr, "0X") {
		return fmt.Errorf("EVM (BEP20/POLYGON/ERC20) adresi '0x' ile başlamalıdır")
	}
	if len(addr) != 42 {
		return fmt.Errorf("EVM adresi 42 karakter olmalıdır")
	}
	for _, r := range addr[2:] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return fmt.Errorf("EVM adresi geçersiz hex karakter içeriyor")
		}
	}
	return nil
}

// NetworkDecimals returns the decimal places of USDT on the given network
func NetworkDecimals(network string) int {
	if NormalizeNetwork(network) == NetworkBEP20 {
		return 18
	}
	return 6
}

// DecimalToUnits converts a decimal string like "10.0042" into raw base units (*big.Int)
func DecimalToUnits(amountStr string, decimals int) (*big.Int, error) {
	s := strings.TrimSpace(amountStr)
	if s == "" {
		return nil, fmt.Errorf("empty amount")
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid decimal format")
	}
	wholePart := parts[0]
	if wholePart == "" {
		wholePart = "0"
	}
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	if len(fracPart) > decimals {
		fracPart = fracPart[:decimals]
	}
	for len(fracPart) < decimals {
		fracPart += "0"
	}
	combined := wholePart + fracPart
	val, ok := new(big.Int).SetString(combined, 10)
	if !ok {
		return nil, fmt.Errorf("invalid integer amount: %s", combined)
	}
	return val, nil
}

// FetchIncomingUSDTTransfers fetches recent confirmed USDT deposits to the given admin address
func FetchIncomingUSDTTransfers(ctx context.Context, network, toAddress string) ([]IncomingTransfer, error) {
	net := NormalizeNetwork(network)
	toAddress = strings.TrimSpace(toAddress)
	if err := ValidateUSDTAddress(net, toAddress); err != nil {
		return nil, err
	}

	switch net {
	case NetworkTRC20:
		return fetchTRC20Transfers(ctx, toAddress)
	case NetworkBEP20:
		return fetchEVMTransfers(ctx, "https://bsc-dataseed.binance.org", bep20USDTContract, toAddress, 18)
	case NetworkPOLYGON:
		return fetchEVMTransfers(ctx, "https://polygon-rpc.com", polygonUSDTContract, toAddress, 6)
	case NetworkERC20:
		return fetchEVMTransfers(ctx, "https://ethereum-rpc.publicnode.com", erc20USDTContract, toAddress, 6)
	default:
		return nil, fmt.Errorf("unsupported network: %s", net)
	}
}

// VerifySpecificTxHash verifies a specific transaction hash against the blockchain
func VerifySpecificTxHash(ctx context.Context, network, toAddress, txHash, minAmountStr string, createdAfterUnix int64) (*IncomingTransfer, error) {
	net := NormalizeNetwork(network)
	txHash = strings.TrimSpace(txHash)
	if txHash == "" {
		return nil, fmt.Errorf("TX Hash boş olamaz")
	}

	decimals := NetworkDecimals(net)
	minUnits, err := DecimalToUnits(minAmountStr, decimals)
	if err != nil {
		return nil, err
	}

	// First scan recent incoming transfers to recipient (fast & handles TRC20 directly)
	transfers, fetchErr := FetchIncomingUSDTTransfers(ctx, net, toAddress)
	if fetchErr == nil {
		for _, tr := range transfers {
			if strings.EqualFold(tr.TxHash, txHash) {
				if !tr.Confirmed {
					return nil, fmt.Errorf("işlem henüz ağda onaylanmadı, birkaç saniye sonra tekrar deneyin")
				}
				if tr.TimestampUnix > 0 && tr.TimestampUnix < createdAfterUnix-120 {
					return nil, fmt.Errorf("bu TX sipariş oluşturulmadan önce yapılmış")
				}
				if tr.AmountUnits.Cmp(minUnits) < 0 {
					return nil, fmt.Errorf("gönderilen tutar yetersiz (beklenen: %s USDT)", minAmountStr)
				}
				trCopy := tr
				return &trCopy, nil
			}
		}
	}

	// For EVM networks, also try eth_getTransactionReceipt directly in case transfer is older than 400 blocks
	if net != NetworkTRC20 {
		rpcURL := "https://bsc-dataseed.binance.org"
		contract := bep20USDTContract
		if net == NetworkPOLYGON {
			rpcURL = "https://polygon-rpc.com"
			contract = polygonUSDTContract
		} else if net == NetworkERC20 {
			rpcURL = "https://ethereum-rpc.publicnode.com"
			contract = erc20USDTContract
		}
		tr, recErr := verifyEVMTxReceipt(ctx, rpcURL, contract, toAddress, txHash, decimals)
		if recErr != nil {
			return nil, recErr
		}
		if tr.AmountUnits.Cmp(minUnits) < 0 {
			return nil, fmt.Errorf("gönderilen tutar yetersiz (beklenen: %s USDT)", minAmountStr)
		}
		return tr, nil
	}

	if fetchErr != nil {
		return nil, fmt.Errorf("ağ sorgusu başarısız: %w", fetchErr)
	}
	return nil, fmt.Errorf("bu TX Hash için alıcı adresinize eşleşen onaylı bir USDT transferi henüz görülmedi")
}

func fetchTRC20Transfers(ctx context.Context, toAddress string) ([]IncomingTransfer, error) {
	url := fmt.Sprintf("https://api.trongrid.io/v1/accounts/%s/transactions/trc20?only_to=true&only_confirmed=true&limit=50&contract_address=%s", toAddress, trc20USDTContract)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TronGrid HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Success bool `json:"success"`
		Data    []struct {
			TransactionID  string `json:"transaction_id"`
			BlockTimestamp int64  `json:"block_timestamp"`
			From           string `json:"from"`
			To             string `json:"to"`
			Type           string `json:"type"`
			Value          string `json:"value"`
			TokenInfo      struct {
				Address  string `json:"address"`
				Decimals int    `json:"decimals"`
			} `json:"token_info"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	out := make([]IncomingTransfer, 0, len(parsed.Data))
	for _, item := range parsed.Data {
		if !strings.EqualFold(item.TokenInfo.Address, trc20USDTContract) {
			continue
		}
		if item.To != toAddress {
			continue
		}
		if item.Type != "" && !strings.EqualFold(item.Type, "Transfer") {
			continue
		}
		amt, ok := new(big.Int).SetString(strings.TrimSpace(item.Value), 10)
		if !ok {
			continue
		}
		dec := item.TokenInfo.Decimals
		if dec <= 0 {
			dec = 6
		}
		out = append(out, IncomingTransfer{
			TxHash:        item.TransactionID,
			From:          item.From,
			To:            item.To,
			AmountUnits:   amt,
			Decimals:      dec,
			TimestampUnix: item.BlockTimestamp / 1000,
			Confirmed:     true,
		})
	}
	return out, nil
}

func evmCall(ctx context.Context, rpcURL, method string, params interface{}, out interface{}) error {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return err
	}
	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return err
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	if out != nil {
		return json.Unmarshal(rpcResp.Result, out)
	}
	return nil
}

func padEVMAddressTopic(addr string) string {
	clean := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(addr), "0x"))
	for len(clean) < 64 {
		clean = "0" + clean
	}
	return "0x" + clean
}

func fetchEVMTransfers(ctx context.Context, rpcURL, contract, toAddress string, decimals int) ([]IncomingTransfer, error) {
	var blockHex string
	if err := evmCall(ctx, rpcURL, "eth_blockNumber", []interface{}{}, &blockHex); err != nil {
		return nil, err
	}
	latestBlock, err := strconv.ParseUint(strings.TrimPrefix(blockHex, "0x"), 16, 64)
	if err != nil {
		return nil, err
	}
	// Query last ~350 blocks (~15-70 mins depending on chain) and require at least 2 confirmations
	confirmedTip := latestBlock
	if confirmedTip > 2 {
		confirmedTip -= 2
	}
	var fromBlock uint64
	if confirmedTip > 350 {
		fromBlock = confirmedTip - 350
	}

	filter := map[string]interface{}{
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   fmt.Sprintf("0x%x", confirmedTip),
		"address":   contract,
		"topics": []interface{}{
			transferEventTopic0,
			nil,
			padEVMAddressTopic(toAddress),
		},
	}

	var logs []struct {
		TransactionHash string   `json:"transactionHash"`
		Topics          []string `json:"topics"`
		Data            string   `json:"data"`
		Removed         bool     `json:"removed"`
	}
	if err := evmCall(ctx, rpcURL, "eth_getLogs", []interface{}{filter}, &logs); err != nil {
		return nil, err
	}

	out := make([]IncomingTransfer, 0, len(logs))
	for _, lg := range logs {
		if lg.Removed || len(lg.Topics) < 3 {
			continue
		}
		valHex := strings.TrimPrefix(strings.TrimSpace(lg.Data), "0x")
		if valHex == "" {
			continue
		}
		amt, ok := new(big.Int).SetString(valHex, 16)
		if !ok {
			continue
		}
		fromAddr := "0x" + lg.Topics[1][len(lg.Topics[1])-40:]
		out = append(out, IncomingTransfer{
			TxHash:        lg.TransactionHash,
			From:          fromAddr,
			To:            toAddress,
			AmountUnits:   amt,
			Decimals:      decimals,
			TimestampUnix: time.Now().Unix(),
			Confirmed:     true,
		})
	}
	return out, nil
}

func verifyEVMTxReceipt(ctx context.Context, rpcURL, contract, toAddress, txHash string, decimals int) (*IncomingTransfer, error) {
	var receipt struct {
		Status string `json:"status"`
		Logs   []struct {
			Address string   `json:"address"`
			Topics  []string `json:"topics"`
			Data    string   `json:"data"`
		} `json:"logs"`
	}
	if err := evmCall(ctx, rpcURL, "eth_getTransactionReceipt", []interface{}{txHash}, &receipt); err != nil {
		return nil, err
	}
	if receipt.Status != "0x1" {
		return nil, fmt.Errorf("işlem başarısız veya henüz onaylanmadı")
	}
	expectedTopicTo := padEVMAddressTopic(toAddress)
	for _, lg := range receipt.Logs {
		if !strings.EqualFold(lg.Address, contract) || len(lg.Topics) < 3 {
			continue
		}
		if !strings.EqualFold(lg.Topics[0], transferEventTopic0) || !strings.EqualFold(lg.Topics[2], expectedTopicTo) {
			continue
		}
		valHex := strings.TrimPrefix(strings.TrimSpace(lg.Data), "0x")
		amt, ok := new(big.Int).SetString(valHex, 16)
		if !ok {
			continue
		}
		return &IncomingTransfer{
			TxHash:        txHash,
			From:          "0x" + lg.Topics[1][len(lg.Topics[1])-40:],
			To:            toAddress,
			AmountUnits:   amt,
			Decimals:      decimals,
			TimestampUnix: time.Now().Unix(),
			Confirmed:     true,
		}, nil
	}
	return nil, fmt.Errorf("bu TX içinde alıcı adresine geçerli bir USDT transferi bulunamadı")
}
