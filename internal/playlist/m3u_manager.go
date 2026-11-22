package playlist

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ChannelItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Group     string `json:"group"`
	Logo      string `json:"logo"`
	SourceURL string `json:"source_url"`
	IsActive  bool   `json:"is_active"`
}

func sanitizeChannelName(rawName string) string {
	rawName = strings.TrimSpace(rawName)
	if strings.Contains(rawName, "\"") {
		parts := strings.Split(rawName, "\"")
		if len(parts) > 0 {
			candidate := strings.TrimSpace(parts[len(parts)-1])
			if candidate != "" && !strings.Contains(candidate, "=") {
				rawName = candidate
			}
		}
	}
	rawName = strings.Trim(rawName, "\"',")
	if rawName == "" {
		return "Canlı Kanal"
	}
	return rawName
}

func stableChannelID(name, sourceURL string) string {
	seed := strings.TrimSpace(sourceURL)
	if seed == "" {
		seed = strings.TrimSpace(name)
	}
	sum := sha256.Sum256([]byte(seed))
	return "ch_" + hex.EncodeToString(sum[:5])
}

// ParseM3U parses M3U / M3U8 playlist strings into structured ChannelItem slices
func ParseM3U(m3uContent string) []ChannelItem {
	var channels []ChannelItem
	scanner := bufio.NewScanner(strings.NewReader(m3uContent))
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)

	var currentItem ChannelItem
	idx := 1

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#EXTINF:") {
			parts := strings.SplitN(line, ",", 2)
			name := fmt.Sprintf("Channel %d", idx)
			if len(parts) > 1 {
				name = sanitizeChannelName(parts[1])
			}

			group := "General"
			if strings.Contains(line, "group-title=\"") {
				gParts := strings.Split(line, "group-title=\"")
				if len(gParts) > 1 {
					group = strings.Split(gParts[1], "\"")[0]
				}
			}

			logo := ""
			if strings.Contains(line, "tvg-logo=\"") {
				lParts := strings.Split(line, "tvg-logo=\"")
				if len(lParts) > 1 {
					logo = strings.Split(lParts[1], "\"")[0]
				}
			}

			currentItem = ChannelItem{
				Name:  name,
				Group: group,
				Logo:  logo,
			}
			idx++
		} else if strings.HasPrefix(strings.ToLower(line), "http://") || strings.HasPrefix(strings.ToLower(line), "https://") || strings.HasPrefix(strings.ToLower(line), "rtsp://") || strings.HasPrefix(strings.ToLower(line), "acestream://") {
			currentItem.SourceURL = line
			if currentItem.Name == "" {
				currentItem.Name = fmt.Sprintf("Channel %d", idx)
				idx++
			}
			currentItem.ID = stableChannelID(currentItem.Name, currentItem.SourceURL)
			channels = append(channels, currentItem)
			currentItem = ChannelItem{}
		}
	}

	return channels
}

// ProbeStream checks if a live video stream URL is active, reachable, and not returning an HTML error page
func ProbeStream(streamURL string, timeout time.Duration) bool {
	lower := strings.ToLower(strings.TrimSpace(streamURL))
	if strings.HasPrefix(lower, "rtsp://") || strings.HasPrefix(lower, "acestream://") {
		return true
	}

	client := &http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequest("GET", streamURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "IPTV-P2P-Server-HealthCheck/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return false
	}

	buf := make([]byte, 1024)
	n, _ := io.ReadAtLeast(resp.Body, buf, 1)
	if n == 0 {
		return false
	}

	snippet := strings.ToLower(strings.TrimSpace(string(buf[:n])))
	if strings.HasPrefix(snippet, "<!doctype html") || strings.HasPrefix(snippet, "<html") {
		return false
	}

	return true
}

// ImportAndPruneM3U parses M3U playlist and auto-prunes dead channels concurrently
func ImportAndPruneM3U(m3uContent string, autoPrune bool) (liveChannels []ChannelItem, prunedCount int) {
	parsed := ParseM3U(m3uContent)
	if !autoPrune {
		return parsed, 0
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	semaphore := make(chan struct{}, 10)

	for _, ch := range parsed {
		wg.Add(1)
		go func(item ChannelItem) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			isActive := ProbeStream(item.SourceURL, 3*time.Second)
			if isActive {
				item.IsActive = true
				mu.Lock()
				liveChannels = append(liveChannels, item)
				mu.Unlock()
			} else {
				mu.Lock()
				prunedCount++
				mu.Unlock()
			}
		}(ch)
	}

	wg.Wait()
	return liveChannels, prunedCount
}
