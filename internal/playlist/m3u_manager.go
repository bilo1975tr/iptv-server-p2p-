package playlist

import (
	"bufio"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

type ChannelItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Group     string `json:"group"`
	Logo      string `json:"logo"`
	TvgID     string `json:"tvg_id,omitempty"`
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

func extractExtInfAttr(line, attrName string) string {
	needle := attrName + "=\""
	idx := strings.Index(line, needle)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(needle):]
	endIdx := strings.IndexByte(rest, '"')
	if endIdx < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:endIdx])
}

func extractExtInfName(line string, idx int) string {
	inQuotes := false
	lastComma := -1
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				lastComma = i
			}
		}
	}
	if lastComma >= 0 && lastComma+1 < len(line) {
		return sanitizeChannelName(line[lastComma+1:])
	}
	parts := strings.SplitN(line, ",", 2)
	if len(parts) > 1 {
		return sanitizeChannelName(parts[1])
	}
	return fmt.Sprintf("Channel %d", idx)
}

func stableChannelID(name, sourceURL string) string {
	seed := strings.TrimSpace(sourceURL)
	if seed == "" {
		seed = strings.TrimSpace(name)
	}
	sum := sha256.Sum256([]byte(seed))
	return "ch_" + hex.EncodeToString(sum[:5])
}

// IsHLSStreamPlaylist detects whether fetched M3U8 text is a single HLS master/media playlist
// (e.g. #EXT-X-STREAM-INF or #EXT-X-TARGETDURATION) rather than an IPTV channel directory.
func IsHLSStreamPlaylist(content string) bool {
	upper := strings.ToUpper(content)
	return strings.Contains(upper, "#EXT-X-TARGETDURATION") ||
		strings.Contains(upper, "#EXT-X-STREAM-INF") ||
		strings.Contains(upper, "#EXT-X-MEDIA-SEQUENCE") ||
		strings.Contains(upper, "#EXT-X-PLAYLIST-TYPE")
}

// IsDirectMediaURL checks whether a URL points directly to a video/audio media file rather than a playlist.
func IsDirectMediaURL(rawURL string) bool {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	ext := strings.ToLower(path.Ext(u.Path))
	switch ext {
	case ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".mp3", ".aac", ".ogg", ".m4a":
		return true
	}
	return false
}

// DeriveChannelNameFromURL extracts a readable channel/title name from a stream URL filename.
func DeriveChannelNameFromURL(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "Canlı Kanal"
	}
	base := path.Base(u.Path)
	if decoded, err := url.PathUnescape(base); err == nil && decoded != "" {
		base = decoded
	}
	if ext := path.Ext(base); ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	base = strings.ReplaceAll(base, "_", " ")
	base = strings.TrimSpace(base)
	if base == "" || base == "." || base == "/" {
		if u.Host != "" {
			return u.Host
		}
		return "Canlı Kanal"
	}
	return base
}

// BuildSingleChannelItem creates a ChannelItem from a standalone stream URL (e.g., direct HLS .m3u8 or .mp4).
func BuildSingleChannelItem(sourceURL, group string) ChannelItem {
	name := DeriveChannelNameFromURL(sourceURL)
	if strings.TrimSpace(group) == "" {
		group = "General"
	}
	return ChannelItem{
		ID:        stableChannelID(name, sourceURL),
		Name:      name,
		Group:     group,
		SourceURL: strings.TrimSpace(sourceURL),
		IsActive:  true,
	}
}

func isPlayableStreamLine(line string) bool {
	low := strings.ToLower(line)
	return strings.HasPrefix(low, "http://") ||
		strings.HasPrefix(low, "https://") ||
		strings.HasPrefix(low, "rtsp://") ||
		strings.HasPrefix(low, "rtmp://") ||
		strings.HasPrefix(low, "udp://") ||
		strings.HasPrefix(low, "mms://") ||
		strings.HasPrefix(low, "acestream://")
}

// ParseM3U parses M3U / M3U8 playlist strings into structured ChannelItem slices
func ParseM3U(m3uContent string) []ChannelItem {
	var channels []ChannelItem
	m3uContent = strings.TrimPrefix(m3uContent, "\uFEFF")
	scanner := bufio.NewScanner(strings.NewReader(m3uContent))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	var currentItem ChannelItem
	idx := 1

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#EXTINF:") {
			name := extractExtInfName(line, idx)
			group := extractExtInfAttr(line, "group-title")
			if group == "" {
				group = "General"
			}
			logo := extractExtInfAttr(line, "tvg-logo")
			tvgID := extractExtInfAttr(line, "tvg-id")

			currentItem = ChannelItem{
				Name:  name,
				Group: group,
				Logo:  logo,
				TvgID: tvgID,
			}
			idx++
		} else if !strings.HasPrefix(line, "#") && isPlayableStreamLine(line) {
			currentItem.SourceURL = line
			if currentItem.Name == "" {
				derived := DeriveChannelNameFromURL(line)
				if derived != "" && derived != "Canlı Kanal" {
					currentItem.Name = derived
				} else {
					currentItem.Name = fmt.Sprintf("Channel %d", idx)
				}
				idx++
			}
			currentItem.ID = stableChannelID(currentItem.Name, currentItem.SourceURL)
			channels = append(channels, currentItem)
			currentItem = ChannelItem{}
		}
	}

	return channels
}

func newProbeClient(timeout time.Duration, insecureTLS bool) *http.Client {
	tr := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     15 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: insecureTLS},
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}
}

// ProbeStreamWithClient checks if a live video stream URL is active, reachable, and not returning an HTML error page
func ProbeStreamWithClient(client *http.Client, streamURL string) bool {
	lower := strings.ToLower(strings.TrimSpace(streamURL))
	if strings.HasPrefix(lower, "rtsp://") || strings.HasPrefix(lower, "rtmp://") || strings.HasPrefix(lower, "udp://") || strings.HasPrefix(lower, "mms://") || strings.HasPrefix(lower, "acestream://") {
		return true
	}

	req, err := http.NewRequest(http.MethodGet, streamURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")

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

// ProbeStream checks if a live video stream URL is active, reachable, and not returning an HTML error page
func ProbeStream(streamURL string, timeout time.Duration) bool {
	return ProbeStreamWithClient(newProbeClient(timeout, false), streamURL)
}

// ImportAndPruneM3U parses M3U playlist and auto-prunes dead channels concurrently
func ImportAndPruneM3U(m3uContent string, autoPrune bool) (liveChannels []ChannelItem, prunedCount int) {
	parsed := ParseM3U(m3uContent)
	if !autoPrune || len(parsed) == 0 {
		return parsed, 0
	}

	client := newProbeClient(2500*time.Millisecond, true)
	var wg sync.WaitGroup
	var mu sync.Mutex

	semaphore := make(chan struct{}, 32)

	for _, ch := range parsed {
		wg.Add(1)
		go func(item ChannelItem) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			isActive := ProbeStreamWithClient(client, item.SourceURL)
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

