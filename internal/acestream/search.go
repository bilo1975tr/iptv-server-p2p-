package acestream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type SearchResultItem struct {
	Name         string   `json:"name"`
	SourceURL    string   `json:"source_url"`
	ContentID    string   `json:"content_id,omitempty"`
	Infohash     string   `json:"infohash,omitempty"`
	ChannelID    int64    `json:"channel_id,omitempty"`
	Availability float64  `json:"availability"`
	Status       int      `json:"status"`
	Bitrate      int      `json:"bitrate,omitempty"`
	Categories   []string `json:"categories,omitempty"`
}

type SearchResponse struct {
	Query   string             `json:"query"`
	Total   int                `json:"total"`
	Results []SearchResultItem `json:"results"`
}

// Search queries the local ace_engine.exe HTTP /search endpoint (port 6878).
// If the user pastes an acestream:// URL or 40-char hex ID directly, it also returns it as a direct result.
func (m *Manager) Search(ctx context.Context, query, category string, page, pageSize int) (*SearchResponse, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("search query cannot be empty")
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 30
	}
	if page < 0 {
		page = 0
	}

	if IsAceStreamURL(query) {
		paramKey, paramVal, err := ParseSource(query)
		if err == nil {
			srcURL := "acestream://" + paramVal
			if paramKey == "infohash" {
				srcURL = "acestream://infohash/" + paramVal
			}
			return &SearchResponse{
				Query: query,
				Total: 1,
				Results: []SearchResultItem{
					{
						Name:         "AceStream Kanal (" + shortID(paramVal) + ")",
						SourceURL:    srcURL,
						ContentID:    paramVal,
						Availability: 1.0,
						Status:       2,
						Categories:   []string{"direct"},
					},
				},
			}, nil
		}
	}

	if err := m.EnsurePrimaryRunning(); err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("query", query)
	q.Set("page", strconv.Itoa(page))
	q.Set("page_size", strconv.Itoa(pageSize))
	if category != "" && category != "all" {
		q.Set("category", category)
	}

	searchURL := fmt.Sprintf("http://%s:%d/search?%s", m.cfg.APIHost, m.cfg.BaseHTTPPort, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, searchURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 12 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("acestream search request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("acestream engine search returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var raw struct {
		Result struct {
			Total   int `json:"total"`
			Results []struct {
				Name  string `json:"name"`
				Items []struct {
					Name         string   `json:"name"`
					ContentID    string   `json:"content_id"`
					Infohash     string   `json:"infohash"`
					ChannelID    int64    `json:"channel_id"`
					Availability float64  `json:"availability"`
					Status       int      `json:"status"`
					Bitrate      int      `json:"bitrate"`
					Categories   []string `json:"categories"`
				} `json:"items"`
			} `json:"results"`
		} `json:"result"`
		Error *string `json:"error"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse acestream search response: %w", err)
	}
	if raw.Error != nil && *raw.Error != "" {
		return nil, fmt.Errorf("acestream search error: %s", *raw.Error)
	}

	var out []SearchResultItem
	for _, group := range raw.Result.Results {
		for _, item := range group.Items {
			name := strings.TrimSpace(item.Name)
			if name == "" {
				name = strings.TrimSpace(group.Name)
			}
			if name == "" {
				name = "AceStream Channel"
			}

			var sourceURL string
			if item.ContentID != "" {
				sourceURL = "acestream://" + item.ContentID
			} else if item.Infohash != "" {
				sourceURL = "acestream://infohash/" + item.Infohash
			} else {
				continue
			}

			out = append(out, SearchResultItem{
				Name:         name,
				SourceURL:    sourceURL,
				ContentID:    item.ContentID,
				Infohash:     item.Infohash,
				ChannelID:    item.ChannelID,
				Availability: item.Availability,
				Status:       item.Status,
				Bitrate:      item.Bitrate,
				Categories:   item.Categories,
			})
		}
	}

	return &SearchResponse{
		Query:   query,
		Total:   raw.Result.Total,
		Results: out,
	}, nil
}

func shortID(id string) string {
	if len(id) <= 10 {
		return id
	}
	return id[:8] + "..."
}
