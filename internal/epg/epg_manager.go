package epg

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxEPGCompressedBytes   int64 = 512 * 1024 * 1024       // 512 MB compressed download cap
	maxEPGUncompressedBytes int64 = 2 * 1024 * 1024 * 1024  // 2 GB uncompressed streaming XML cap
	maxEPGKeptProgrammes          = 3000000                 // Safety cap for kept programmes in RAM
)

var errEPGSizeLimitExceeded = errors.New("EPG dosyası 2 GB sıkıştırılmamış boyut sınırını aştı")

// XMLTV Structures (used for streaming decode & /api/epg/client.xml export)
type XMLTV struct {
	XMLName    xml.Name         `xml:"tv" json:"-"`
	Channels   []XMLTVChannel   `xml:"channel" json:"channels"`
	Programmes []XMLTVProgramme `xml:"programme" json:"-"`
}

type XMLTVChannel struct {
	ID           string             `xml:"id,attr" json:"id"`
	DisplayNames []XMLTVDisplayName `xml:"display-name" json:"display_names"`
	SourceName   string             `json:"source_name,omitempty"`
}

type XMLTVDisplayName struct {
	Value string `xml:",chardata" json:"value"`
}

type XMLTVProgramme struct {
	Start   string       `xml:"start,attr"`
	Stop    string       `xml:"stop,attr"`
	Channel string       `xml:"channel,attr"`
	Titles  []XMLTVTitle `xml:"title"`
	Descs   []XMLTVDesc  `xml:"desc"`
}

type XMLTVTitle struct {
	Value string `xml:",chardata"`
}

type XMLTVDesc struct {
	Value string `xml:",chardata"`
}

// CompactProgramme avoids nested slice heap allocations when storing large multi-country EPGs in RAM.
type CompactProgramme struct {
	Start   string
	Stop    string
	Channel string
	Title   string
	Desc    string
}

type parsedEPGData struct {
	Channels   []XMLTVChannel
	Programmes []CompactProgramme
}

type ProgramInfo struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Start       string `json:"start"`
	Stop        string `json:"stop"`
}

type ChannelGuide struct {
	Current  *ProgramInfo  `json:"current"`
	Next     *ProgramInfo  `json:"next"`
	Schedule []ProgramInfo `json:"schedule"` // Full schedule for list
}

type EPGSource struct {
	URL        string `json:"url"`
	TimeOffset int    `json:"time_offset"` // 999 for Auto, or -12 to +14
}

type EPGSourceInfo struct {
	ID           string `json:"id"`
	URL          string `json:"url"`
	TimeOffset   int    `json:"time_offset"`
	ChannelCount int    `json:"channel_count"`
	MappedCount  int    `json:"mapped_count"`
}

type EPGManager struct {
	mu         sync.RWMutex
	sources    map[string]EPGSource
	channelMap map[string]string // internal_channel_id -> upstream_epg_channel_id
	parsedTVs  map[string]*parsedEPGData
}

var GlobalEPG = NewEPGManager()

func NewEPGManager() *EPGManager {
	return &EPGManager{
		sources:    make(map[string]EPGSource),
		channelMap: make(map[string]string),
		parsedTVs:  make(map[string]*parsedEPGData),
	}
}

func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}

func validateEPGURL(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("geçersiz EPG URL formatı")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("yalnızca http:// veya https:// EPG adresleri desteklenir")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return fmt.Errorf("EPG URL içinde sunucu adı eksik")
	}
	if host == "localhost" || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return fmt.Errorf("yerel ağ/loopback adreslerinden EPG çekilemez")
	}
	if ip := net.ParseIP(host); ip != nil && isBlockedIP(ip) {
		return fmt.Errorf("özel veya yerel IP adreslerinden EPG çekilemez")
	}
	return nil
}

func newSafeEPGHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil || len(ips) == 0 {
				return nil, fmt.Errorf("EPG sunucusu çözümlenemedi: %s", host)
			}
			var safeAddr string
			for _, ipAddr := range ips {
				if !isBlockedIP(ipAddr.IP) {
					safeAddr = net.JoinHostPort(ipAddr.IP.String(), port)
					break
				}
			}
			if safeAddr == "" {
				return nil, fmt.Errorf("engellenen yerel/özel ağ IP adresi: %s", host)
			}
			return dialer.DialContext(ctx, network, safeAddr)
		},
	}

	return &http.Client{
		Timeout:   300 * time.Second,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("çok fazla yönlendirme (redirect)")
			}
			return validateEPGURL(req.URL.String())
		},
	}
}

type cappedReader struct {
	r         io.Reader
	remaining int64
	exceeded  bool
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		c.exceeded = true
		return 0, errEPGSizeLimitExceeded
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.r.Read(p)
	c.remaining -= int64(n)
	return n, err
}

func (m *EPGManager) AddSource(sourceID, rawURL string, timeOffset int) (int, int, error) {
	cleanURL := strings.TrimSpace(rawURL)
	if err := validateEPGURL(cleanURL); err != nil {
		return 0, 0, err
	}
	return m.fetchAndParse(sourceID, cleanURL, timeOffset)
}

func (m *EPGManager) RemoveSource(sourceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sources, sourceID)
	delete(m.parsedTVs, sourceID)
}

func (m *EPGManager) MapChannel(internalID, epgChannelID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channelMap[internalID] = epgChannelID
}

func (m *EPGManager) GetSources() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]string, len(m.sources))
	for id, src := range m.sources {
		res[id] = src.URL
	}
	return res
}

func (m *EPGManager) GetSourceDetails() map[string]EPGSource {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]EPGSource, len(m.sources))
	for k, v := range m.sources {
		res[k] = v
	}
	return res
}

func (m *EPGManager) GetMappings() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]string, len(m.channelMap))
	for k, v := range m.channelMap {
		res[k] = v
	}
	return res
}

func (m *EPGManager) GetSourceStats() []EPGSourceInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]EPGSourceInfo, 0, len(m.sources))
	for id, src := range m.sources {
		chCount := 0
		chIDSet := make(map[string]bool)
		if tv, ok := m.parsedTVs[id]; ok {
			chCount = len(tv.Channels)
			for _, ch := range tv.Channels {
				chIDSet[ch.ID] = true
			}
		}

		mappedCount := 0
		for _, epgID := range m.channelMap {
			if chIDSet[epgID] {
				mappedCount++
			}
		}

		stats = append(stats, EPGSourceInfo{
			ID:           id,
			URL:          src.URL,
			TimeOffset:   src.TimeOffset,
			ChannelCount: chCount,
			MappedCount:  mappedCount,
		})
	}
	return stats
}

func (m *EPGManager) fetchAndParse(sourceID, rawURL string, timeOffset int) (int, int, error) {
	log.Printf("[EPG] Streaming XMLTV from %s (Offset: %d)...", rawURL, timeOffset)
	client := newSafeEPGHTTPClient()

	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/xml,text/xml,application/gzip,*/*")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[EPG] Failed to fetch EPG source %s: %v", sourceID, err)
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, 0, fmt.Errorf("EPG sunucusu HTTP %d döndürdü", resp.StatusCode)
	}

	compressedCap := &cappedReader{r: resp.Body, remaining: maxEPGCompressedBytes}
	bufReader := bufio.NewReaderSize(compressedCap, 64*1024)

	var xmlInput io.Reader = bufReader
	magic, peekErr := bufReader.Peek(2)
	if peekErr == nil && len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gzReader, gzErr := gzip.NewReader(bufReader)
		if gzErr != nil {
			return 0, 0, fmt.Errorf("GZIP açma hatası: %w", gzErr)
		}
		defer gzReader.Close()
		xmlInput = gzReader
	} else if strings.HasSuffix(strings.ToLower(strings.Split(rawURL, "?")[0]), ".gz") {
		gzReader, gzErr := gzip.NewReader(bufReader)
		if gzErr == nil {
			defer gzReader.Close()
			xmlInput = gzReader
		}
	}

	uncompressedCap := &cappedReader{r: xmlInput, remaining: maxEPGUncompressedBytes}
	decoder := xml.NewDecoder(uncompressedCap)
	decoder.Strict = false

	extraOffset := timeOffset
	if extraOffset == 999 {
		extraOffset = 0
	}

	now := time.Now()
	windowStart := now.Add(-6 * time.Hour)
	windowEnd := now.Add(72 * time.Hour)

	var channels []XMLTVChannel
	var programmes []CompactProgramme
	channelIntern := make(map[string]string, 4096)
	totalProgrammesSeen := 0

	internID := func(id string) string {
		if existing, ok := channelIntern[id]; ok {
			return existing
		}
		channelIntern[id] = id
		return id
	}

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			if uncompressedCap.exceeded || errors.Is(err, errEPGSizeLimitExceeded) {
				return 0, 0, errEPGSizeLimitExceeded
			}
			if compressedCap.exceeded {
				return 0, 0, fmt.Errorf("sıkıştırılmış EPG dosyası 512 MB sınırını aştı")
			}
			log.Printf("[EPG] Failed to stream-parse XMLTV for %s: %v", sourceID, err)
			return 0, 0, err
		}

		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		switch se.Name.Local {
		case "channel":
			var ch XMLTVChannel
			if decErr := decoder.DecodeElement(&ch, &se); decErr != nil {
				return 0, 0, decErr
			}
			if ch.ID != "" {
				ch.ID = internID(strings.TrimSpace(ch.ID))
			}
			ch.SourceName = sourceID
			channels = append(channels, ch)

		case "programme":
			totalProgrammesSeen++
			var raw XMLTVProgramme
			if decErr := decoder.DecodeElement(&raw, &se); decErr != nil {
				return 0, 0, decErr
			}
			if len(programmes) >= maxEPGKeptProgrammes {
				continue
			}

			startTime, err1 := parseXMLTVTimeWithOffset(raw.Start, extraOffset)
			stopTime, err2 := parseXMLTVTimeWithOffset(raw.Stop, extraOffset)
			if err1 == nil && err2 == nil {
				if stopTime.Before(windowStart) || startTime.After(windowEnd) {
					continue
				}
			}

			title := ""
			if len(raw.Titles) > 0 {
				title = raw.Titles[0].Value
			}
			desc := ""
			if len(raw.Descs) > 0 {
				desc = raw.Descs[0].Value
			}

			programmes = append(programmes, CompactProgramme{
				Start:   raw.Start,
				Stop:    raw.Stop,
				Channel: internID(strings.TrimSpace(raw.Channel)),
				Title:   title,
				Desc:    desc,
			})
		}
	}

	if len(channels) == 0 && len(programmes) == 0 {
		return 0, 0, fmt.Errorf("XMLTV dosyasında geçerli kanal veya program verisi bulunamadı")
	}

	m.mu.Lock()
	m.sources[sourceID] = EPGSource{URL: rawURL, TimeOffset: extraOffset}
	m.parsedTVs[sourceID] = &parsedEPGData{
		Channels:   channels,
		Programmes: programmes,
	}
	m.mu.Unlock()

	log.Printf("[EPG] Successfully stream-loaded EPG source %s: %d channels, %d active-window programmes (scanned %d total)", sourceID, len(channels), len(programmes), totalProgrammesSeen)
	return len(channels), len(programmes), nil
}

func parseXMLTVTime(rawTime string) (time.Time, error) {
	rawTime = strings.TrimSpace(rawTime)
	if len(rawTime) < 14 {
		return time.Time{}, fmt.Errorf("invalid time format")
	}

	for _, layout := range []string{
		"20060102150405 -0700",
		"20060102150405-0700",
		"20060102150405 -07:00",
		"20060102150405-07:00",
	} {
		if t, err := time.Parse(layout, rawTime); err == nil {
			return t, nil
		}
	}

	timeStr := rawTime[:14]
	t, err := time.ParseInLocation("20060102150405", timeStr, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse error")
	}
	return t, nil
}

func parseXMLTVTimeWithOffset(rawTime string, extraHoursOffset int) (time.Time, error) {
	t, err := parseXMLTVTime(rawTime)
	if err != nil {
		return time.Time{}, err
	}
	if extraHoursOffset == 999 {
		extraHoursOffset = 0
	}
	return t.In(time.Local).Add(time.Duration(extraHoursOffset) * time.Hour), nil
}

func ShiftTimestamp(rawTime string, hours int) string {
	t, err := parseXMLTVTimeWithOffset(rawTime, hours)
	if err != nil {
		return rawTime
	}
	return t.Format("20060102150405 -0700")
}

func (m *EPGManager) GetClientEPGJson() map[string]ChannelGuide {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]ChannelGuide)
	now := time.Now()

	upstreamToInternal := make(map[string][]string)
	for internalID, upstreamID := range m.channelMap {
		upstreamToInternal[upstreamID] = append(upstreamToInternal[upstreamID], internalID)
	}

	for sourceID, tv := range m.parsedTVs {
		offset := m.sources[sourceID].TimeOffset
		if offset == 999 {
			offset = 0
		}

		chProgs := make(map[string][]CompactProgramme)
		for _, prog := range tv.Programmes {
			if internalIDs, mapped := upstreamToInternal[prog.Channel]; mapped {
				for _, internalID := range internalIDs {
					chProgs[internalID] = append(chProgs[internalID], prog)
				}
			}
		}

		for internalID, progs := range chProgs {
			type timedProg struct {
				start time.Time
				stop  time.Time
				info  ProgramInfo
			}
			var parsedList []timedProg
			for _, p := range progs {
				startTime, err1 := parseXMLTVTimeWithOffset(p.Start, offset)
				stopTime, err2 := parseXMLTVTimeWithOffset(p.Stop, offset)
				if err1 != nil || err2 != nil {
					continue
				}
				parsedList = append(parsedList, timedProg{
					start: startTime,
					stop:  stopTime,
					info: ProgramInfo{
						Title:       p.Title,
						Description: p.Desc,
						Start:       startTime.Format("15:04"),
						Stop:        stopTime.Format("15:04"),
					},
				})
			}

			sort.Slice(parsedList, func(i, j int) bool {
				return parsedList[i].start.Before(parsedList[j].start)
			})

			var curr, nxt *ProgramInfo
			var schedule []ProgramInfo
			for i := range parsedList {
				tp := parsedList[i]
				if tp.stop.After(now.Add(-2 * time.Hour)) {
					schedule = append(schedule, tp.info)
				}
				if (now.Equal(tp.start) || now.After(tp.start)) && now.Before(tp.stop) {
					infoCopy := tp.info
					curr = &infoCopy
				} else if tp.start.After(now) && nxt == nil {
					infoCopy := tp.info
					nxt = &infoCopy
				}
			}

			if curr != nil || nxt != nil || len(schedule) > 0 {
				result[internalID] = ChannelGuide{Current: curr, Next: nxt, Schedule: schedule}
			}
		}
	}

	return result
}

func (m *EPGManager) GenerateCleanXML() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cleanTV := XMLTV{
		Channels:   []XMLTVChannel{},
		Programmes: []XMLTVProgramme{},
	}

	upstreamToInternal := make(map[string][]string)
	for internalID, upstreamID := range m.channelMap {
		upstreamToInternal[upstreamID] = append(upstreamToInternal[upstreamID], internalID)
	}

	for sourceID, tv := range m.parsedTVs {
		offset := m.sources[sourceID].TimeOffset
		if offset == 999 {
			offset = 0
		}

		for _, ch := range tv.Channels {
			if internalIDs, mapped := upstreamToInternal[ch.ID]; mapped {
				for _, internalID := range internalIDs {
					cleanCh := XMLTVChannel{
						ID:           internalID,
						DisplayNames: ch.DisplayNames,
					}
					cleanTV.Channels = append(cleanTV.Channels, cleanCh)
				}
			}
		}

		for _, prog := range tv.Programmes {
			if internalIDs, mapped := upstreamToInternal[prog.Channel]; mapped {
				for _, internalID := range internalIDs {
					cleanProg := XMLTVProgramme{
						Channel: internalID,
						Start:   ShiftTimestamp(prog.Start, offset),
						Stop:    ShiftTimestamp(prog.Stop, offset),
					}
					if prog.Title != "" {
						cleanProg.Titles = []XMLTVTitle{{Value: prog.Title}}
					}
					if prog.Desc != "" {
						cleanProg.Descs = []XMLTVDesc{{Value: prog.Desc}}
					}
					cleanTV.Programmes = append(cleanTV.Programmes, cleanProg)
				}
			}
		}
	}

	output, err := xml.MarshalIndent(cleanTV, "", "  ")
	if err != nil {
		return nil, err
	}

	header := []byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE tv SYSTEM \"xmltv.dtd\">\n")
	return append(header, output...), nil
}

func (m *EPGManager) GetAllUpstreamChannels() []XMLTVChannel {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var allChannels []XMLTVChannel
	for _, tv := range m.parsedTVs {
		allChannels = append(allChannels, tv.Channels...)
	}
	return allChannels
}
