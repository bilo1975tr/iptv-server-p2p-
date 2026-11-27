package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"iptv-p2p-server/internal/acestream"
	"iptv-p2p-server/internal/api"
	"iptv-p2p-server/internal/auth"
	"iptv-p2p-server/internal/epg"
	"iptv-p2p-server/internal/mesh"
	"iptv-p2p-server/internal/seeder"
	"iptv-p2p-server/internal/storage"
	"iptv-p2p-server/internal/tracker"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		HTTPPort       int    `yaml:"http_port"`
		WebRTCListenIP string `yaml:"webrtc_listen_ip"`
		WebRTCMinPort  uint16 `yaml:"webrtc_min_port"`
		WebRTCMaxPort  uint16 `yaml:"webrtc_max_port"`
		AdminUser      string `yaml:"admin_user"`
		AdminPass      string `yaml:"admin_pass"`
		JWTSecret      string `yaml:"jwt_secret"`
		AESMasterKey   string `yaml:"aes_master_key"`
	} `yaml:"server"`
	Transcoder struct {
		EncoderType       string `yaml:"encoder_type"`
		FallbackEncoder   string `yaml:"fallback_encoder"`
		SmoothnessProfile string `yaml:"smoothness_profile"`
		PreserveSourceFPS *bool  `yaml:"preserve_source_fps"`
		GOPSeconds        int    `yaml:"gop_seconds"`
		HLSSegmentSeconds int    `yaml:"hls_segment_seconds"`
		HLSListSize       int    `yaml:"hls_list_size"`
		VideoBitrate      string `yaml:"video_bitrate"`
		MaxRate           string `yaml:"max_rate"`
		BufSize           string `yaml:"buf_size"`
		Preset            string `yaml:"preset"`
		TargetFPS         int    `yaml:"target_fps"`
	} `yaml:"transcoder"`
	Channels []struct {
		ID        string `yaml:"id"`
		Name      string `yaml:"name"`
		Tier      string `yaml:"tier"`
		SourceURL string `yaml:"source_url"`
	} `yaml:"channels"`
	AceStream acestream.Config `yaml:"acestream"`
	Tailscale mesh.Config      `yaml:"tailscale"`
}

func main() {
	log.Println("==================================================================")
	log.Println("🚀 Starting Ultra-Low Latency IPTV P2P Server (On-Demand + HLS Proxy)")
	log.Println("==================================================================")

	// 1. Load Configuration
	cfgData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config.yaml: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(cfgData, &cfg); err != nil {
		log.Fatalf("Failed to parse config.yaml: %v", err)
	}

	if cfg.Server.HTTPPort <= 0 {
		cfg.Server.HTTPPort = 8080
	}
	if cfg.Server.AdminUser == "" {
		cfg.Server.AdminUser = "admin"
	}
	if cfg.Server.AdminPass == "" {
		cfg.Server.AdminPass = "admin123"
	}
	if cfg.Transcoder.TargetFPS <= 0 {
		cfg.Transcoder.TargetFPS = 30
	}
	if cfg.Transcoder.GOPSeconds <= 0 {
		cfg.Transcoder.GOPSeconds = 2
	}
	if cfg.Transcoder.FallbackEncoder == "" {
		cfg.Transcoder.FallbackEncoder = "libx264"
	}
	preserveFPS := true
	if cfg.Transcoder.PreserveSourceFPS != nil {
		preserveFPS = *cfg.Transcoder.PreserveSourceFPS
	}

	// 2. Initialize Core Subsystems
	authMgr := auth.NewAuthManager(cfg.Server.JWTSecret, cfg.Server.AESMasterKey)
	authMgr.SetAdminCredentials(cfg.Server.AdminUser, cfg.Server.AdminPass)

	swarmTracker := tracker.NewSwarmTracker(authMgr, cfg.Server.WebRTCMinPort, cfg.Server.WebRTCMaxPort)

	seedServer := seeder.NewSeedServer(
		authMgr,
		cfg.Server.AESMasterKey,
		cfg.Transcoder.EncoderType,
		cfg.Transcoder.FallbackEncoder,
		cfg.Transcoder.Preset,
		cfg.Transcoder.TargetFPS,
		cfg.Transcoder.GOPSeconds,
		cfg.Server.JWTSecret,
	)
	seedServer.SetTranscoderOptions(
		cfg.Transcoder.SmoothnessProfile,
		preserveFPS,
		cfg.Transcoder.HLSSegmentSeconds,
		cfg.Transcoder.HLSListSize,
		cfg.Transcoder.VideoBitrate,
		cfg.Transcoder.MaxRate,
		cfg.Transcoder.BufSize,
	)
	swarmTracker.SetChannelTierResolver(seedServer.GetChannelTier)

	aceManager := acestream.NewManager(cfg.AceStream)
	seedServer.SetAceStreamManager(aceManager)

	meshMgr := mesh.NewManager(cfg.Tailscale)
	meshMgr.SetActiveChannelProvider(seedServer.GetActiveChannelIDs)
	seedServer.SetMeshManager(meshMgr)

	adminAPI := api.NewAdminAPI(authMgr, swarmTracker, api.StreamLifecycleHooks{
		OnUpsert: func(id, tier string, sourceURLs []string) {
			seedServer.RegisterChannelConfig(id, tier, sourceURLs)
		},
		OnDelete: func(id string) {
			seedServer.UnregisterChannel(id)
		},
		OnStop: func(id string) {
			seedServer.StopChannel(id)
		},
		GetActiveIDs:   seedServer.GetActiveChannelIDs,
		GetHTTPViewers: seedServer.GetActiveViewers,
		RecordViewer:   seedServer.RecordViewer,
		RemoveViewer:   seedServer.RemoveViewer,
	})
	adminAPI.SetAceStreamManager(aceManager)
	adminAPI.SetMeshManager(meshMgr)
	adminAPI.StartBillingPoller()

	if cfg.AceStream.Enabled {
		go func() {
			if err := aceManager.EnsurePrimaryRunning(); err != nil {
				log.Printf("⚠️ [ACESTREAM] Primary engine auto-start skipped/failed: %v", err)
			}
		}()
	}

	// 3. Load persisted state if exists (On-Demand: FFmpeg starts only when a viewer tunes in)
	if savedState, err := storage.LoadState(); err == nil && savedState != nil {
		log.Printf("[STORAGE] Restoring saved channels and EPG sources from disk (On-Demand mode)...")
		adminAPI.RestoreState(savedState)
		for _, sMap := range savedState.Streams {
			b, _ := json.Marshal(sMap)
			var s api.StreamRequest
			if json.Unmarshal(b, &s) == nil {
				adminAPI.RestoreStream(s)
			}
		}
		if deduped := adminAPI.DeduplicateByURL(); deduped > 0 {
			log.Printf("[STORAGE] Automatically merged %d channel(s) sharing duplicate stream URLs", deduped)
		}
		for epgID, epgURL := range savedState.EPGSources {
			log.Printf("[EPG] Restoring EPG source: %s", epgURL)
			go epg.GlobalEPG.AddSource(epgID, epgURL, 0)
		}
		for internalID, epgChannelID := range savedState.EPGMappings {
			epg.GlobalEPG.MapChannel(internalID, epgChannelID)
		}
	} else {
		for _, chCfg := range cfg.Channels {
			adminAPI.RestoreStream(api.StreamRequest{
				ID:         chCfg.ID,
				Name:       chCfg.Name,
				Tier:       chCfg.Tier,
				SourceURL:  chCfg.SourceURL,
				SourceURLs: []string{chCfg.SourceURL},
			})
		}
	}

	// 4. Setup HTTP Router & Static Asset File Servers
	mux := http.NewServeMux()

	adminFS := http.FileServer(http.Dir("web/admin"))
	publicFS := http.FileServer(http.Dir("web/public"))
	pcClientFS := http.FileServer(http.Dir("pc-client"))
	tvClientFS := http.FileServer(http.Dir("tv-client"))

	noCacheStatic := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			next.ServeHTTP(w, r)
		})
	}

	mux.Handle("/admin/", http.StripPrefix("/admin", adminFS))
	mux.HandleFunc("/pc", func(w http.ResponseWriter, r *http.Request) {
		target := "/pc/"
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusFound)
	})
	mux.Handle("/pc/", noCacheStatic(http.StripPrefix("/pc/", pcClientFS)))
	mux.HandleFunc("/tv", func(w http.ResponseWriter, r *http.Request) {
		target := "/tv/"
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, target, http.StatusFound)
	})
	mux.Handle("/tv/", noCacheStatic(http.StripPrefix("/tv/", tvClientFS)))
	mux.Handle("/player.html", publicFS)
	mux.Handle("/", publicFS)

	mux.HandleFunc("/ws/signaling", swarmTracker.HandleWebSocket)
	mux.HandleFunc("/live/", seedServer.ServeHTTP)
	mux.HandleFunc("/api/stream/", seedServer.ServeHLSProxy)
	mux.HandleFunc("/api/mesh/", seedServer.ServeMesh)

	adminAPI.RegisterRoutes(mux)

	// Dedicated WireGuard-only HTTP mux for Tailscale Mesh peer transport
	meshMux := http.NewServeMux()
	meshMux.HandleFunc("/api/mesh/", seedServer.ServeMesh)
	meshMgr.SetMeshHTTPHandler(meshMux)
	if meshMgr.IsEnabled() {
		if err := meshMgr.Start(); err != nil {
			log.Printf("⚠️ [TAILSCALE-MESH] Startup failed: %v", err)
		}
	}

	// 5. Start Dual-Stack IPv4 & IPv6 HTTP Server
	listenHost := strings.Trim(cfg.Server.WebRTCListenIP, "[]")
	var bindAddr string
	if listenHost == "" || listenHost == "::" || listenHost == "0.0.0.0" {
		bindAddr = ":" + strconv.Itoa(cfg.Server.HTTPPort)
	} else {
		bindAddr = net.JoinHostPort(listenHost, strconv.Itoa(cfg.Server.HTTPPort))
	}
	log.Printf("🌐 Server listening on Dual-Stack IPv6 & IPv4 (%s)...", bindAddr)
	log.Printf("🔑 Admin Dashboard: http://localhost:%d/admin/ (Auth Active)", cfg.Server.HTTPPort)
	log.Printf("📺 Web Test Player: http://localhost:%d/player.html", cfg.Server.HTTPPort)
	log.Printf("🧩 Multiview Player: http://localhost:%d/multiview.html", cfg.Server.HTTPPort)
	log.Printf("💻 PC Masaüstü İstemcisi: http://localhost:%d/pc/", cfg.Server.HTTPPort)
	log.Printf("📺 Smart TV İstemcisi: http://localhost:%d/tv/", cfg.Server.HTTPPort)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("🛑 Shutting down Tailscale Mesh, AceStream sessions and server...")
		meshMgr.Stop()
		aceManager.StopAll()
		os.Exit(0)
	}()

	if err := http.ListenAndServe(bindAddr, mux); err != nil {
		meshMgr.Stop()
		aceManager.StopAll()
		log.Fatalf("Server shutdown with error: %v", err)
	}
}
