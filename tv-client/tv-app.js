(function () {
    'use strict';

    const DEFAULT_SERVER = window.location.protocol === 'file:' ? 'http://localhost:8080' : window.location.origin;
    let serverBase = (localStorage.getItem('iptv_tv_server_url') || DEFAULT_SERVER).replace(/\/+$/, '');
    const viewerTabId = 'tv_' + Math.random().toString(36).slice(2, 10);

    const urlParams = new URLSearchParams(window.location.search);
    let currentChannelId = urlParams.get('channel_id') || localStorage.getItem('iptv_tv_last_ch') || '';
    let token = urlParams.get('token') || localStorage.getItem('iptv_tv_token') || '';
    let currentUser = null;
    let userFavorites = new Set();

    let channels = [];
    let guideList = [];
    let activeCategory = 'all';
    let epgData = {};

    let hlsInstance = null;
    let msePollTimer = null;
    let activeMediaSource = null;
    let activeBlobUrl = null;
    let p2pEngine = null;
    let heartbeatTimer = null;
    let osdTimer = null;
    let osdTickTimer = null;
    let zapBuffer = '';
    let zapTimer = null;
    let userExplicitMute = localStorage.getItem('iptv_tv_muted') === '1';

    // 10-Foot Focus State
    let osdFocusIdx = 0;
    let guideCol = 'list'; // 'cats' | 'list'
    let guideCatIdx = 0;
    let guideChIdx = 0;
    let modalFocusIdx = 0;

    const video = document.getElementById('tvVideo');
    const zapBox = document.getElementById('tvZapBox');
    const centerMsg = document.getElementById('tvCenterMsg');
    const settingsModal = document.getElementById('tvSettingsModal');

    const CAT_TITLES = {
        all: 'Tüm Kanallar',
        favorites: 'Favori Kanallar',
        free: 'Ücretsiz Kanallar',
        paid: 'Ücretli Kanallar',
        acestream: 'AceStream Kanalları'
    };

    function apiUrl(path) {
        return serverBase + path;
    }

    function escapeHtml(str) {
        if (str === null || str === undefined) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function authHeaders(extra) {
        const h = Object.assign({ 'X-Requested-With': 'XMLHttpRequest' }, extra || {});
        if (token) h['Authorization'] = 'Bearer ' + token;
        return h;
    }

    function formatClockHHMM(unixSec) {
        const d = new Date(unixSec * 1000);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
    }

    function formatLag(sec) {
        const s = Math.max(0, Math.floor(sec));
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}:${String(r).padStart(2, '0')}`;
    }

    function updateTopClock() {
        const el = document.getElementById('tvTopClock');
        if (el) el.innerText = formatClockHHMM(Math.floor(Date.now() / 1000));
    }

    function loadP2PScript() {
        return new Promise((resolve) => {
            if (typeof window.IPTVP2PEngine !== 'undefined') {
                resolve(true);
                return;
            }
            const s = document.createElement('script');
            s.src = apiUrl('/p2p-engine.js');
            s.onload = () => resolve(true);
            s.onerror = () => resolve(false);
            document.head.appendChild(s);
        });
    }

    function getDvrLagSeconds() {
        try {
            if (video && video.seekable && video.seekable.length > 0 && Number.isFinite(video.currentTime)) {
                const edge = video.seekable.end(video.seekable.length - 1);
                if (Number.isFinite(edge) && edge > 0) {
                    return Math.max(0, edge - video.currentTime);
                }
            }
        } catch (_) {}
        return 0;
    }

    function findProgramAtUnix(schedule, targetUnix) {
        if (!Array.isArray(schedule) || schedule.length === 0) {
            return { current: null, next: null };
        }
        for (let i = 0; i < schedule.length; i++) {
            const p = schedule[i];
            const st = Number(p.start_unix || 0);
            const sp = Number(p.stop_unix || 0);
            if (st > 0 && sp > 0 && targetUnix >= st && targetUnix < sp) {
                return { current: p, next: schedule[i + 1] || null };
            }
        }
        for (let i = 0; i < schedule.length; i++) {
            const p = schedule[i];
            if (Number(p.start_unix || 0) > targetUnix) {
                return { current: null, next: p };
            }
        }
        return { current: null, next: null };
    }

    function isGuideOpen() {
        return document.body.classList.contains('guide-open');
    }

    function isModalOpen() {
        return settingsModal.style.display === 'flex';
    }

    function isCenterMsgOpen() {
        return centerMsg.style.display === 'flex';
    }

    function showStatusBanner(text) {
        const el = document.getElementById('tvStatusBanner');
        if (!el) return;
        el.innerText = text;
        el.style.display = 'block';
    }

    function hideStatusBanner() {
        const el = document.getElementById('tvStatusBanner');
        if (el) el.style.display = 'none';
    }

    function markChannelWorking(chId) {
        if (chId) localStorage.setItem('iptv_tv_last_ch', chId);
    }

    function clearBadLastChannel(chId) {
        if (chId && localStorage.getItem('iptv_tv_last_ch') === chId) {
            localStorage.removeItem('iptv_tv_last_ch');
        }
    }

    function showCenterMessage(title, body, opts) {
        hideStatusBanner();
        const cfg = (typeof opts === 'object' && opts !== null)
            ? opts
            : { showRetry: opts !== false, showSkip: true, showSettings: true };
        document.getElementById('tvMsgTitle').innerText = title;
        document.getElementById('tvMsgBody').innerText = body;

        const retryBtn = document.getElementById('tvRetryBtn');
        const nextBtn = document.getElementById('tvNextChBtn');
        const guideBtn = document.getElementById('tvGuideMsgBtn');
        const settingsBtn = document.getElementById('tvSettingsMsgBtn');

        if (retryBtn) retryBtn.style.display = cfg.showRetry === false ? 'none' : 'inline-block';
        if (nextBtn) nextBtn.style.display = (cfg.showSkip !== false && channels.length > 1) ? 'inline-block' : 'none';
        if (guideBtn) guideBtn.style.display = channels.length > 0 ? 'inline-block' : 'none';
        if (settingsBtn) settingsBtn.style.display = cfg.showSettings === false ? 'none' : 'inline-block';

        centerMsg.style.display = 'flex';
        setTimeout(() => {
            if (!isCenterMsgOpen()) return;
            if (cfg.focusSettings && settingsBtn) settingsBtn.focus();
            else if (nextBtn && nextBtn.style.display !== 'none') nextBtn.focus();
            else if (guideBtn && guideBtn.style.display !== 'none') guideBtn.focus();
            else if (retryBtn && retryBtn.style.display !== 'none') retryBtn.focus();
        }, 30);
    }

    function hideCenterMessage() {
        centerMsg.style.display = 'none';
    }

    function getOsdButtons() {
        return Array.from(document.querySelectorAll('#osdActions .osd-nav-btn'));
    }

    function applyOsdFocus() {
        const btns = getOsdButtons();
        if (btns.length === 0) return;
        osdFocusIdx = ((osdFocusIdx % btns.length) + btns.length) % btns.length;
        btns.forEach((b, i) => b.classList.toggle('tv-focused', i === osdFocusIdx));
    }

    function showOsd(keepOpen) {
        document.body.classList.add('osd-visible');
        updateOsdInfo();
        applyOsdFocus();
        if (osdTimer) clearTimeout(osdTimer);
        if (!keepOpen && !isGuideOpen() && !isModalOpen()) {
            osdTimer = setTimeout(() => {
                if (!isGuideOpen() && !isModalOpen() && !video.paused) {
                    hideOsd();
                }
            }, 5500);
        }
    }

    function hideOsd() {
        if (osdTimer) {
            clearTimeout(osdTimer);
            osdTimer = null;
        }
        document.body.classList.remove('osd-visible');
    }

    function tryAutoUnmute() {
        if (userExplicitMute) return;
        if (video.muted) {
            video.muted = false;
            video.volume = 1;
            const p = video.play();
            if (p && typeof p.catch === 'function') {
                p.catch(() => {
                    video.muted = true;
                    video.play().catch(() => {});
                });
            }
        }
    }

    function updateOsdInfo() {
        const chIdx = channels.findIndex(c => c.id === currentChannelId);
        const ch = chIdx >= 0 ? channels[chIdx] : null;
        document.getElementById('osdChNum').innerText = String(chIdx >= 0 ? chIdx + 1 : 1).padStart(3, '0');
        document.getElementById('osdChName').innerText = ch ? ch.name : 'Kanal Seçilmedi';

        const lagSec = getDvrLagSeconds();
        const isRewound = lagSec > 15;
        const targetUnix = Math.floor(Date.now() / 1000 - (isRewound ? lagSec : 0));
        const modeEl = document.getElementById('osdModeText');
        if (video.paused) {
            modeEl.innerText = isRewound ? `DURAKLATILDI · -${formatLag(lagSec)}` : 'DURAKLATILDI';
            modeEl.style.color = 'var(--tv-gold)';
        } else if (isRewound) {
            modeEl.innerText = `DVR GERİ SARMA · -${formatLag(lagSec)}`;
            modeEl.style.color = 'var(--tv-gold)';
        } else {
            modeEl.innerText = ch && ch.is_acestream ? 'CANLI · ACESTREAM' : 'CANLI';
            modeEl.style.color = 'var(--tv-accent)';
        }

        const playBtn = document.getElementById('osdPlayBtn');
        if (playBtn) playBtn.innerText = video.paused ? 'Oynat' : 'Duraklat';

        const liveBtn = document.getElementById('osdLiveBtn');
        if (liveBtn) liveBtn.style.opacity = isRewound ? '1' : '0.65';

        const favBtn = document.querySelector('#osdActions .osd-nav-btn[data-action="fav"]');
        if (favBtn && ch) {
            favBtn.innerText = userFavorites.has(ch.id) ? 'Favoride ★' : 'Favori ☆';
        }

        const guide = ch ? epgData[ch.id] : null;
        let curr = null;
        let next = null;
        if (guide) {
            const match = findProgramAtUnix(guide.schedule, targetUnix);
            if (isRewound) {
                curr = match.current;
                next = match.next;
            } else {
                curr = match.current || guide.current || null;
                next = match.next || guide.next || null;
            }
        }

        const nowEl = document.getElementById('osdEpgNow');
        const timeEl = document.getElementById('osdEpgTime');
        const nextEl = document.getElementById('osdEpgNext');
        const fillEl = document.getElementById('osdProgressFill');

        if (curr) {
            nowEl.innerText = curr.title;
            timeEl.innerText = `${curr.start} - ${curr.stop}`;
            const st = Number(curr.start_unix || 0);
            const sp = Number(curr.stop_unix || 0);
            if (st > 0 && sp > st) {
                const pct = Math.max(0, Math.min(100, ((targetUnix - st) / (sp - st)) * 100));
                fillEl.style.width = `${pct.toFixed(1)}%`;
            } else {
                fillEl.style.width = '0%';
            }
        } else if (isRewound && guide) {
            nowEl.innerText = 'Bu saat için geçmiş EPG kaydı yok';
            timeEl.innerText = formatClockHHMM(targetUnix);
            fillEl.style.width = '0%';
        } else {
            nowEl.innerText = 'Yayın bilgisi bulunamadı';
            timeEl.innerText = '--:-- - --:--';
            fillEl.style.width = '0%';
        }

        nextEl.innerText = next ? `Sonraki (${next.start}): ${next.title}` : 'Sonraki: --';
    }

    function applyUserSession(user, newToken) {
        if (user && newToken) {
            token = newToken;
            localStorage.setItem('iptv_tv_token', newToken);
            if (user.tier === 'admin') {
                localStorage.setItem('iptv_admin_token', newToken);
            }
        } else if (!user) {
            token = '';
            localStorage.removeItem('iptv_tv_token');
        }
        currentUser = user || null;
        userFavorites = new Set((user && Array.isArray(user.favorites)) ? user.favorites : []);
        if (!currentUser && activeCategory === 'favorites') {
            activeCategory = 'all';
        }
        updateOsdInfo();
        if (isGuideOpen()) renderGuideList();
    }

    async function loadSession() {
        try {
            const res = await fetch(apiUrl('/api/auth/me'), { credentials: 'include', headers: authHeaders() });
            if (res.ok) {
                const me = await res.json();
                if (me.authenticated && me.user) {
                    applyUserSession(me.user, me.token || '');
                    return;
                }
            }
            const storedAdminTok = localStorage.getItem('iptv_admin_token') || '';
            if (storedAdminTok && storedAdminTok !== token) {
                const admRes = await fetch(apiUrl('/api/auth/me'), {
                    credentials: 'include',
                    headers: { 'X-Requested-With': 'XMLHttpRequest', 'Authorization': 'Bearer ' + storedAdminTok }
                });
                if (admRes.ok) {
                    const admMe = await admRes.json();
                    if (admMe.authenticated && admMe.user) {
                        applyUserSession(admMe.user, admMe.token || storedAdminTok);
                        return;
                    }
                }
            }
        } catch (_) {}
        applyUserSession(null, '');
    }

    async function loadEpg() {
        try {
            const res = await fetch(apiUrl('/api/epg/json'));
            if (res.ok) {
                epgData = await res.json();
                updateOsdInfo();
                if (isGuideOpen()) renderGuideList();
            }
        } catch (_) {}
    }

    function canCurrentUserPlay(ch) {
        if (!ch || ch.tier !== 'paid') return true;
        return !!(currentUser && (currentUser.tier === 'paid' || currentUser.tier === 'admin'));
    }

    async function loadChannels() {
        try {
            const res = await fetch(apiUrl('/api/channels'));
            if (!res.ok) throw new Error('HTTP ' + res.status);
            const data = await res.json();
            channels = Array.isArray(data) ? data : [];
            channels.sort((a, b) => {
                const pa = a.tier === 'paid' ? 1 : 0;
                const pb = b.tier === 'paid' ? 1 : 0;
                if (pa !== pb) return pa - pb;
                return (a.name || '').localeCompare(b.name || '', 'tr');
            });
            if (channels.length === 0) {
                showCenterMessage('Kanal Bulunamadı', 'Admin panelinden kanal veya M3U listesi ekleyin.', true);
                return;
            }
            const existing = channels.find(c => c.id === currentChannelId);
            if (!existing || !canCurrentUserPlay(existing)) {
                const playable = channels.find(canCurrentUserPlay);
                currentChannelId = (playable || channels[0]).id;
            }
            hideCenterMessage();
            playChannel(currentChannelId);
        } catch (_) {
            showCenterMessage(
                'Sunucuya Bağlanılamadı',
                `${serverBase} adresine ulaşılamıyor. Ayarlardan sunucu IP adresini düzenleyebilirsiniz.`,
                true
            );
        }
    }

    function sendHeartbeat(chId, action) {
        if (!chId) return;
        fetch(apiUrl('/api/channels/heartbeat'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ channel_id: chId, viewer_id: viewerTabId, action: action || 'ping' }),
            keepalive: action === 'leave'
        }).catch(() => {});
    }

    async function refreshViewers() {
        try {
            const res = await fetch(apiUrl('/api/channels/viewers'));
            if (!res.ok) return;
            const map = await res.json();
            channels.forEach(c => {
                c.viewer_count = Number(map[c.id] || 0);
            });
            if (isGuideOpen()) renderGuideList();
        } catch (_) {}
    }

    function connectP2P(chId) {
        if (p2pEngine) {
            p2pEngine.destroy();
            p2pEngine = null;
        }
        if (typeof window.IPTVP2PEngine === 'undefined') return;
        p2pEngine = new window.IPTVP2PEngine({
            channelId: chId,
            getToken: () => token,
            maxPeers: 8,
            onStatus: () => {},
            onStats: (s) => {
                const el = document.getElementById('osdP2pInfo');
                if (el) {
                    el.innerText = `Swarm: ${s.totalSwarmPeers} Eş · %${s.offloadPct} P2P`;
                }
            }
        });
    }

    function stopPlayback() {
        if (p2pEngine && !p2pEngine.destroyed) {
            p2pEngine.stopPausedDvrPrefetch();
        }
        if (hlsInstance) {
            hlsInstance.destroy();
            hlsInstance = null;
        }
        if (msePollTimer) {
            clearInterval(msePollTimer);
            msePollTimer = null;
        }
        activeMediaSource = null;
        video.removeAttribute('src');
        video.load();
        if (activeBlobUrl) {
            URL.revokeObjectURL(activeBlobUrl);
            activeBlobUrl = null;
        }
    }

    function switchChannel(chId) {
        if (!chId) return;
        if (currentChannelId && currentChannelId !== chId) {
            sendHeartbeat(currentChannelId, 'leave');
        }
        currentChannelId = chId;
        playChannel(chId);
    }

    function zapRelative(step) {
        if (channels.length === 0) return;
        const idx = channels.findIndex(c => c.id === currentChannelId);
        const cur = idx >= 0 ? idx : 0;
        const nextIdx = (cur + step + channels.length) % channels.length;
        switchChannel(channels[nextIdx].id);
    }

    async function playChannel(chId) {
        const ch = channels.find(c => c.id === chId);
        if (!ch) return;

        hideCenterMessage();
        hideStatusBanner();
        stopPlayback();
        showOsd(false);

        sendHeartbeat(chId, 'join');
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        heartbeatTimer = setInterval(() => sendHeartbeat(currentChannelId, 'ping'), 12000);

        connectP2P(chId);

        if (ch.is_acestream) {
            showStatusBanner(`${ch.name}: AceStream P2P yayını hazırlanıyor...`);
        }

        const proxyUrl = apiUrl(`/api/stream/${encodeURIComponent(chId)}/master.m3u8?token=${encodeURIComponent(token)}&vid=${encodeURIComponent(viewerTabId)}`);

        try {
            const res = await fetch(proxyUrl);
            if (res.status === 403) {
                showCenterMessage('Abonelik Gerekli', `${ch.name} ücretli bir kanaldır. Ayarlardan hesabınızla giriş yapın.`, {
                    showRetry: true,
                    showSkip: true,
                    showSettings: true,
                    focusSettings: true
                });
                return;
            }
            const ct = (res.headers.get('content-type') || '').toLowerCase();
            if (res.ok && (ct.includes('mpegurl') || ct.includes('text'))) {
                if (typeof Hls !== 'undefined' && Hls.isSupported()) {
                    const cfg = {
                        lowLatencyMode: false,
                        liveSyncDurationCount: 4,
                        liveMaxLatencyDurationCount: Infinity,
                        maxLiveSyncPlaybackRate: 1,
                        maxBufferLength: 60,
                        maxMaxBufferLength: 120,
                        backBufferLength: 90,
                        maxBufferHole: 1.5,
                        nudgeOffset: 0.2,
                        nudgeMaxRetry: 8,
                        fragLoadingTimeOut: 20000,
                        fragLoadingMaxRetry: 6,
                        fragLoadingRetryDelay: 500,
                        manifestLoadingTimeOut: 15000,
                        manifestLoadingMaxRetry: 6,
                        levelLoadingTimeOut: 15000,
                        levelLoadingMaxRetry: 6,
                        enableWorker: true
                    };
                    if (p2pEngine && Hls.DefaultConfig && Hls.DefaultConfig.loader) {
                        cfg.pLoader = p2pEngine.createHlsPlaylistLoaderClass(Hls.DefaultConfig.loader);
                        cfg.fLoader = p2pEngine.createHlsLoaderClass(Hls.DefaultConfig.loader);
                    }
                    let netRetries = 0;
                    let mediaRecover = 0;
                    hlsInstance = new Hls(cfg);
                    hlsInstance.loadSource(proxyUrl);
                    hlsInstance.attachMedia(video);

                    hlsInstance.on(Hls.Events.MANIFEST_PARSED, () => {
                        hideCenterMessage();
                        hideStatusBanner();
                        markChannelWorking(chId);
                        video.play().catch(() => {});
                    });
                    hlsInstance.on(Hls.Events.FRAG_LOADED, () => {
                        hideCenterMessage();
                        hideStatusBanner();
                        markChannelWorking(chId);
                        netRetries = 0;
                    });
                    hlsInstance.on(Hls.Events.ERROR, (_, data) => {
                        if (!data || !data.fatal) return;
                        if (data.type === Hls.ErrorTypes.NETWORK_ERROR && netRetries < 4) {
                            netRetries++;
                            setTimeout(() => { if (hlsInstance) hlsInstance.startLoad(); }, 1200);
                            return;
                        }
                        if (data.type === Hls.ErrorTypes.MEDIA_ERROR && mediaRecover < 3) {
                            mediaRecover++;
                            try { hlsInstance.recoverMediaError(); } catch (_) {}
                            return;
                        }
                        startMSEFallback(chId, ch.name, !!ch.is_acestream);
                    });
                    return;
                } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
                    video.src = proxyUrl;
                    hideCenterMessage();
                    hideStatusBanner();
                    markChannelWorking(chId);
                    video.play().catch(() => {});
                    return;
                }
            }
        } catch (_) {}

        startMSEFallback(chId, ch.name, !!ch.is_acestream);
    }

    function startMSEFallback(chId, chName, isAce) {
        if (typeof MediaSource === 'undefined') {
            showCenterMessage(chName, 'TV tarayıcısı MediaSource desteklemiyor.', { showRetry: true, showSkip: true });
            return;
        }
        stopPlayback();
        showStatusBanner(`${chName}: ${isAce ? 'AceStream fMP4 başlatılıyor...' : 'Yayın segmenti bekleniyor...'}`);

        const ms = new MediaSource();
        activeMediaSource = ms;
        activeBlobUrl = URL.createObjectURL(ms);
        video.src = activeBlobUrl;

        ms.addEventListener('sourceopen', () => {
            if (activeMediaSource !== ms) return;
            let sb = null;
            let initLoaded = false;
            let seq = 0;
            let busy = false;
            const startTs = Date.now();

            const createSB = (codec) => {
                const list = codec === 'HEVC'
                    ? ['video/mp4; codecs="hvc1.1.6.L93.B0, mp4a.40.2"', 'video/mp4; codecs="hev1.1.6.L93.B0, mp4a.40.2"']
                    : [
                        'video/mp4; codecs="avc1.4d401f, mp4a.40.2"',
                        'video/mp4; codecs="avc1.64001f, mp4a.40.2"',
                        'video/mp4; codecs="avc1.42E01E, mp4a.40.2"'
                      ];
                for (const m of list) {
                    if (MediaSource.isTypeSupported(m)) {
                        try { return ms.addSourceBuffer(m); } catch (_) {}
                    }
                }
                return null;
            };

            const attachSBListeners = (sourceBuf) => {
                let initialAutoStarted = false;
                sourceBuf.addEventListener('updateend', () => {
                    if (currentChannelId !== chId || activeMediaSource !== ms) return;
                    try {
                        if (sourceBuf.buffered.length > 0) {
                            const bufStart = sourceBuf.buffered.start(0);
                            const bufEnd = sourceBuf.buffered.end(sourceBuf.buffered.length - 1);
                            if (video.currentTime < bufStart) {
                                video.currentTime = bufStart;
                            }
                            if (!initialAutoStarted && bufEnd - bufStart >= 0.5) {
                                initialAutoStarted = true;
                                if (video.paused) video.play().catch(() => {});
                            }
                        }
                    } catch (_) {}
                });
            };

            msePollTimer = setInterval(async () => {
                if (busy || currentChannelId !== chId || activeMediaSource !== ms || ms.readyState !== 'open' || (sb && sb.updating)) return;
                busy = true;
                try {
                    if (!initLoaded) {
                        const elapsedSec = Math.floor((Date.now() - startTs) / 1000);
                        const r = await fetch(apiUrl(`/live/${encodeURIComponent(chId)}/init.mp4?token=${encodeURIComponent(token)}&vid=${encodeURIComponent(viewerTabId)}`));
                        if (r.status === 403) {
                            clearInterval(msePollTimer);
                            showCenterMessage('Abonelik Gerekli', 'Ücretli kanal için ayarlardan giriş yapın.', {
                                showRetry: true,
                                showSkip: true,
                                showSettings: true,
                                focusSettings: true
                            });
                            return;
                        }
                        if (r.status === 404 || r.status === 502 || r.status === 503) {
                            clearInterval(msePollTimer);
                            clearBadLastChannel(chId);
                            showCenterMessage(chName, 'Bu kanalın kaynak sunucusu yanıt vermiyor. Sonraki kanala geçebilir veya rehberden başka bir kanal seçebilirsiniz.', {
                                showRetry: true,
                                showSkip: true,
                                showSettings: true
                            });
                            return;
                        }
                        if (!r.ok || r.status === 202) {
                            if (elapsedSec > 45) {
                                clearInterval(msePollTimer);
                                clearBadLastChannel(chId);
                                showCenterMessage(chName, 'Yayın kaynağı yanıt vermedi veya FFmpeg başlatılamadı.', {
                                    showRetry: true,
                                    showSkip: true,
                                    showSettings: true
                                });
                            } else {
                                showStatusBanner(`${chName}: ${isAce ? 'AceStream fMP4 hazırlanıyor' : 'Yayın segmenti bekleniyor'}... (${elapsedSec} sn)`);
                            }
                            return;
                        }
                        const codec = (r.headers.get('X-Chunk-Codec') || 'AVC').toUpperCase();
                        const buf = await r.arrayBuffer();
                        if (!sb) {
                            sb = createSB(codec);
                            if (!sb) {
                                if (codec === 'HEVC' && elapsedSec < 12) return;
                                clearInterval(msePollTimer);
                                showCenterMessage(chName, `Tarayıcı ${codec} video kodeğini desteklemiyor.`, { showRetry: true, showSkip: true });
                                return;
                            }
                            attachSBListeners(sb);
                        }
                        sb.appendBuffer(buf);
                        initLoaded = true;
                        hideCenterMessage();
                        hideStatusBanner();
                        markChannelWorking(chId);
                        video.play().catch(() => {});
                        return;
                    }
                    if (sb.buffered.length > 0 && video.currentTime - sb.buffered.start(0) > 120) {
                        sb.remove(sb.buffered.start(0), video.currentTime - 60);
                        return;
                    }
                    const u = seq === 0
                        ? apiUrl(`/live/${encodeURIComponent(chId)}/latest?token=${encodeURIComponent(token)}&vid=${encodeURIComponent(viewerTabId)}`)
                        : apiUrl(`/live/${encodeURIComponent(chId)}/chunk/${seq}?token=${encodeURIComponent(token)}&vid=${encodeURIComponent(viewerTabId)}`);
                    const r = await fetch(u);
                    if (r.status === 404 || r.status === 410) { seq = 0; return; }
                    if (r.ok && r.status !== 204) {
                        const buf = await r.arrayBuffer();
                        const hdrSeq = parseInt(r.headers.get('X-Chunk-Seq') || '0', 10);
                        if (hdrSeq > 0) seq = hdrSeq + 1;
                        else if (seq > 0) seq++;
                        if (!sb.updating && ms.readyState === 'open') sb.appendBuffer(buf);
                    }
                } catch (err) {
                    if (err && err.name === 'QuotaExceededError' && sb && sb.buffered.length > 0 && !sb.updating) {
                        try { sb.remove(sb.buffered.start(0), Math.max(sb.buffered.start(0) + 5, video.currentTime - 5)); } catch (_) {}
                    }
                } finally {
                    busy = false;
                }
            }, 400);
        });
    }

    function seekRelative(deltaSec) {
        try {
            if (!video.seekable || video.seekable.length === 0) return;
            const s = video.seekable.start(0);
            const e = video.seekable.end(video.seekable.length - 1);
            video.currentTime = Math.min(e - 1, Math.max(s, video.currentTime + deltaSec));
            showOsd(false);
        } catch (_) {}
    }

    function jumpToLive() {
        try {
            if (video.seekable && video.seekable.length > 0) {
                const e = video.seekable.end(video.seekable.length - 1);
                if (Number.isFinite(e) && e > 0) video.currentTime = Math.max(0, e - 3);
            }
            video.play().catch(() => {});
            showOsd(false);
        } catch (_) {}
    }

    function togglePlayPause() {
        if (video.paused) {
            video.play().catch(() => {});
            showOsd(false);
        } else {
            video.pause();
            showOsd(true);
        }
    }

    async function toggleCurrentFavorite() {
        if (!currentChannelId) return;
        if (!currentUser || !currentUser.user_id) {
            openTvSettings('Favori eklemek için kullanıcı adınızla giriş yapın.');
            return;
        }
        try {
            const res = await fetch(apiUrl('/api/user/favorites/toggle'), {
                method: 'POST',
                credentials: 'include',
                headers: authHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({ channel_id: currentChannelId })
            });
            if (res.ok) {
                const d = await res.json();
                userFavorites = new Set(Array.isArray(d.favorites) ? d.favorites : []);
                updateOsdInfo();
                if (isGuideOpen()) renderGuideList();
            }
        } catch (_) {}
    }

    function executeOsdAction(action) {
        switch (action) {
            case 'guide':
                openGuide();
                break;
            case 'playpause':
                togglePlayPause();
                break;
            case 'rw5m':
                seekRelative(-300);
                break;
            case 'ff5m':
                seekRelative(300);
                break;
            case 'live':
                jumpToLive();
                break;
            case 'fav':
                toggleCurrentFavorite();
                break;
            case 'settings':
                openTvSettings();
                break;
        }
    }

    // 10-Foot Guide Drawer
    function getCategoryButtons() {
        return Array.from(document.querySelectorAll('#tvCategoryRail .tv-cat-btn'));
    }

    function renderGuideList() {
        const cats = getCategoryButtons();
        cats.forEach((b, i) => {
            const cat = b.dataset.cat;
            b.classList.toggle('active', cat === activeCategory);
            b.classList.toggle('tv-focused', guideCol === 'cats' && i === guideCatIdx);
        });

        document.getElementById('tvGuideTitle').innerText = CAT_TITLES[activeCategory] || 'Kanallar';

        guideList = channels.filter(ch => {
            if (activeCategory === 'favorites') return userFavorites.has(ch.id);
            if (activeCategory === 'free') return ch.tier !== 'paid';
            if (activeCategory === 'paid') return ch.tier === 'paid';
            if (activeCategory === 'acestream') return !!ch.is_acestream;
            return true;
        });

        const listEl = document.getElementById('tvChannelList');
        listEl.innerHTML = '';

        if (guideList.length === 0) {
            listEl.innerHTML = '<div style="padding:36px; font-size:1.25rem; color:var(--text-muted);">Bu kategoride kanal yok.</div>';
            guideChIdx = 0;
            return;
        }

        if (guideChIdx >= guideList.length) guideChIdx = 0;
        if (guideChIdx < 0) guideChIdx = 0;

        const nowUnix = Math.floor(Date.now() / 1000);

        guideList.forEach((ch, idx) => {
            const card = document.createElement('div');
            const isPlaying = ch.id === currentChannelId;
            card.className = 'tv-ch-card'
                + (isPlaying ? ' is-playing' : '')
                + (guideCol === 'list' && idx === guideChIdx ? ' tv-focused' : '');

            const globalNum = channels.findIndex(c => c.id === ch.id) + 1;
            const numDiv = document.createElement('div');
            numDiv.className = 'tv-card-num tabular';
            numDiv.innerText = String(globalNum > 0 ? globalNum : idx + 1).padStart(3, '0');

            const logoDiv = document.createElement('div');
            logoDiv.className = 'tv-card-logo';
            const initials = (ch.name || '?').slice(0, 2).toUpperCase();
            if (ch.logo_url) {
                const img = document.createElement('img');
                img.src = ch.logo_url;
                img.alt = ch.name;
                img.referrerPolicy = 'no-referrer';
                img.onerror = () => { logoDiv.textContent = initials; };
                logoDiv.appendChild(img);
            } else {
                logoDiv.textContent = initials;
            }

            let epgLine = 'Canlı Yayın';
            const g = epgData[ch.id];
            if (g) {
                const match = findProgramAtUnix(g.schedule, nowUnix);
                const curr = match.current || g.current;
                if (curr && curr.title) {
                    epgLine = `${curr.start ? curr.start + ' ' : ''}${curr.title}`;
                }
            }

            const bodyDiv = document.createElement('div');
            bodyDiv.className = 'tv-card-body';
            bodyDiv.innerHTML = `
                <div class="tv-card-name">${escapeHtml(ch.name)}${userFavorites.has(ch.id) ? ' ★' : ''}</div>
                <div class="tv-card-epg">${escapeHtml(epgLine)}</div>
            `;

            const metaDiv = document.createElement('div');
            metaDiv.className = 'tv-card-meta tabular';
            const tierStr = ch.tier === 'paid' ? 'Ücretli' : 'Ücretsiz';
            metaDiv.innerText = `${tierStr} · ${ch.viewer_count || 0} izleyici`;

            card.appendChild(numDiv);
            card.appendChild(logoDiv);
            card.appendChild(bodyDiv);
            card.appendChild(metaDiv);

            card.onclick = () => {
                closeGuide();
                switchChannel(ch.id);
            };

            listEl.appendChild(card);
        });

        applyGuideFocusScroll();
    }

    function applyGuideFocusScroll() {
        const cats = getCategoryButtons();
        cats.forEach((b, i) => b.classList.toggle('tv-focused', guideCol === 'cats' && i === guideCatIdx));

        const cards = Array.from(document.querySelectorAll('#tvChannelList .tv-ch-card'));
        cards.forEach((c, i) => {
            const focused = guideCol === 'list' && i === guideChIdx;
            c.classList.toggle('tv-focused', focused);
            if (focused) c.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        });
    }

    function openGuide() {
        hideOsd();
        document.body.classList.add('guide-open');
        guideCol = 'list';
        guideCatIdx = Math.max(0, getCategoryButtons().findIndex(b => b.dataset.cat === activeCategory));
        renderGuideList();
        const curIdx = guideList.findIndex(c => c.id === currentChannelId);
        if (curIdx >= 0) {
            guideChIdx = curIdx;
            applyGuideFocusScroll();
        }
    }

    function closeGuide() {
        document.body.classList.remove('guide-open');
        showOsd(false);
    }

    // Numeric Zap
    function handleNumericZapDigit(digit) {
        zapBuffer = (zapBuffer + digit).slice(-4);
        zapBox.innerText = zapBuffer.padStart(3, '0');
        zapBox.style.display = 'block';
        if (zapTimer) clearTimeout(zapTimer);
        zapTimer = setTimeout(commitNumericZap, 1350);
    }

    function commitNumericZap() {
        if (zapTimer) {
            clearTimeout(zapTimer);
            zapTimer = null;
        }
        const num = parseInt(zapBuffer, 10);
        zapBuffer = '';
        zapBox.style.display = 'none';
        if (!Number.isFinite(num) || num < 1 || channels.length === 0) return;
        const target = channels[num - 1];
        if (target) {
            if (isGuideOpen()) closeGuide();
            switchChannel(target.id);
        }
    }

    // Modal Navigation & Public Global Functions
    function getModalFocusables() {
        return [
            document.getElementById('tvServerInput'),
            document.getElementById('tvUserInput'),
            document.getElementById('tvPassInput'),
            ...Array.from(document.querySelectorAll('#tvSettingsModal .tv-modal-btn'))
                .filter(el => el.style.display !== 'none')
        ].filter(Boolean);
    }

    function applyModalFocus() {
        const items = getModalFocusables();
        if (items.length === 0) return;
        modalFocusIdx = ((modalFocusIdx % items.length) + items.length) % items.length;
        items.forEach((el, i) => {
            if (el.classList.contains('tv-btn')) el.classList.toggle('tv-focused', i === modalFocusIdx);
        });
        items[modalFocusIdx].focus();
    }

    function updateSettingsSessionBox() {
        const box = document.getElementById('tvSessionStatus');
        const logoutBtn = document.getElementById('tvLogoutBtn');
        if (box) {
            if (currentUser && currentUser.user_id) {
                const tierLabel = currentUser.tier === 'admin'
                    ? 'YÖNETİCİ (ADMIN)'
                    : (currentUser.tier === 'paid' ? 'ÜCRETLİ ÜYE (PAID)' : 'ÜCRETSİZ ÜYE');
                box.innerText = `Aktif Oturum: ${currentUser.user_id} — ${tierLabel}`;
                box.style.color = '#4ade80';
            } else {
                box.innerText = 'Aktif Oturum: Misafir (Ücretsiz Kanallar)';
                box.style.color = 'var(--text-sub)';
            }
        }
        if (logoutBtn) {
            logoutBtn.style.display = (currentUser && currentUser.user_id) ? 'inline-block' : 'none';
        }
    }

    window.reloadTvApp = () => {
        hideCenterMessage();
        loadP2PScript().then(() => loadSession()).then(() => loadChannels());
        loadEpg();
    };

    window.nextChannelFromMsg = () => {
        hideCenterMessage();
        if (channels.length === 0) {
            openGuide();
            return;
        }
        const curIdx = channels.findIndex(c => c.id === currentChannelId);
        for (let step = 1; step <= channels.length; step++) {
            const candidate = channels[(Math.max(0, curIdx) + step) % channels.length];
            if (candidate && candidate.id !== currentChannelId && canCurrentUserPlay(candidate)) {
                switchChannel(candidate.id);
                return;
            }
        }
        zapRelative(1);
    };

    window.openGuideFromMsg = () => {
        hideCenterMessage();
        openGuide();
    };

    window.openTvSettings = (notice) => {
        hideOsd();
        updateSettingsSessionBox();
        document.getElementById('tvServerInput').value = serverBase;
        document.getElementById('tvUserInput').value = (currentUser && currentUser.user_id) ? currentUser.user_id : '';
        document.getElementById('tvPassInput').value = '';
        const msgEl = document.getElementById('tvSettingsMsg');
        msgEl.style.color = 'var(--tv-gold)';
        msgEl.innerText = notice || '';
        settingsModal.style.display = 'flex';
        modalFocusIdx = (currentUser && currentUser.user_id) ? 3 : 1;
        applyModalFocus();
    };

    window.closeTvSettings = () => {
        settingsModal.style.display = 'none';
        showOsd(false);
    };

    window.toggleTvMute = () => {
        video.muted = !video.muted;
        userExplicitMute = video.muted;
        localStorage.setItem('iptv_tv_muted', video.muted ? '1' : '0');
        const msgEl = document.getElementById('tvSettingsMsg');
        msgEl.style.color = 'var(--tv-gold)';
        msgEl.innerText = video.muted ? 'Ses kapatıldı.' : 'Ses açıldı.';
    };

    window.logoutTvAccount = async () => {
        try {
            await fetch(apiUrl('/api/auth/logout'), { method: 'POST', credentials: 'include', headers: authHeaders() });
        } catch (_) {}
        localStorage.removeItem('iptv_admin_token');
        applyUserSession(null, '');
        updateSettingsSessionBox();
        const msgEl = document.getElementById('tvSettingsMsg');
        msgEl.style.color = 'var(--tv-gold)';
        msgEl.innerText = 'Oturum kapatıldı.';
        document.getElementById('tvUserInput').value = '';
        document.getElementById('tvPassInput').value = '';
    };

    window.saveTvSettings = async () => {
        const msgEl = document.getElementById('tvSettingsMsg');
        const nextUrl = document.getElementById('tvServerInput').value.trim().replace(/\/+$/, '');
        const user = document.getElementById('tvUserInput').value.trim();
        const pass = document.getElementById('tvPassInput').value;

        if ((user && !pass && !(currentUser && currentUser.user_id === user)) || (!user && pass)) {
            msgEl.style.color = '#fca5a5';
            msgEl.innerText = 'Giriş yapmak için hem kullanıcı adı hem şifre girin.';
            return;
        }

        if (nextUrl) {
            serverBase = nextUrl;
            localStorage.setItem('iptv_tv_server_url', nextUrl);
        }

        if (user && pass) {
            msgEl.style.color = 'var(--tv-gold)';
            msgEl.innerText = 'Giriş yapılıyor...';
            try {
                const res = await fetch(apiUrl('/api/auth/login'), {
                    method: 'POST',
                    credentials: 'include',
                    headers: authHeaders({ 'Content-Type': 'application/json' }),
                    body: JSON.stringify({ username: user, password: pass })
                });
                const data = await res.json();
                if (!res.ok) {
                    msgEl.style.color = '#fca5a5';
                    msgEl.innerText = data.error || 'Kullanıcı adı veya şifre hatalı.';
                    return;
                }
                applyUserSession(data.user, data.token);
                updateSettingsSessionBox();
                msgEl.style.color = '#4ade80';
                msgEl.innerText = 'Giriş başarılı! Kanallar yükleniyor...';
            } catch (_) {
                msgEl.style.color = '#fca5a5';
                msgEl.innerText = 'Sunucuya bağlanılamadı.';
                return;
            }
        }

        window.closeTvSettings();
        await loadP2PScript();
        await loadSession();
        await loadChannels();
        loadEpg();
    };

    // Wire Category Clicks & OSD Action Clicks
    getCategoryButtons().forEach((btn, idx) => {
        btn.onclick = () => {
            activeCategory = btn.dataset.cat || 'all';
            guideCatIdx = idx;
            guideCol = 'list';
            guideChIdx = 0;
            renderGuideList();
        };
    });

    getOsdButtons().forEach((btn, idx) => {
        btn.onclick = () => {
            osdFocusIdx = idx;
            applyOsdFocus();
            executeOsdAction(btn.dataset.action);
        };
    });

    // D-Pad & Smart TV Remote Keyboard Navigation
    document.addEventListener('keydown', (e) => {
        tryAutoUnmute();

        const key = e.key;
        const activeTag = (document.activeElement && document.activeElement.tagName) || '';

        // 1. Settings Modal Active
        if (isModalOpen()) {
            if (key === 'Escape' || key === 'Backspace' && activeTag !== 'INPUT' || key === 'BrowserBack') {
                e.preventDefault();
                window.closeTvSettings();
                return;
            }
            const items = getModalFocusables();
            if (key === 'ArrowDown') {
                e.preventDefault();
                modalFocusIdx = (modalFocusIdx + 1) % items.length;
                applyModalFocus();
            } else if (key === 'ArrowUp') {
                e.preventDefault();
                modalFocusIdx = (modalFocusIdx - 1 + items.length) % items.length;
                applyModalFocus();
            } else if (key === 'ArrowRight' && activeTag !== 'INPUT') {
                e.preventDefault();
                modalFocusIdx = (modalFocusIdx + 1) % items.length;
                applyModalFocus();
            } else if (key === 'ArrowLeft' && activeTag !== 'INPUT') {
                e.preventDefault();
                modalFocusIdx = (modalFocusIdx - 1 + items.length) % items.length;
                applyModalFocus();
            } else if (key === 'Enter' && activeTag === 'INPUT') {
                e.preventDefault();
                if (document.activeElement && document.activeElement.id === 'tvPassInput') {
                    window.saveTvSettings();
                } else if (modalFocusIdx < items.length - 1) {
                    modalFocusIdx++;
                    applyModalFocus();
                } else {
                    window.saveTvSettings();
                }
            }
            return;
        }

        // Numeric Zap (0-9) works anytime outside modal input
        if (/^[0-9]$/.test(key)) {
            e.preventDefault();
            handleNumericZapDigit(key);
            return;
        }
        if (key === 'Enter' && zapBuffer.length > 0) {
            e.preventDefault();
            commitNumericZap();
            return;
        }

        // 2. Guide Drawer Active
        if (isGuideOpen()) {
            if (key === 'Escape' || key === 'Backspace' || key === 'BrowserBack' || key === 'g' || key === 'G') {
                e.preventDefault();
                closeGuide();
                return;
            }
            const cats = getCategoryButtons();
            if (guideCol === 'cats') {
                if (key === 'ArrowDown') {
                    e.preventDefault();
                    guideCatIdx = (guideCatIdx + 1) % cats.length;
                    applyGuideFocusScroll();
                } else if (key === 'ArrowUp') {
                    e.preventDefault();
                    guideCatIdx = (guideCatIdx - 1 + cats.length) % cats.length;
                    applyGuideFocusScroll();
                } else if (key === 'ArrowRight') {
                    e.preventDefault();
                    activeCategory = cats[guideCatIdx].dataset.cat || 'all';
                    guideChIdx = 0;
                    renderGuideList();
                    if (guideList.length > 0) {
                        guideCol = 'list';
                        applyGuideFocusScroll();
                    }
                } else if (key === 'Enter') {
                    e.preventDefault();
                    activeCategory = cats[guideCatIdx].dataset.cat || 'all';
                    guideChIdx = 0;
                    renderGuideList();
                }
            } else {
                if (key === 'ArrowDown') {
                    e.preventDefault();
                    if (guideList.length > 0) {
                        guideChIdx = (guideChIdx + 1) % guideList.length;
                        applyGuideFocusScroll();
                    }
                } else if (key === 'ArrowUp') {
                    e.preventDefault();
                    if (guideList.length > 0) {
                        guideChIdx = (guideChIdx - 1 + guideList.length) % guideList.length;
                        applyGuideFocusScroll();
                    }
                } else if (key === 'ArrowLeft') {
                    e.preventDefault();
                    guideCol = 'cats';
                    applyGuideFocusScroll();
                } else if (key === 'Enter') {
                    e.preventDefault();
                    const ch = guideList[guideChIdx];
                    if (ch) {
                        closeGuide();
                        switchChannel(ch.id);
                    }
                }
            }
            return;
        }

        // 3. Center Error/Auth Overlay Active
        if (isCenterMsgOpen()) {
            if (key === 'g' || key === 'G') {
                e.preventDefault();
                window.openGuideFromMsg();
                return;
            }
            if (key === 'ArrowLeft' || key === 'ArrowRight') {
                const btns = Array.from(centerMsg.querySelectorAll('.tv-btn')).filter(b => b.style.display !== 'none');
                if (btns.length > 1) {
                    e.preventDefault();
                    const curIdx = btns.indexOf(document.activeElement);
                    const delta = key === 'ArrowRight' ? 1 : -1;
                    const nextIdx = curIdx < 0 ? 0 : (curIdx + delta + btns.length) % btns.length;
                    btns[nextIdx].focus();
                }
            } else if (key === 'ArrowUp' || key === 'ChannelUp' || key === 'PageUp') {
                e.preventDefault();
                zapRelative(-1);
            } else if (key === 'ArrowDown' || key === 'ChannelDown' || key === 'PageDown') {
                e.preventDefault();
                zapRelative(1);
            }
            return;
        }

        // 4. Fullscreen Video / Broadcast OSD State
        if (key === 'ChannelUp' || key === 'PageUp') {
            e.preventDefault();
            zapRelative(1);
            return;
        }
        if (key === 'ChannelDown' || key === 'PageDown') {
            e.preventDefault();
            zapRelative(-1);
            return;
        }
        if (key === 'MediaPlayPause' || key === ' ') {
            e.preventDefault();
            togglePlayPause();
            return;
        }
        if (key === 'MediaRewind') {
            e.preventDefault();
            seekRelative(-300);
            return;
        }
        if (key === 'MediaFastForward') {
            e.preventDefault();
            seekRelative(300);
            return;
        }
        if (key === 'm' || key === 'M') {
            e.preventDefault();
            window.toggleTvMute();
            showOsd(false);
            return;
        }
        if (key === 'g' || key === 'G') {
            e.preventDefault();
            openGuide();
            return;
        }

        const osdVisible = document.body.classList.contains('osd-visible');

        if (!osdVisible) {
            if (key === 'Enter') {
                e.preventDefault();
                showOsd(false);
            } else if (key === 'ArrowUp') {
                e.preventDefault();
                zapRelative(-1);
            } else if (key === 'ArrowDown') {
                e.preventDefault();
                zapRelative(1);
            } else if (key === 'ArrowLeft') {
                e.preventDefault();
                seekRelative(-60);
            } else if (key === 'ArrowRight') {
                e.preventDefault();
                seekRelative(60);
            }
            return;
        }

        // OSD is visible: D-Pad navigates OSD action buttons or zaps
        if (key === 'Escape' || key === 'Backspace' || key === 'BrowserBack') {
            e.preventDefault();
            hideOsd();
        } else if (key === 'ArrowRight') {
            e.preventDefault();
            osdFocusIdx++;
            showOsd(false);
        } else if (key === 'ArrowLeft') {
            e.preventDefault();
            osdFocusIdx--;
            showOsd(false);
        } else if (key === 'ArrowUp') {
            e.preventDefault();
            zapRelative(-1);
        } else if (key === 'ArrowDown') {
            e.preventDefault();
            zapRelative(1);
        } else if (key === 'Enter') {
            e.preventDefault();
            const btns = getOsdButtons();
            const activeBtn = btns[osdFocusIdx];
            if (activeBtn) executeOsdAction(activeBtn.dataset.action);
        }
    });

    // Mouse / Touch support on fullscreen stage wakes OSD
    document.querySelector('.tv-stage').addEventListener('click', () => {
        tryAutoUnmute();
        if (document.body.classList.contains('osd-visible')) hideOsd();
        else showOsd(false);
    });

    document.addEventListener('mousemove', () => {
        if (!isGuideOpen() && !isModalOpen() && !isCenterMsgOpen()) {
            showOsd(false);
        }
    });

    video.addEventListener('play', () => {
        if (p2pEngine && !p2pEngine.destroyed) p2pEngine.stopPausedDvrPrefetch();
        updateOsdInfo();
    });
    video.addEventListener('pause', () => {
        if (!video.ended && p2pEngine && !p2pEngine.destroyed) p2pEngine.startPausedDvrPrefetch();
        updateOsdInfo();
    });
    video.addEventListener('seeked', updateOsdInfo);

    window.addEventListener('beforeunload', () => {
        if (currentChannelId) sendHeartbeat(currentChannelId, 'leave');
    });

    // Boot
    (async () => {
        updateTopClock();
        setInterval(updateTopClock, 10000);
        osdTickTimer = setInterval(() => {
            if (document.body.classList.contains('osd-visible')) {
                updateOsdInfo();
            }
        }, 1500);

        await loadP2PScript();
        await loadSession();
        await loadChannels();
        loadEpg();
        setInterval(loadEpg, 30000);
        setInterval(refreshViewers, 8000);
    })();
})();
