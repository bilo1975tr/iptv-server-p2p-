(function () {
    'use strict';

    const DEFAULT_SERVER = window.location.protocol === 'file:' ? 'http://localhost:8080' : window.location.origin;
    let serverBase = (localStorage.getItem('iptv_pc_server_url') || DEFAULT_SERVER).replace(/\/+$/, '');
    const viewerTabId = 'pc_' + Math.random().toString(36).slice(2, 10);

    const urlParams = new URLSearchParams(window.location.search);
    let currentChannelId = urlParams.get('channel_id') || localStorage.getItem('iptv_pc_last_ch') || '';
    let token = urlParams.get('token') || '';
    let currentUser = null;
    let userFavorites = new Set();

    let channels = [];
    let filteredList = [];
    let activeFilter = 'all';
    let epgData = {};
    let authMode = 'login';

    let hlsInstance = null;
    let msePollTimer = null;
    let activeMediaSource = null;
    let activeBlobUrl = null;
    let p2pEngine = null;
    let heartbeatTimer = null;
    let liveLagTimer = null;
    let toastTimer = null;

    const video = document.getElementById('videoPlayer');

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

    function showToast(msg) {
        const el = document.getElementById('toastMsg');
        if (!el) return;
        el.innerText = msg;
        el.style.display = 'block';
        if (toastTimer) clearTimeout(toastTimer);
        toastTimer = setTimeout(() => { el.style.display = 'none'; }, 2400);
    }

    function formatBytes(bytes) {
        if (!bytes || bytes < 1024) return `${Math.round(bytes || 0)} B`;
        if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
        return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
    }

    function formatLag(sec) {
        const s = Math.max(0, Math.floor(sec));
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}:${String(r).padStart(2, '0')}`;
    }

    function formatClockHHMM(unixSec) {
        const d = new Date(unixSec * 1000);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
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
            return { current: null, next: null, index: -1 };
        }
        for (let i = 0; i < schedule.length; i++) {
            const p = schedule[i];
            const st = Number(p.start_unix || 0);
            const sp = Number(p.stop_unix || 0);
            if (st > 0 && sp > 0 && targetUnix >= st && targetUnix < sp) {
                return { current: p, next: schedule[i + 1] || null, index: i };
            }
        }
        for (let i = 0; i < schedule.length; i++) {
            const p = schedule[i];
            const st = Number(p.start_unix || 0);
            if (st > targetUnix) {
                return { current: null, next: p, index: -1 };
            }
        }
        return { current: null, next: null, index: -1 };
    }

    function showOverlay(title, body, showAuth) {
        document.getElementById('overlayHeading').innerText = title;
        document.getElementById('overlayBody').innerText = body;
        document.getElementById('overlayActionBtn').style.display = showAuth ? 'inline-block' : 'none';
        document.getElementById('stageOverlay').style.display = 'flex';
    }

    function hideOverlay() {
        document.getElementById('stageOverlay').style.display = 'none';
    }

    function updateTvModeLink(chId) {
        const link = document.getElementById('tvModeNavLink');
        if (!link) return;
        link.href = chId ? `/tv/?channel_id=${encodeURIComponent(chId)}` : '/tv/';
    }

    function markChannelWorking(chId) {
        if (!chId) return;
        localStorage.setItem('iptv_pc_last_ch', chId);
        updateTvModeLink(chId);
    }

    function clearBadLastChannel(chId) {
        if (chId && localStorage.getItem('iptv_pc_last_ch') === chId) {
            localStorage.removeItem('iptv_pc_last_ch');
        }
    }

    function applyUserSession(user, newToken) {
        if (user && newToken) {
            token = newToken;
        } else if (!user) {
            token = '';
        }
        currentUser = user || null;
        userFavorites = new Set((user && Array.isArray(user.favorites)) ? user.favorites : []);
        const isAdmin = !!(currentUser && currentUser.tier === 'admin');
        if (isAdmin && token) {
            localStorage.setItem('iptv_admin_token', token);
        }
        const adminNav = document.getElementById('adminPanelLink');
        if (adminNav) adminNav.style.display = isAdmin ? 'inline-block' : 'none';
        const profAdminBtn = document.getElementById('profileAdminBtn');
        if (profAdminBtn) profAdminBtn.style.display = isAdmin ? 'block' : 'none';

        const topBtn = document.getElementById('authTopBtn');
        const favBtn = document.getElementById('favTabBtn');
        if (currentUser && currentUser.user_id) {
            topBtn.innerText = `${currentUser.user_id} (${(currentUser.tier || 'free').toUpperCase()})`;
            favBtn.innerText = `Favori (${userFavorites.size})`;
        } else {
            topBtn.innerText = 'Giriş Yap';
            favBtn.innerText = 'Favori';
            if (activeFilter === 'favorites') {
                activeFilter = 'all';
                document.querySelectorAll('#filterTabs .seg-btn').forEach(b => b.classList.toggle('active', b.dataset.filter === 'all'));
            }
        }
        renderChannels();
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
                localStorage.removeItem('iptv_admin_token');
            }
        } catch (_) {}
        applyUserSession(null, '');
    }

    async function loadEpg() {
        try {
            const res = await fetch(apiUrl('/api/epg/json'));
            if (res.ok) {
                epgData = await res.json();
                renderChannels();
                updateEpgPanel(true);
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
            if (channels.length > 0) {
                if (!currentChannelId || !channels.some(c => c.id === currentChannelId)) {
                    const playable = channels.find(canCurrentUserPlay);
                    currentChannelId = (playable || channels[0]).id;
                }
                renderChannels();
                playChannel(currentChannelId);
            } else {
                document.getElementById('channelList').innerHTML = '<div style="padding:20px; text-align:center; color:var(--text-secondary);">Kayıtlı kanal bulunamadı.</div>';
                showOverlay('Kanal Bulunamadı', 'Admin panelinden M3U listesi veya kanal ekleyin.', false);
            }
        } catch (err) {
            document.getElementById('channelList').innerHTML = '<div style="padding:20px; text-align:center; color:#fca5a5;">Sunucuya bağlanılamadı.</div>';
            showOverlay('Sunucu Bağlantı Hatası', `Sunucuya ulaşılamadı (${serverBase}). Ayarlardan sunucu adresini kontrol edin.`, false);
        }
    }

    async function refreshViewers() {
        try {
            const res = await fetch(apiUrl('/api/channels/viewers'));
            if (!res.ok) return;
            const map = await res.json();
            let changed = false;
            channels.forEach(c => {
                const next = Number(map[c.id] || 0);
                if ((c.viewer_count || 0) !== next) {
                    c.viewer_count = next;
                    changed = true;
                }
            });
            const cur = channels.find(c => c.id === currentChannelId);
            if (cur) {
                document.getElementById('metaViewers').innerText = `${cur.viewer_count || 0} izleyici`;
            }
            if (changed) renderChannels();
        } catch (_) {}
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

    function setFilter(f) {
        if (f === 'favorites' && (!currentUser || !currentUser.user_id)) {
            openAuthModal('login');
            showToast('Favoriler için giriş yapın');
            return;
        }
        activeFilter = f;
        document.querySelectorAll('#filterTabs .seg-btn').forEach(b => b.classList.toggle('active', b.dataset.filter === f));
        renderChannels();
    }

    function getFilteredSortedChannels() {
        const q = (document.getElementById('searchInput').value || '').trim().toLowerCase();
        const sort = document.getElementById('sortSelect').value;

        const out = channels.filter(ch => {
            if (activeFilter === 'favorites' && !userFavorites.has(ch.id)) return false;
            if (activeFilter === 'free' && ch.tier === 'paid') return false;
            if (activeFilter === 'paid' && ch.tier !== 'paid') return false;
            if (activeFilter === 'acestream' && !ch.is_acestream) return false;
            if (!q) return true;
            const nameHit = (ch.name || '').toLowerCase().includes(q);
            const idHit = (ch.id || '').toLowerCase().includes(q);
            let progHit = false;
            const g = epgData[ch.id];
            if (g) {
                const match = findProgramAtUnix(g.schedule, Math.floor(Date.now() / 1000));
                const curr = match.current || g.current;
                if (curr && curr.title) progHit = curr.title.toLowerCase().includes(q);
            }
            return nameHit || idHit || progHit;
        });

        out.sort((a, b) => {
            const va = a.viewer_count || 0;
            const vb = b.viewer_count || 0;
            switch (sort) {
                case 'name_asc': return a.name.localeCompare(b.name, 'tr');
                case 'name_desc': return b.name.localeCompare(a.name, 'tr');
                case 'fav_first': {
                    const fa = userFavorites.has(a.id) ? 1 : 0;
                    const fb = userFavorites.has(b.id) ? 1 : 0;
                    return fb !== fa ? fb - fa : vb - va;
                }
                case 'free_first': return (a.tier === 'paid' ? 1 : 0) - (b.tier === 'paid' ? 1 : 0) || a.name.localeCompare(b.name, 'tr');
                case 'paid_first': return (a.tier === 'paid' ? 0 : 1) - (b.tier === 'paid' ? 0 : 1) || a.name.localeCompare(b.name, 'tr');
                case 'viewers_desc':
                default:
                    return vb !== va ? vb - va : a.name.localeCompare(b.name, 'tr');
            }
        });
        return out;
    }

    function renderChannels() {
        filteredList = getFilteredSortedChannels();
        const container = document.getElementById('channelList');
        container.innerHTML = '';

        if (filteredList.length === 0) {
            container.innerHTML = '<div style="padding:20px; text-align:center; color:var(--text-secondary);">Eşleşen kanal yok.</div>';
            return;
        }

        const nowUnix = Math.floor(Date.now() / 1000);
        const loggedIn = !!(currentUser && currentUser.user_id);

        filteredList.forEach((ch, idx) => {
            const row = document.createElement('div');
            row.className = 'ch-row' + (ch.id === currentChannelId ? ' active' : '');
            row.onclick = () => switchChannel(ch.id);

            const avatar = document.createElement('div');
            avatar.className = 'ch-avatar';
            const initials = (ch.name || '?').slice(0, 2).toUpperCase();
            if (ch.logo_url) {
                const img = document.createElement('img');
                img.src = ch.logo_url;
                img.alt = ch.name;
                img.referrerPolicy = 'no-referrer';
                img.onerror = () => { avatar.textContent = initials; };
                avatar.appendChild(img);
            } else {
                avatar.textContent = initials;
            }

            let currTitle = '';
            const g = epgData[ch.id];
            if (g) {
                const m = findProgramAtUnix(g.schedule, nowUnix);
                const c = m.current || g.current;
                if (c && c.title) currTitle = c.title;
            }

            const tierLabel = ch.tier === 'paid' ? 'Ücretli' : 'Ücretsiz';
            const metaText = currTitle
                ? `${currTitle} · ${ch.viewer_count || 0} izleyici`
                : `${String(idx + 1).padStart(2, '0')} · ${tierLabel} · ${ch.viewer_count || 0} izleyici`;

            const body = document.createElement('div');
            body.className = 'ch-body';
            body.innerHTML = `<div class="ch-title">${escapeHtml(ch.name)}</div><div class="ch-meta">${escapeHtml(metaText)}</div>`;

            const favBtn = document.createElement('button');
            favBtn.type = 'button';
            favBtn.className = 'fav-btn' + (loggedIn && userFavorites.has(ch.id) ? ' starred' : '');
            favBtn.title = loggedIn ? 'Favori' : 'Favoriler için giriş yapın';
            favBtn.innerText = loggedIn && userFavorites.has(ch.id) ? '★' : '☆';
            favBtn.onclick = (e) => toggleFavorite(ch.id, e);

            row.appendChild(avatar);
            row.appendChild(body);
            row.appendChild(favBtn);
            container.appendChild(row);
        });
    }

    function switchChannel(chId) {
        if (!chId || chId === currentChannelId) return;
        sendHeartbeat(currentChannelId, 'leave');
        currentChannelId = chId;
        updateTvModeLink(chId);
        renderChannels();
        playChannel(chId);
    }

    function updateEpgPanel(rebuildSchedule) {
        const guide = epgData[currentChannelId];
        const lagSec = getDvrLagSeconds();
        const isRewound = lagSec > 15;
        const targetUnix = Math.floor(Date.now() / 1000 - (isRewound ? lagSec : 0));

        document.getElementById('epgBoxHeading').innerText = isRewound ? 'DVR Geçmiş Rehberi' : 'Canlı TV Rehberi';
        document.getElementById('epgClockLabel').innerText = formatClockHHMM(targetUnix);

        let currProg = null;
        let nextProg = null;
        let activeIdx = -1;

        if (guide) {
            const matched = findProgramAtUnix(guide.schedule, targetUnix);
            activeIdx = matched.index;
            if (isRewound) {
                currProg = matched.current;
                nextProg = matched.next;
            } else {
                currProg = matched.current || guide.current || null;
                nextProg = matched.next || guide.next || null;
            }
        }

        if (currProg) {
            document.getElementById('epgCurrTime').innerText = `${currProg.start} - ${currProg.stop}`;
            document.getElementById('epgCurrTitle').innerText = currProg.title;
            document.getElementById('epgCurrDesc').innerText = currProg.description || 'Açıklama bulunmuyor.';
        } else if (isRewound && guide) {
            document.getElementById('epgCurrTime').innerText = formatClockHHMM(targetUnix);
            document.getElementById('epgCurrTitle').innerText = 'Bu saat için geçmiş EPG kaydı yok';
            document.getElementById('epgCurrDesc').innerText = '';
        } else {
            document.getElementById('epgCurrTime').innerText = '--:-- - --:--';
            document.getElementById('epgCurrTitle').innerText = 'Yayın akışı bulunamadı';
            document.getElementById('epgCurrDesc').innerText = 'Admin panelinden EPG eşlemesi yapabilirsiniz.';
        }

        document.getElementById('epgNextRow').innerText = nextProg
            ? `Sonraki (${nextProg.start}): ${nextProg.title}`
            : 'Sonraki: Bilgi yok';

        if (rebuildSchedule) {
            const listEl = document.getElementById('epgScheduleList');
            listEl.innerHTML = '';
            if (!guide || !Array.isArray(guide.schedule) || guide.schedule.length === 0) {
                listEl.innerHTML = '<div style="padding:12px; color:var(--text-secondary); font-size:0.8rem;">Bu kanal için yayın akışı listesi yok.</div>';
                return;
            }
            const nowSec = Math.floor(Date.now() / 1000);
            guide.schedule.forEach((item, idx) => {
                const row = document.createElement('div');
                row.className = 'sched-item' + (idx === activeIdx ? ' is-current' : '');
                row.dataset.startUnix = String(item.start_unix || 0);
                row.innerHTML = `
                    <div class="sched-time tabular">${escapeHtml(item.start)} - ${escapeHtml(item.stop)}</div>
                    <div class="sched-title">${escapeHtml(item.title)}</div>
                `;
                row.onclick = () => {
                    const st = Number(item.start_unix || 0);
                    if (!st || st > nowSec) {
                        showToast('İlerideki program henüz başlamadı');
                        return;
                    }
                    seekToUnixTimestamp(st);
                };
                listEl.appendChild(row);
            });
        }
    }

    function seekToUnixTimestamp(targetUnix) {
        try {
            if (!video.seekable || video.seekable.length === 0) {
                showToast('Bu kanal için DVR zaman çizelgesi henüz hazır değil');
                return;
            }
            const startRange = video.seekable.start(0);
            const endRange = video.seekable.end(video.seekable.length - 1);
            const nowSec = Math.floor(Date.now() / 1000);
            const desiredLag = Math.max(0, nowSec - targetUnix);
            const desiredTime = endRange - desiredLag;
            if (desiredTime < startRange) {
                showToast('Bu program oynatıcının mevcut DVR tamponundan daha eski');
                return;
            }
            video.currentTime = Math.min(endRange - 2, Math.max(startRange, desiredTime));
            video.play().catch(() => {});
            updateEpgPanel(false);
        } catch (_) {}
    }

    function stopPlayback() {
        if (liveLagTimer) {
            clearInterval(liveLagTimer);
            liveLagTimer = null;
        }
        document.getElementById('liveReturnBtn').style.display = 'none';
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
                document.getElementById('statOffload').innerText = `%${s.offloadPct} (${formatBytes(s.p2pBytesIn)})`;
                document.getElementById('statPeers').innerText = `${s.totalSwarmPeers} Peer (${s.openP2PPeers} WebRTC)`;
                if (s.dvr) {
                    const sec = s.dvr.timelineSec || 0;
                    const durText = sec > 3900 ? `${(sec / 3600).toFixed(1)} sa` : `${(sec / 60).toFixed(1)} dk`;
                    document.getElementById('statDvr').innerText = `${durText} (${formatBytes(s.dvr.bytes || 0)})`;
                }
            }
        });
    }

    function startLagMonitor() {
        if (liveLagTimer) clearInterval(liveLagTimer);
        const btn = document.getElementById('liveReturnBtn');
        liveLagTimer = setInterval(() => {
            updateEpgPanel(false);
            const lag = getDvrLagSeconds();
            if (lag > 15) {
                btn.style.display = 'inline-flex';
                btn.style.alignItems = 'center';
                btn.innerText = `-${formatLag(lag)} · Canlıya Dön`;
            } else {
                btn.style.display = 'none';
            }
        }, 1500);
    }

    async function playChannel(chId) {
        const ch = channels.find(c => c.id === chId);
        if (!ch) return;

        hideOverlay();
        stopPlayback();
        updateTvModeLink(chId);

        document.getElementById('activeChTitle').innerText = ch.name;
        document.getElementById('metaTier').innerText = ch.tier === 'paid' ? 'Ücretli' : 'Ücretsiz';
        document.getElementById('metaViewers').innerText = `${ch.viewer_count || 0} izleyici`;
        document.getElementById('metaTransport').innerText = ch.is_acestream ? 'AceStream HLS + P2P DVR' : 'HLS + P2P DVR';

        sendHeartbeat(chId, 'join');
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        heartbeatTimer = setInterval(() => sendHeartbeat(currentChannelId, 'ping'), 12000);

        updateEpgPanel(true);
        connectP2P(chId);

        if (ch.is_acestream) {
            showOverlay('AceStream P2P Bağlanıyor', 'Motor yayını tarayıcı formatına hazırlıyor...', false);
        }

        const proxyUrl = apiUrl(`/api/stream/${encodeURIComponent(chId)}/master.m3u8?token=${encodeURIComponent(token)}&vid=${encodeURIComponent(viewerTabId)}`);

        try {
            const t0 = performance.now();
            const headRes = await fetch(proxyUrl);
            const rtt = Math.round(performance.now() - t0);
            document.getElementById('statLatency').innerText = `H.264/HEVC · ${rtt} ms`;

            if (headRes.status === 403) {
                showOverlay('Abonelik Gerekli', 'Bu ücretli kanalı izlemek için hesabınızla giriş yapın.', true);
                return;
            }

            const ct = (headRes.headers.get('content-type') || '').toLowerCase();
            if (headRes.ok && (ct.includes('mpegurl') || ct.includes('text'))) {
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
                    startLagMonitor();

                    hlsInstance.on(Hls.Events.MANIFEST_PARSED, () => {
                        hideOverlay();
                        markChannelWorking(chId);
                        video.play().catch(() => {});
                    });
                    hlsInstance.on(Hls.Events.FRAG_LOADED, (_, data) => {
                        hideOverlay();
                        markChannelWorking(chId);
                        netRetries = 0;
                        if (data && data.frag && data.frag.stats && data.frag.stats.loading) {
                            const ms = Math.round(data.frag.stats.loading.end - data.frag.stats.loading.start);
                            if (ms >= 0) document.getElementById('statLatency').innerText = `HLS · ${ms} ms`;
                        }
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
                        startMSEFallback(chId, !!ch.is_acestream);
                    });
                    return;
                } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
                    video.src = proxyUrl;
                    hideOverlay();
                    markChannelWorking(chId);
                    startLagMonitor();
                    video.play().catch(() => {});
                    return;
                }
            }
        } catch (_) {}

        startMSEFallback(chId, !!ch.is_acestream);
    }

    function startMSEFallback(chId, isAce) {
        if (typeof MediaSource === 'undefined') {
            showOverlay('Oynatılamıyor', 'Tarayıcı MediaSource desteklemiyor.', false);
            return;
        }
        stopPlayback();
        document.getElementById('metaTransport').innerText = isAce ? 'AceStream fMP4' : 'FFmpeg fMP4';
        showOverlay(isAce ? 'AceStream Hazırlanıyor' : 'Yayın Başlatılıyor', 'Segment bekleniyor...', false);

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
                            showOverlay('Abonelik Gerekli', 'Ücretli kanal için giriş yapın.', true);
                            return;
                        }
                        if (r.status === 404 || r.status === 502 || r.status === 503) {
                            clearInterval(msePollTimer);
                            clearBadLastChannel(chId);
                            showOverlay('Yayın Kaynağı Çevrimdışı', 'Bu kanalın kaynak sunucusu yanıt vermiyor. Başka bir kanal seçebilirsiniz.', false);
                            return;
                        }
                        if (!r.ok || r.status === 202) {
                            if (elapsedSec > 45) {
                                clearInterval(msePollTimer);
                                clearBadLastChannel(chId);
                                showOverlay('Yayın Başlatılamadı', 'Kaynak akış zaman aşımına uğradı veya sunucu FFmpeg akışı başlatamadı.', false);
                            } else {
                                document.getElementById('overlayBody').innerText = `Segment bekleniyor... (${elapsedSec} sn)`;
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
                                showOverlay('Codec Hatası', `Tarayıcı ${codec} video kodeğini desteklemiyor.`, false);
                                return;
                            }
                            attachSBListeners(sb);
                        }
                        sb.appendBuffer(buf);
                        initLoaded = true;
                        hideOverlay();
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

    // Desktop Player Actions
    window.togglePlayPause = () => {
        if (video.paused) video.play().catch(() => {});
        else video.pause();
    };

    window.seekRelative = (deltaSec) => {
        try {
            if (!video.seekable || video.seekable.length === 0) return;
            const s = video.seekable.start(0);
            const e = video.seekable.end(video.seekable.length - 1);
            video.currentTime = Math.min(e - 1, Math.max(s, video.currentTime + deltaSec));
            updateEpgPanel(false);
        } catch (_) {}
    };

    window.jumpToLiveEdge = () => {
        try {
            if (video.seekable && video.seekable.length > 0) {
                const e = video.seekable.end(video.seekable.length - 1);
                if (Number.isFinite(e) && e > 0) video.currentTime = Math.max(0, e - 3);
            }
            video.play().catch(() => {});
            updateEpgPanel(false);
        } catch (_) {}
    };

    window.toggleMute = () => {
        video.muted = !video.muted;
        document.getElementById('muteBtn').innerText = video.muted ? 'Sessiz' : 'Ses Açık';
    };

    window.setVolume = (v) => {
        const val = Number(v);
        video.volume = val;
        if (val > 0 && video.muted) video.muted = false;
        localStorage.setItem('iptv_pc_vol', String(val));
        document.getElementById('muteBtn').innerText = video.muted ? 'Sessiz' : 'Ses Açık';
    };

    window.captureSnapshot = () => {
        try {
            if (!video.videoWidth || !video.videoHeight) {
                showToast('Kare yakalanacak aktif video yok');
                return;
            }
            const canvas = document.createElement('canvas');
            canvas.width = video.videoWidth;
            canvas.height = video.videoHeight;
            canvas.getContext('2d').drawImage(video, 0, 0);
            const url = canvas.toDataURL('image/png');
            const a = document.createElement('a');
            a.href = url;
            a.download = `iptv_${currentChannelId}_${Date.now()}.png`;
            a.click();
            showToast('Ekran görüntüsü PNG olarak indirildi');
        } catch (_) {
            showToast('Tarayıcı güvenliği nedeniyle kare yakalanamadı');
        }
    };

    window.togglePiP = async () => {
        try {
            if (document.pictureInPictureElement) {
                await document.exitPictureInPicture();
            } else if (document.pictureInPictureEnabled && video.readyState >= 2) {
                await video.requestPictureInPicture();
            }
        } catch (_) {}
    };

    window.toggleFullscreen = () => {
        const stage = document.querySelector('.video-stage');
        if (document.fullscreenElement) document.exitFullscreen().catch(() => {});
        else if (stage && stage.requestFullscreen) stage.requestFullscreen().catch(() => {});
    };

    window.toggleTheaterMode = () => {
        document.getElementById('workspaceRoot').classList.toggle('theater-mode');
    };

    window.copyStreamUrl = () => {
        if (!currentChannelId) return;
        const link = apiUrl(`/api/stream/${encodeURIComponent(currentChannelId)}/master.m3u8${token ? '?token=' + encodeURIComponent(token) : ''}`);
        navigator.clipboard.writeText(link).then(() => showToast('VLC / HLS yayın bağlantısı kopyalandı')).catch(() => {});
    };

    window.changeUploadProfile = (val) => {
        if (typeof window.getIPTVUploadQuota === 'function') {
            window.getIPTVUploadQuota().setLimitProfile(val);
        }
    };

    window.openSettingsModal = () => {
        document.getElementById('serverUrlInput').value = serverBase;
        document.getElementById('settingsModal').style.display = 'flex';
    };
    window.closeSettingsModal = () => {
        document.getElementById('settingsModal').style.display = 'none';
    };
    window.saveClientSettings = async () => {
        const nextUrl = document.getElementById('serverUrlInput').value.trim().replace(/\/+$/, '');
        if (nextUrl) {
            serverBase = nextUrl;
            localStorage.setItem('iptv_pc_server_url', nextUrl);
        }
        closeSettingsModal();
        await loadP2PScript();
        await loadSession();
        await loadChannels();
        loadEpg();
        showToast('Sunucu bağlantısı güncellendi');
    };

    // Auth Actions
    window.openAuthModal = (mode) => {
        if (currentUser && currentUser.user_id) {
            document.getElementById('authFormView').style.display = 'none';
            document.getElementById('authProfileView').style.display = 'block';
            document.getElementById('profileName').innerText = currentUser.user_id;
            document.getElementById('profileTier').innerText = currentUser.tier === 'admin'
                ? 'Yönetici (Admin) — Tam Erişim'
                : (currentUser.tier === 'paid' ? 'Ücretli (Paid) Abonelik' : 'Ücretsiz Üye');
            const profAdminBtn = document.getElementById('profileAdminBtn');
            if (profAdminBtn) profAdminBtn.style.display = currentUser.tier === 'admin' ? 'block' : 'none';
        } else {
            document.getElementById('authProfileView').style.display = 'none';
            document.getElementById('authFormView').style.display = 'block';
            switchAuthMode(mode || 'login');
        }
        document.getElementById('authModal').style.display = 'flex';
    };
    window.closeAuthModal = () => {
        document.getElementById('authModal').style.display = 'none';
    };
    window.switchAuthMode = (mode) => {
        authMode = mode;
        document.getElementById('tabLogin').classList.toggle('active', mode === 'login');
        document.getElementById('tabReg').classList.toggle('active', mode === 'register');
        document.getElementById('pcAuthSubmitBtn').innerText = mode === 'login' ? 'Giriş Yap' : 'Ücretsiz Hesap Oluştur';
        document.getElementById('pcAuthError').style.display = 'none';
    };

    document.getElementById('pcLoginForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const errEl = document.getElementById('pcAuthError');
        errEl.style.display = 'none';
        const username = document.getElementById('pcLoginUser').value.trim();
        const password = document.getElementById('pcLoginPass').value;
        const endpoint = authMode === 'login' ? '/api/auth/login' : '/api/auth/register';
        const payload = authMode === 'login' ? { username, password } : { username, password, plan: 'free' };
        try {
            const res = await fetch(apiUrl(endpoint), {
                method: 'POST',
                credentials: 'include',
                headers: authHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            if (!res.ok) {
                errEl.innerText = data.error || 'İşlem başarısız';
                errEl.style.display = 'block';
                return;
            }
            applyUserSession(data.user, data.token);
            closeAuthModal();
            if (currentChannelId) playChannel(currentChannelId);
        } catch (_) {
            errEl.innerText = 'Sunucuya bağlanılamadı.';
            errEl.style.display = 'block';
        }
    });

    window.logoutAccount = async () => {
        try {
            await fetch(apiUrl('/api/auth/logout'), { method: 'POST', credentials: 'include', headers: authHeaders() });
        } catch (_) {}
        token = '';
        localStorage.removeItem('iptv_admin_token');
        applyUserSession(null, '');
        closeAuthModal();
        if (currentChannelId) playChannel(currentChannelId);
    };

    async function toggleFavorite(chId, evt) {
        if (evt) evt.stopPropagation();
        if (!currentUser || !currentUser.user_id) {
            openAuthModal('login');
            showToast('Favoriler için giriş yapın');
            return;
        }
        try {
            const res = await fetch(apiUrl('/api/user/favorites/toggle'), {
                method: 'POST',
                credentials: 'include',
                headers: authHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({ channel_id: chId })
            });
            if (res.ok) {
                const d = await res.json();
                userFavorites = new Set(Array.isArray(d.favorites) ? d.favorites : []);
                applyUserSession(currentUser, '');
            }
        } catch (_) {}
    }

    // Video state bindings
    video.addEventListener('play', () => {
        document.getElementById('playPauseBtn').innerText = 'Duraklat';
        if (p2pEngine && !p2pEngine.destroyed) p2pEngine.stopPausedDvrPrefetch();
    });
    video.addEventListener('pause', () => {
        document.getElementById('playPauseBtn').innerText = 'Oynat';
        if (!video.ended && p2pEngine && !p2pEngine.destroyed) p2pEngine.startPausedDvrPrefetch();
    });
    video.addEventListener('seeked', () => updateEpgPanel(false));

    // Desktop keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        const tag = (e.target && e.target.tagName) || '';
        if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA') {
            if (e.key === 'Escape') e.target.blur();
            return;
        }
        if (e.key === '/') {
            e.preventDefault();
            document.getElementById('searchInput').focus();
            return;
        }
        switch (e.key.toLowerCase()) {
            case ' ':
            case 'k':
                e.preventDefault();
                window.togglePlayPause();
                break;
            case 'f':
                e.preventDefault();
                window.toggleFullscreen();
                break;
            case 'p':
                e.preventDefault();
                window.togglePiP();
                break;
            case 't':
                e.preventDefault();
                window.toggleTheaterMode();
                break;
            case 'm':
                e.preventDefault();
                window.toggleMute();
                break;
            case 'l':
                e.preventDefault();
                window.jumpToLiveEdge();
                break;
            case 'arrowleft':
                e.preventDefault();
                window.seekRelative(e.shiftKey ? -300 : -10);
                break;
            case 'arrowright':
                e.preventDefault();
                window.seekRelative(e.shiftKey ? 300 : 10);
                break;
            case 'arrowup': {
                e.preventDefault();
                if (filteredList.length > 0) {
                    const idx = filteredList.findIndex(c => c.id === currentChannelId);
                    const prev = filteredList[(idx - 1 + filteredList.length) % filteredList.length];
                    if (prev) switchChannel(prev.id);
                }
                break;
            }
            case 'arrowdown': {
                e.preventDefault();
                if (filteredList.length > 0) {
                    const idx = filteredList.findIndex(c => c.id === currentChannelId);
                    const next = filteredList[(idx + 1) % filteredList.length];
                    if (next) switchChannel(next.id);
                }
                break;
            }
            default:
                if (/^[1-9]$/.test(e.key)) {
                    const ch = filteredList[Number(e.key) - 1];
                    if (ch) switchChannel(ch.id);
                }
        }
    });

    document.getElementById('searchInput').addEventListener('input', renderChannels);
    window.addEventListener('beforeunload', () => {
        if (currentChannelId) sendHeartbeat(currentChannelId, 'leave');
    });

    // Expose public UI handlers
    window.setFilter = setFilter;
    window.renderChannels = renderChannels;

    // Boot
    (async () => {
        const savedVol = localStorage.getItem('iptv_pc_vol');
        if (savedVol !== null) {
            const v = Number(savedVol);
            if (Number.isFinite(v) && v >= 0 && v <= 1) {
                video.volume = v;
                document.getElementById('volRange').value = String(v);
            }
        }
        await loadP2PScript();
        await loadSession();
        await loadChannels();
        loadEpg();
        setInterval(loadEpg, 30000);
        setInterval(refreshViewers, 8000);
    })();
})();
