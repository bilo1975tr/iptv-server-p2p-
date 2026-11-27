(function (global) {
    'use strict';

    const FRAME_HEADER_BYTES = 12;
    const FRAME_PAYLOAD_SIZE = 16384; // 16 KB safe SCTP DataChannel frame
    const MAX_DC_BUFFERED = 4 * 1024 * 1024;  // 4 MB backpressure ceiling for high P2P throughput

    const DVR_DB_NAME = 'IPTV_P2P_DVR_V1';
    const DVR_STORE_NAME = 'segments';
    const MAX_DVR_AGE_MS = 3600 * 1000;           // 1 hour client-side DVR retention
    const MAX_DVR_BYTES = 1200 * 1024 * 1024;     // 1.2 GB client IndexedDB ceiling
    const MAX_DVR_TIMELINE_SEC = 3600;            // 60 minutes rolling playlist window

    async function computeSha256Hex(arrayBuffer) {
        try {
            if (global.crypto && global.crypto.subtle && typeof global.crypto.subtle.digest === 'function') {
                const digest = await global.crypto.subtle.digest('SHA-256', arrayBuffer);
                const bytes = new Uint8Array(digest);
                let hex = '';
                for (let i = 0; i < bytes.length; i++) {
                    hex += bytes[i].toString(16).padStart(2, '0');
                }
                return hex;
            }
        } catch (_) {}
        return '';
    }

    function normalizePeerTier(tier) {
        const t = String(tier || 'guest').toLowerCase();
        if (t === 'paid' || t === 'admin') return 'paid';
        if (t === 'free') return 'free';
        return 'guest';
    }

    function isTailscaleIP(addr) {
        const s = String(addr || '').replace(/^::ffff:/i, '').trim();
        return /^100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\./.test(s) || /^fd7a:115c:a1e0:/i.test(s);
    }

    /**
     * Global Client-Side IndexedDB DVR Storage:
     * Holds up to 1 hour (~1.2 GB) of .ts segments on the client device without bloating browser RAM,
     * allowing paused viewers to resume seamlessly AND share historical .ts chunks via WebRTC P2P.
     */
    class ClientDVRCache {
        constructor() {
            this.db = null;
            this.initPromise = null;
            this.index = new Map(); // key -> { key, channelId, ts, size, sha256 }
            this.totalBytes = 0;
            this._pruneRunning = false;
            this._lastPruneTs = 0;
        }

        init() {
            if (this.initPromise) return this.initPromise;
            this.initPromise = new Promise((resolve) => {
                if (typeof global.indexedDB === 'undefined') {
                    resolve(false);
                    return;
                }
                try {
                    const req = global.indexedDB.open(DVR_DB_NAME, 1);
                    req.onupgradeneeded = (evt) => {
                        const db = evt.target.result;
                        if (!db.objectStoreNames.contains(DVR_STORE_NAME)) {
                            const store = db.createObjectStore(DVR_STORE_NAME, { keyPath: 'key' });
                            store.createIndex('by_ts', 'ts', { unique: false });
                            store.createIndex('by_channel', 'channelId', { unique: false });
                        }
                    };
                    req.onsuccess = (evt) => {
                        this.db = evt.target.result;
                        this._loadIndexAndPrune().then(() => resolve(true));
                    };
                    req.onerror = () => resolve(false);
                } catch (_) {
                    resolve(false);
                }
            });
            return this.initPromise;
        }

        _loadIndexAndPrune() {
            return new Promise((resolve) => {
                if (!this.db) {
                    resolve();
                    return;
                }
                try {
                    const tx = this.db.transaction(DVR_STORE_NAME, 'readwrite');
                    const store = tx.objectStore(DVR_STORE_NAME);
                    const cutoff = Date.now() - MAX_DVR_AGE_MS;
                    const req = store.openCursor();
                    req.onsuccess = (e) => {
                        const cursor = e.target.result;
                        if (!cursor) {
                            this._enforceByteCap();
                            resolve();
                            return;
                        }
                        const rec = cursor.value;
                        if (!rec || !rec.ts || rec.ts < cutoff || !rec.buf) {
                            cursor.delete();
                        } else {
                            const sz = Number(rec.size || rec.buf.byteLength || 0);
                            this.index.set(rec.key, {
                                key: rec.key,
                                channelId: rec.channelId || '',
                                ts: rec.ts,
                                size: sz,
                                sha256: rec.sha256 || ''
                            });
                            this.totalBytes += sz;
                        }
                        cursor.continue();
                    };
                    req.onerror = () => resolve();
                } catch (_) {
                    resolve();
                }
            });
        }

        has(key) {
            const meta = this.index.get(key);
            if (!meta) return false;
            if (Date.now() - meta.ts > MAX_DVR_AGE_MS) return false;
            return true;
        }

        async put(key, channelId, arrayBuffer, sha256) {
            if (!key || !arrayBuffer || arrayBuffer.byteLength === 0) return;
            await this.init();
            const sz = arrayBuffer.byteLength;
            const now = Date.now();

            const prev = this.index.get(key);
            if (prev) {
                this.totalBytes = Math.max(0, this.totalBytes - prev.size);
                this.index.delete(key);
            }

            this.index.set(key, {
                key,
                channelId: channelId || '',
                ts: now,
                size: sz,
                sha256: sha256 || ''
            });
            this.totalBytes += sz;

            if (this.db) {
                try {
                    const tx = this.db.transaction(DVR_STORE_NAME, 'readwrite');
                    tx.objectStore(DVR_STORE_NAME).put({
                        key,
                        channelId: channelId || '',
                        ts: now,
                        size: sz,
                        sha256: sha256 || '',
                        buf: arrayBuffer
                    });
                } catch (_) {}
            }

            if (now - this._lastPruneTs > 15000 || this.totalBytes > MAX_DVR_BYTES) {
                this._lastPruneTs = now;
                this._enforceByteCap();
            }
        }

        async get(key) {
            if (!key) return null;
            await this.init();
            const meta = this.index.get(key);
            if (!meta || !this.db) return null;
            if (Date.now() - meta.ts > MAX_DVR_AGE_MS) {
                this._deleteKeys([key]);
                return null;
            }
            return new Promise((resolve) => {
                try {
                    const tx = this.db.transaction(DVR_STORE_NAME, 'readonly');
                    const req = tx.objectStore(DVR_STORE_NAME).get(key);
                    req.onsuccess = (e) => {
                        const rec = e.target.result;
                        if (rec && rec.buf && rec.buf.byteLength > 0) {
                            resolve({
                                buf: rec.buf,
                                sha256: rec.sha256 || '',
                                size: rec.size || rec.buf.byteLength
                            });
                        } else {
                            resolve(null);
                        }
                    };
                    req.onerror = () => resolve(null);
                } catch (_) {
                    resolve(null);
                }
            });
        }

        _deleteKeys(keys) {
            if (!keys || keys.length === 0) return;
            for (let i = 0; i < keys.length; i++) {
                const k = keys[i];
                const meta = this.index.get(k);
                if (meta) {
                    this.totalBytes = Math.max(0, this.totalBytes - meta.size);
                    this.index.delete(k);
                }
            }
            if (!this.db) return;
            try {
                const tx = this.db.transaction(DVR_STORE_NAME, 'readwrite');
                const store = tx.objectStore(DVR_STORE_NAME);
                for (let i = 0; i < keys.length; i++) {
                    store.delete(keys[i]);
                }
            } catch (_) {}
        }

        _enforceByteCap() {
            if (this._pruneRunning) return;
            this._pruneRunning = true;
            try {
                const now = Date.now();
                const cutoff = now - MAX_DVR_AGE_MS;
                const toDelete = [];
                for (const [k, meta] of this.index.entries()) {
                    if (meta.ts < cutoff) {
                        toDelete.push(k);
                    }
                }
                if (toDelete.length > 0) {
                    this._deleteKeys(toDelete);
                }
                if (this.totalBytes > MAX_DVR_BYTES) {
                    const overDelete = [];
                    let projected = this.totalBytes;
                    for (const [k, meta] of this.index.entries()) {
                        if (projected <= MAX_DVR_BYTES * 0.85) break;
                        overDelete.push(k);
                        projected -= meta.size;
                    }
                    if (overDelete.length > 0) {
                        this._deleteKeys(overDelete);
                    }
                }
            } finally {
                this._pruneRunning = false;
            }
        }

        getKeysForChannel(channelId) {
            const list = [];
            const cutoff = Date.now() - MAX_DVR_AGE_MS;
            for (const meta of this.index.values()) {
                if (meta.channelId === channelId && meta.ts >= cutoff) {
                    list.push({ k: meta.key, sz: meta.size, h: meta.sha256 || '' });
                }
            }
            return list;
        }

        getChannelStats(channelId) {
            let count = 0;
            let bytes = 0;
            const cutoff = Date.now() - MAX_DVR_AGE_MS;
            for (const meta of this.index.values()) {
                if (meta.channelId === channelId && meta.ts >= cutoff) {
                    count++;
                    bytes += meta.size;
                }
            }
            return { count, bytes };
        }
    }

    function getGlobalDVRCache() {
        if (!global.__iptvClientDVRCache) {
            global.__iptvClientDVRCache = new ClientDVRCache();
            global.__iptvClientDVRCache.init();
        }
        return global.__iptvClientDVRCache;
    }

    /**
     * Global browser-wide Upload Quota Coordinator:
     * Enforces high P2P upload quota: 35% for Free+Guest peers and 40% for Paid peers (75% total max).
     */
    class UploadQuotaCoordinator {
        constructor() {
            this.mode = 'auto'; // 'auto' or numeric Mbps
            this.estimatedUploadBps = 12500000; // 100 Mbps = 12,500,000 bytes/sec initial estimate
            this.freeGuestBucket = 1250000;
            this.paidBucket = 1250000;
            this.lastTickMs = performance.now();

            this.freeGuestQueue = [];
            this.paidQueue = [];
            this.rrFreeIdx = 0;
            this.rrPaidIdx = 0;

            this.outFreeBytes = 0;
            this.outPaidBytes = 0;
            this.windowSamples = []; // { ts, freeBytes, paidBytes }

            this._timer = setInterval(() => this._tick(), 20);
        }

        setLimitProfile(val) {
            if (!val || val === 'auto') {
                this.mode = 'auto';
                return;
            }
            const mbps = Number(val);
            if (Number.isFinite(mbps) && mbps > 0) {
                this.mode = mbps;
            }
        }

        updateEstimatedBandwidthBits(bitsPerSec) {
            if (this.mode !== 'auto') return;
            if (!Number.isFinite(bitsPerSec) || bitsPerSec <= 0) return;
            const bytesPerSec = Math.max(250000, Math.min(125000000, bitsPerSec / 8)); // clamp 2 Mbps .. 1000 Mbps
            this.estimatedUploadBps = this.estimatedUploadBps * 0.7 + bytesPerSec * 0.3;
        }

        getCapacityBytesPerSec() {
            if (typeof this.mode === 'number' && this.mode > 0) {
                return (this.mode * 1000000) / 8;
            }
            return this.estimatedUploadBps;
        }

        getPoolLimits() {
            const cap = this.getCapacityBytesPerSec();
            return {
                capacityBps: cap,
                freeGuestMaxBps: cap * 0.35, // 35% for Free + Guest
                paidMaxBps: cap * 0.40,      // 40% for Paid
                totalMaxBps: cap * 0.75      // 75% combined high P2P quota
            };
        }

        enqueueTransfer(peerTier, dc, reqId, buffer) {
            if (!dc || dc.readyState !== 'open' || !buffer || buffer.byteLength === 0) return;
            const normTier = normalizePeerTier(peerTier);
            const totalBytes = buffer.byteLength;
            const totalFrames = Math.max(1, Math.ceil(totalBytes / FRAME_PAYLOAD_SIZE));

            const job = {
                pool: normTier === 'paid' ? 'paid' : 'free_guest',
                dc,
                reqId,
                buffer,
                totalFrames,
                nextFrame: 0
            };

            if (job.pool === 'paid') {
                if (this.paidQueue.length > 48) this.paidQueue.shift();
                this.paidQueue.push(job);
            } else {
                if (this.freeGuestQueue.length > 48) this.freeGuestQueue.shift();
                this.freeGuestQueue.push(job);
            }
        }

        _tick() {
            const now = performance.now();
            const dt = Math.min(0.25, Math.max(0.001, (now - this.lastTickMs) / 1000));
            this.lastTickMs = now;

            const limits = this.getPoolLimits();
            const freeMaxBurst = Math.max(FRAME_PAYLOAD_SIZE * 8, limits.freeGuestMaxBps * 1.5);
            const paidMaxBurst = Math.max(FRAME_PAYLOAD_SIZE * 8, limits.paidMaxBps * 1.5);

            this.freeGuestBucket = Math.min(freeMaxBurst, this.freeGuestBucket + limits.freeGuestMaxBps * dt);
            this.paidBucket = Math.min(paidMaxBurst, this.paidBucket + limits.paidMaxBps * dt);

            const freeSent = this._drainQueue(this.freeGuestQueue, 'freeGuestBucket', 'free_guest');
            const paidSent = this._drainQueue(this.paidQueue, 'paidBucket', 'paid');

            if (freeSent > 0 || paidSent > 0) {
                this.windowSamples.push({ ts: Date.now(), freeBytes: freeSent, paidBytes: paidSent });
            }
            const cutoff = Date.now() - 2000;
            while (this.windowSamples.length > 0 && this.windowSamples[0].ts < cutoff) {
                this.windowSamples.shift();
            }
        }

        _drainQueue(queue, bucketProp, poolName) {
            let sentBytesTotal = 0;
            let guard = 0;

            while (queue.length > 0 && guard < 64) {
                guard++;
                const idx = poolName === 'paid'
                    ? (this.rrPaidIdx % queue.length)
                    : (this.rrFreeIdx % queue.length);
                const job = queue[idx];

                if (!job || !job.dc || job.dc.readyState !== 'open') {
                    queue.splice(idx, 1);
                    continue;
                }
                if (job.dc.bufferedAmount > MAX_DC_BUFFERED) {
                    if (poolName === 'paid') this.rrPaidIdx++;
                    else this.rrFreeIdx++;
                    if (guard >= queue.length) break;
                    continue;
                }

                const start = job.nextFrame * FRAME_PAYLOAD_SIZE;
                const end = Math.min(job.buffer.byteLength, start + FRAME_PAYLOAD_SIZE);
                const payloadLen = end - start;
                const frameCost = FRAME_HEADER_BYTES + payloadLen;

                if (this[bucketProp] < frameCost) {
                    break;
                }

                const frameBuf = new ArrayBuffer(frameCost);
                const view = new DataView(frameBuf);
                view.setUint32(0, job.reqId >>> 0, false);
                view.setUint32(4, job.nextFrame >>> 0, false);
                view.setUint32(8, job.totalFrames >>> 0, false);
                new Uint8Array(frameBuf, FRAME_HEADER_BYTES).set(new Uint8Array(job.buffer, start, payloadLen));

                try {
                    job.dc.send(frameBuf);
                } catch (_) {
                    queue.splice(idx, 1);
                    continue;
                }

                this[bucketProp] -= frameCost;
                sentBytesTotal += frameCost;
                if (poolName === 'paid') this.outPaidBytes += frameCost;
                else this.outFreeBytes += frameCost;

                job.nextFrame++;
                if (job.nextFrame >= job.totalFrames) {
                    queue.splice(idx, 1);
                } else {
                    if (poolName === 'paid') this.rrPaidIdx++;
                    else this.rrFreeIdx++;
                }
            }
            return sentBytesTotal;
        }

        getStats() {
            const cutoff = Date.now() - 2000;
            let freeWin = 0;
            let paidWin = 0;
            for (let i = 0; i < this.windowSamples.length; i++) {
                const s = this.windowSamples[i];
                if (s.ts >= cutoff) {
                    freeWin += s.freeBytes;
                    paidWin += s.paidBytes;
                }
            }
            const limits = this.getPoolLimits();
            return {
                mode: this.mode,
                capacityBps: limits.capacityBps,
                freeLimitBps: limits.freeGuestMaxBps,
                paidLimitBps: limits.paidMaxBps,
                totalLimitBps: limits.totalMaxBps,
                freeRateBps: freeWin / 2,
                paidRateBps: paidWin / 2,
                outFreeBytes: this.outFreeBytes,
                outPaidBytes: this.outPaidBytes
            };
        }
    }

    function getGlobalQuota() {
        if (!global.__iptvUploadQuota) {
            global.__iptvUploadQuota = new UploadQuotaCoordinator();
        }
        return global.__iptvUploadQuota;
    }

    /**
     * WebRTC DataChannel P2P + Client-Side 1-Hour .ts DVR Engine per Channel:
     * - Stores .ts segments in client IndexedDB (up to 60 minutes / 1.2 GB) so server stores 0 DVR data.
     * - Builds & shares a rolling 1-hour client-side .m3u8 timeline so pausing for up to 1 hour resumes seamlessly.
     * - Shares cached .ts segments (both RAM and IndexedDB history) across peers over WebRTC DataChannel.
     */
    class IPTVP2PEngine {
        constructor(options) {
            this.channelId = options.channelId;
            this.getToken = options.getToken || (() => '');
            this.maxPeers = options.maxPeers || 8;
            this.onStatus = options.onStatus || (() => {});
            this.onStats = options.onStats || (() => {});

            this.quota = getGlobalQuota();
            this.dvrCache = getGlobalDVRCache();
            this.selfId = '';
            this.selfTier = 'guest';
            this.ws = null;
            this.destroyed = false;
            this.reconnectAttempt = 0;
            this.reconnectTimer = null;

            this.peers = new Map(); // remotePeerId -> { id, tier, isTailscale, pc, dc, remoteHave: Map<key,{sz,h}>, pendingIce: [], inFlightReqs: 0, failCount: 0 }
            this.latestPeerTierMap = new Map();
            this.latestPeerTailscaleMap = new Map();
            this.chunkCache = new Map(); // hot RAM LRU: key -> { buf, sha256, ts, size }
            this.cacheBytes = 0;
            this.maxCacheEntries = 60;
            this.maxCacheBytes = 64 * 1024 * 1024;

            // Client-side rolling 1-hour HLS .m3u8 timeline per media variant
            this.dvrTimelines = new Map(); // variantKey -> { targetDuration, baseMediaSeq, totalDuration, segments: [], lastRefreshTs }
            this.activeMediaVariantKey = '';
            this.activeMediaPlaylistUrl = '';
            this.lastPlaylistMergeTs = 0;
            this.pausedPrefetchTimer = null;
            this.pausedPrefetchInFlight = false;

            this.nextReqId = 1;
            this.pendingDownloads = new Map();
            this.inFlightKeys = new Map();

            this.p2pBytesIn = 0;
            this.httpBytesIn = 0;
            this.totalSwarmPeers = 1;

            this.iceServers = [
                { urls: 'stun:stun.l.google.com:19302' },
                { urls: 'stun:stun1.l.google.com:19302' }
            ];

            this._statsTimer = setInterval(() => this._pollWebRTCAndEmitStats(), 2500);
            this._wsReportTimer = setInterval(() => this._sendStatsReportToTracker(), 5000);
            this._connectWS();
        }

        _connectWS() {
            if (this.destroyed || !this.channelId) return;
            const proto = global.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const tok = this.getToken() || '';
            const url = `${proto}//${global.location.host}/ws/signaling?channel_id=${encodeURIComponent(this.channelId)}&token=${encodeURIComponent(tok)}`;

            try {
                this.ws = new WebSocket(url);
            } catch (_) {
                this._scheduleReconnect();
                return;
            }

            this.ws.onopen = () => {
                if (this.destroyed) return;
                this.reconnectAttempt = 0;
                this.onStatus({ wsConnected: true, authorized: true, totalPeers: this.totalSwarmPeers, p2pPeers: this.getOpenPeerCount() });
            };

            this.ws.onmessage = (evt) => {
                if (this.destroyed) return;
                try {
                    const msg = JSON.parse(evt.data);
                    if (msg.error) {
                        this.onStatus({ wsConnected: false, authorized: false, error: msg.error, totalPeers: 0, p2pPeers: 0 });
                        return;
                    }
                    if (msg.type === 'WELCOME') {
                        this.selfId = msg.target_id || '';
                        if (msg.self_tier) this.selfTier = normalizePeerTier(msg.self_tier);
                    } else if (msg.type === 'SWARM_PEERS') {
                        if (msg.self_tier) this.selfTier = normalizePeerTier(msg.self_tier);
                        this.totalSwarmPeers = msg.total_peers || 1;
                        const infos = Array.isArray(msg.peer_infos) && msg.peer_infos.length > 0
                            ? msg.peer_infos
                            : (msg.peers || []).map(id => ({ id, tier: 'guest' }));
                        this._syncPeerList(infos);
                        this._emitStats();
                    } else if (msg.type === 'OFFER' && msg.sender_id && msg.sdp) {
                        this._handleOffer(msg.sender_id, msg.sdp);
                    } else if (msg.type === 'ANSWER' && msg.sender_id && msg.sdp) {
                        this._handleAnswer(msg.sender_id, msg.sdp);
                    } else if (msg.type === 'ICE_CANDIDATE' && msg.sender_id && msg.candidate) {
                        this._handleRemoteIce(msg.sender_id, msg.candidate);
                    }
                } catch (_) {}
            };

            this.ws.onclose = () => {
                if (this.destroyed) return;
                this.onStatus({ wsConnected: false, authorized: true, totalPeers: this.totalSwarmPeers, p2pPeers: this.getOpenPeerCount() });
                this._scheduleReconnect();
            };
        }

        _scheduleReconnect() {
            if (this.destroyed || this.reconnectTimer) return;
            const delay = Math.min(12000, 1500 * Math.pow(1.5, this.reconnectAttempt));
            this.reconnectAttempt++;
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                this._connectWS();
            }, delay);
        }

        _sendWS(obj) {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                try {
                    this.ws.send(JSON.stringify(obj));
                } catch (_) {}
            }
        }

        _syncPeerList(peerInfos) {
            const activeMap = new Map();
            peerInfos.forEach(p => {
                if (p && p.id && p.id !== this.selfId) {
                    const norm = normalizePeerTier(p.tier);
                    const isTs = Boolean(p.is_tailscale);
                    activeMap.set(p.id, { id: p.id, tier: norm, isTailscale: isTs });
                    this.latestPeerTierMap.set(p.id, norm);
                    if (isTs) this.latestPeerTailscaleMap.set(p.id, true);
                }
            });

            for (const pid of Array.from(this.peers.keys())) {
                if (!activeMap.has(pid)) {
                    this._closePeer(pid);
                } else {
                    const peerObj = this.peers.get(pid);
                    if (peerObj) {
                        peerObj.tier = activeMap.get(pid).tier;
                        if (activeMap.get(pid).isTailscale) peerObj.isTailscale = true;
                    }
                }
            }

            if (typeof global.RTCPeerConnection === 'undefined' || !this.selfId) {
                return;
            }

            for (const [pid, info] of activeMap.entries()) {
                if (this.peers.size >= this.maxPeers) break;
                if (this.peers.has(pid)) continue;
                const initiator = this.selfId < pid;
                if (initiator) {
                    this._createPeerConnection(pid, info.tier, true, info.isTailscale);
                }
            }
        }

        _createPeerConnection(remoteId, remoteTier, isInitiator, isTailscale) {
            if (this.destroyed || typeof global.RTCPeerConnection === 'undefined') return null;
            if (this.peers.has(remoteId)) return this.peers.get(remoteId);

            const pc = new RTCPeerConnection({ iceServers: this.iceServers });
            const peerObj = {
                id: remoteId,
                tier: normalizePeerTier(remoteTier),
                isTailscale: Boolean(isTailscale || this.latestPeerTailscaleMap.get(remoteId)),
                pc,
                dc: null,
                remoteHave: new Map(),
                pendingIce: [],
                inFlightReqs: 0,
                failCount: 0
            };
            this.peers.set(remoteId, peerObj);

            pc.onicecandidate = (evt) => {
                if (evt.candidate && !this.destroyed) {
                    this._sendWS({
                        type: 'ICE_CANDIDATE',
                        target_id: remoteId,
                        candidate: JSON.stringify(evt.candidate)
                    });
                }
            };

            pc.onconnectionstatechange = () => {
                const st = pc.connectionState;
                if (st === 'failed' || st === 'closed' || st === 'disconnected') {
                    if (st === 'failed' || st === 'closed') {
                        this._closePeer(remoteId);
                    }
                    this._emitStats();
                }
            };

            if (isInitiator) {
                const dc = pc.createDataChannel('iptv-p2p', { ordered: true });
                this._attachDataChannel(peerObj, dc);
                pc.createOffer()
                    .then(offer => pc.setLocalDescription(offer).then(() => offer))
                    .then(offer => {
                        if (!this.destroyed) {
                            this._sendWS({
                                type: 'OFFER',
                                target_id: remoteId,
                                sdp: JSON.stringify(offer)
                            });
                        }
                    })
                    .catch(() => this._closePeer(remoteId));
            } else {
                pc.ondatachannel = (evt) => {
                    this._attachDataChannel(peerObj, evt.channel);
                };
            }

            return peerObj;
        }

        async _handleOffer(remoteId, sdpRaw) {
            if (this.destroyed || typeof global.RTCPeerConnection === 'undefined') return;
            try {
                let peerObj = this.peers.get(remoteId);
                if (!peerObj) {
                    const verifiedTier = this.latestPeerTierMap.get(remoteId) || 'guest';
                    const isTs = !!this.latestPeerTailscaleMap.get(remoteId);
                    peerObj = this._createPeerConnection(remoteId, verifiedTier, false, isTs);
                }
                if (!peerObj) return;
                const offer = JSON.parse(sdpRaw);
                await peerObj.pc.setRemoteDescription(new RTCSessionDescription(offer));
                await this._flushPendingIce(peerObj);
                const answer = await peerObj.pc.createAnswer();
                await peerObj.pc.setLocalDescription(answer);
                this._sendWS({
                    type: 'ANSWER',
                    target_id: remoteId,
                    sdp: JSON.stringify(answer)
                });
            } catch (_) {}
        }

        async _handleAnswer(remoteId, sdpRaw) {
            const peerObj = this.peers.get(remoteId);
            if (!peerObj) return;
            try {
                const answer = JSON.parse(sdpRaw);
                await peerObj.pc.setRemoteDescription(new RTCSessionDescription(answer));
                await this._flushPendingIce(peerObj);
            } catch (_) {}
        }

        async _handleRemoteIce(remoteId, candRaw) {
            const peerObj = this.peers.get(remoteId);
            if (!peerObj) return;
            try {
                const cand = JSON.parse(candRaw);
                if (peerObj.pc.remoteDescription && peerObj.pc.remoteDescription.type) {
                    await peerObj.pc.addIceCandidate(new RTCIceCandidate(cand));
                } else {
                    peerObj.pendingIce.push(cand);
                }
            } catch (_) {}
        }

        async _flushPendingIce(peerObj) {
            while (peerObj.pendingIce.length > 0) {
                const cand = peerObj.pendingIce.shift();
                try {
                    await peerObj.pc.addIceCandidate(new RTCIceCandidate(cand));
                } catch (_) {}
            }
        }

        _attachDataChannel(peerObj, dc) {
            dc.binaryType = 'arraybuffer';
            peerObj.dc = dc;

            dc.onopen = () => {
                // Announce BOTH hot RAM chunks AND persistent client IndexedDB .ts segments
                const merged = new Map();
                const persisted = this.dvrCache.getKeysForChannel(this.channelId);
                for (let i = 0; i < persisted.length; i++) {
                    merged.set(persisted[i].k, persisted[i]);
                }
                for (const [k, entry] of this.chunkCache.entries()) {
                    merged.set(k, { k, sz: entry.size, h: entry.sha256 || '' });
                }

                const allItems = Array.from(merged.values());
                for (let i = 0; i < allItems.length; i += 120) {
                    try {
                        dc.send(JSON.stringify({ t: 'HAVE_BATCH', items: allItems.slice(i, i + 120) }));
                    } catch (_) {
                        break;
                    }
                }

                // Also share recent DVR playlist timeline entries so peers can discover historical .ts files
                if (this.activeMediaVariantKey) {
                    const tl = this.dvrTimelines.get(this.activeMediaVariantKey);
                    if (tl && tl.segments.length > 0) {
                        const tail = tl.segments.slice(-450);
                        for (let i = 0; i < tail.length; i += 80) {
                            try {
                                dc.send(JSON.stringify({
                                    t: 'DVR_INDEX',
                                    vk: this.activeMediaVariantKey,
                                    td: tl.targetDuration,
                                    entries: tail.slice(i, i + 80).map(s => ({
                                        k: s.key,
                                        u: s.url,
                                        d: s.duration,
                                        ti: s.title || '',
                                        dc: !!s.discontinuity,
                                        ts: s.ts || Date.now()
                                    }))
                                }));
                            } catch (_) {
                                break;
                            }
                        }
                    }
                }

                this._emitStats();
            };

            dc.onclose = () => {
                this._emitStats();
            };

            dc.onmessage = (evt) => {
                if (this.destroyed) return;
                if (typeof evt.data === 'string') {
                    this._handleDCControl(peerObj, evt.data);
                } else if (evt.data instanceof ArrayBuffer) {
                    this._handleDCBinaryFrame(peerObj, evt.data);
                } else if (evt.data instanceof Blob) {
                    evt.data.arrayBuffer().then(ab => this._handleDCBinaryFrame(peerObj, ab)).catch(() => {});
                }
            };
        }

        _recordRemoteHave(peerObj, key, sz, h) {
            if (!key) return;
            peerObj.remoteHave.set(key, { sz: Number(sz || 0), h: String(h || '') });
            while (peerObj.remoteHave.size > 2000) {
                const firstKey = peerObj.remoteHave.keys().next().value;
                peerObj.remoteHave.delete(firstKey);
            }
        }

        async _handleDCControl(peerObj, text) {
            try {
                const msg = JSON.parse(text);
                if (msg.t === 'HAVE' && msg.k) {
                    this._recordRemoteHave(peerObj, msg.k, msg.sz, msg.h);
                } else if (msg.t === 'HAVE_BATCH' && Array.isArray(msg.items)) {
                    for (let i = 0; i < msg.items.length; i++) {
                        const it = msg.items[i];
                        if (it && it.k) this._recordRemoteHave(peerObj, it.k, it.sz, it.h);
                    }
                } else if (msg.t === 'DVR_INDEX' && msg.vk && Array.isArray(msg.entries)) {
                    this._mergePeerTimelineEntries(msg.vk, msg.td, msg.entries);
                } else if (msg.t === 'REQ' && msg.id && msg.k) {
                    let entry = this.chunkCache.get(msg.k);
                    if (!entry || !entry.buf) {
                        // Look up client's persistent 1-hour IndexedDB DVR cache!
                        entry = await this.dvrCache.get(msg.k);
                    }
                    if (entry && entry.buf && peerObj.dc && peerObj.dc.readyState === 'open') {
                        this.quota.enqueueTransfer(peerObj.tier, peerObj.dc, Number(msg.id), entry.buf);
                    } else if (peerObj.dc && peerObj.dc.readyState === 'open') {
                        try {
                            peerObj.dc.send(JSON.stringify({ t: 'MISS', id: msg.id, k: msg.k }));
                        } catch (_) {}
                    }
                } else if (msg.t === 'MISS' && msg.id) {
                    const pending = this.pendingDownloads.get(Number(msg.id));
                    if (pending && pending.peerId === peerObj.id) {
                        pending.reject(new Error('peer_miss'));
                    }
                }
            } catch (_) {}
        }

        _handleDCBinaryFrame(peerObj, ab) {
            if (!ab || ab.byteLength <= FRAME_HEADER_BYTES) return;
            const view = new DataView(ab);
            const reqId = view.getUint32(0, false);
            const frameIdx = view.getUint32(4, false);
            const totalFrames = view.getUint32(8, false);

            const pending = this.pendingDownloads.get(reqId);
            if (!pending || pending.peerId !== peerObj.id) return;
            if (totalFrames === 0 || totalFrames > 4096 || frameIdx >= totalFrames) return;

            if (pending.firstByteTimer) {
                clearTimeout(pending.firstByteTimer);
                pending.firstByteTimer = null;
            }
            if (!pending.frames) {
                pending.totalFrames = totalFrames;
                pending.frames = new Array(totalFrames);
            }
            if (!pending.frames[frameIdx]) {
                pending.frames[frameIdx] = new Uint8Array(ab, FRAME_HEADER_BYTES);
                pending.receivedCount++;
            }

            if (pending.receivedCount === pending.totalFrames) {
                let totalLen = 0;
                for (let i = 0; i < pending.frames.length; i++) {
                    if (!pending.frames[i]) return;
                    totalLen += pending.frames[i].byteLength;
                }
                const combined = new Uint8Array(totalLen);
                let offset = 0;
                for (let i = 0; i < pending.frames.length; i++) {
                    combined.set(pending.frames[i], offset);
                    offset += pending.frames[i].byteLength;
                }
                pending.resolve(combined.buffer);
            }
        }

        _closePeer(pid) {
            const peerObj = this.peers.get(pid);
            if (!peerObj) return;
            this.peers.delete(pid);
            try { if (peerObj.dc) peerObj.dc.close(); } catch (_) {}
            try { if (peerObj.pc) peerObj.pc.close(); } catch (_) {}
        }

        getOpenPeerCount() {
            let cnt = 0;
            for (const p of this.peers.values()) {
                if (p.dc && p.dc.readyState === 'open') cnt++;
            }
            return cnt;
        }

        _storeInHotRam(key, arrayBuffer, shaHex) {
            if (this.destroyed || !key || !arrayBuffer || arrayBuffer.byteLength === 0) return;
            const size = arrayBuffer.byteLength;
            if (size > 24 * 1024 * 1024) return;

            if (this.chunkCache.has(key)) {
                const prev = this.chunkCache.get(key);
                this.cacheBytes -= prev.size;
                this.chunkCache.delete(key);
            }
            this.chunkCache.set(key, { buf: arrayBuffer, sha256: shaHex || '', ts: Date.now(), size });
            this.cacheBytes += size;

            while (this.chunkCache.size > this.maxCacheEntries || this.cacheBytes > this.maxCacheBytes) {
                const oldestKey = this.chunkCache.keys().next().value;
                if (!oldestKey) break;
                const oldEntry = this.chunkCache.get(oldestKey);
                this.cacheBytes -= (oldEntry ? oldEntry.size : 0);
                this.chunkCache.delete(oldestKey);
            }
        }

        cacheAndAnnounce(key, arrayBuffer, knownSha256) {
            if (this.destroyed || !key || !arrayBuffer || arrayBuffer.byteLength === 0) return;
            const size = arrayBuffer.byteLength;
            if (size > 24 * 1024 * 1024) return;

            const store = (shaHex) => {
                if (this.destroyed) return;
                this._storeInHotRam(key, arrayBuffer, shaHex || '');
                // Persist into client IndexedDB (1-hour DVR buffer for expired live segments)
                this.dvrCache.put(key, this.channelId, arrayBuffer, shaHex || '').then(() => {
                    if (!this.destroyed) this._emitStats();
                });

                const msgStr = JSON.stringify({ t: 'HAVE', k: key, sz: size, h: shaHex || '' });
                for (const p of this.peers.values()) {
                    if (p.dc && p.dc.readyState === 'open') {
                        try { p.dc.send(msgStr); } catch (_) {}
                    }
                }
            };

            if (knownSha256) {
                store(knownSha256);
            } else {
                computeSha256Hex(arrayBuffer).then(store);
            }
        }

        hasOpenPeerForKey(key) {
            for (const p of this.peers.values()) {
                if (p.dc && p.dc.readyState === 'open' && p.remoteHave.has(key)) {
                    return true;
                }
            }
            return false;
        }

        _pickBestPeerForKey(key, excludeIds) {
            const candidates = [];
            for (const p of this.peers.values()) {
                if (excludeIds && excludeIds.has(p.id)) continue;
                if (!p.dc || p.dc.readyState !== 'open') continue;
                const info = p.remoteHave.get(key);
                if (!info) continue;
                candidates.push({ peer: p, meta: info });
            }
            if (candidates.length === 0) return null;
            candidates.sort((a, b) =>
                ((b.peer.isTailscale ? 1 : 0) - (a.peer.isTailscale ? 1 : 0)) ||
                (a.peer.inFlightReqs - b.peer.inFlightReqs) ||
                (a.peer.failCount - b.peer.failCount)
            );
            return candidates[0];
        }

        async _downloadFromPeer(peerObj, key, expectedHash) {
            const reqId = this.nextReqId++;
            return new Promise((resolve, reject) => {
                let settled = false;
                const cleanup = () => {
                    this.pendingDownloads.delete(reqId);
                    peerObj.inFlightReqs = Math.max(0, peerObj.inFlightReqs - 1);
                    if (pending.firstByteTimer) clearTimeout(pending.firstByteTimer);
                    if (pending.fullTimer) clearTimeout(pending.fullTimer);
                };

                const pending = {
                    peerId: peerObj.id,
                    key,
                    expectedHash: expectedHash || '',
                    frames: null,
                    receivedCount: 0,
                    totalFrames: 0,
                    firstByteTimer: null,
                    fullTimer: null,
                    resolve: async (buf) => {
                        if (settled) return;
                        settled = true;
                        cleanup();
                        if (pending.expectedHash) {
                            const actual = await computeSha256Hex(buf);
                            if (actual && actual.toLowerCase() !== pending.expectedHash.toLowerCase()) {
                                peerObj.failCount++;
                                reject(new Error('checksum_mismatch'));
                                return;
                            }
                        }
                        peerObj.failCount = Math.max(0, peerObj.failCount - 1);
                        resolve(buf);
                    },
                    reject: (err) => {
                        if (settled) return;
                        settled = true;
                        cleanup();
                        peerObj.failCount++;
                        reject(err);
                    }
                };

                pending.firstByteTimer = setTimeout(() => pending.reject(new Error('first_byte_timeout')), 900);
                pending.fullTimer = setTimeout(() => pending.reject(new Error('transfer_timeout')), 4500);

                this.pendingDownloads.set(reqId, pending);
                peerObj.inFlightReqs++;

                try {
                    peerObj.dc.send(JSON.stringify({ t: 'REQ', id: reqId, k: key }));
                } catch (err) {
                    pending.reject(err);
                }
            });
        }

        async tryFetchFromPeers(key, expectedSha256) {
            if (this.destroyed || !key) return null;

            const cached = this.chunkCache.get(key);
            if (cached && cached.buf) {
                this.p2pBytesIn += cached.size;
                this._emitStats();
                return cached.buf;
            }

            if (this.inFlightKeys.has(key)) {
                try {
                    return await this.inFlightKeys.get(key);
                } catch (_) {
                    return null;
                }
            }

            const execPromise = (async () => {
                // Check client's own 1-hour IndexedDB DVR cache first!
                const localStored = await this.dvrCache.get(key);
                if (localStored && localStored.buf) {
                    this._storeInHotRam(key, localStored.buf, localStored.sha256 || expectedSha256 || '');
                    this._emitStats();
                    return localStored.buf;
                }

                // Try downloading from connected peers (which also check their 1-hour IndexedDB!)
                const tried = new Set();
                for (let attempt = 0; attempt < 2; attempt++) {
                    const pick = this._pickBestPeerForKey(key, tried);
                    if (!pick) break;
                    tried.add(pick.peer.id);
                    const hashToCheck = expectedSha256 || pick.meta.h || '';
                    try {
                        const buf = await this._downloadFromPeer(pick.peer, key, hashToCheck);
                        this.p2pBytesIn += buf.byteLength;
                        this.cacheAndAnnounce(key, buf, hashToCheck);
                        this._emitStats();
                        return buf;
                    } catch (_) {
                        // Try next peer or fall back to HTTP
                    }
                }
                return null;
            })();

            this.inFlightKeys.set(key, execPromise);
            try {
                return await execPromise;
            } finally {
                this.inFlightKeys.delete(key);
            }
        }

        recordHttpDownload(key, arrayBuffer, knownSha256) {
            if (!arrayBuffer) return;
            this.httpBytesIn += arrayBuffer.byteLength;
            if (key) {
                this.cacheAndAnnounce(key, arrayBuffer, knownSha256);
            }
            this._emitStats();
        }

        extractCanonicalSegmentKey(rawUrl) {
            try {
                const u = new URL(rawUrl, global.location.origin);
                if (u.pathname.includes('/api/stream/') && u.pathname.endsWith('/seg')) {
                    const targetU = u.searchParams.get('u');
                    if (targetU) {
                        return `hls:${this.channelId}:${targetU}`;
                    }
                }
                u.searchParams.delete('token');
                u.searchParams.delete('vid');
                return `http:${this.channelId}:${u.pathname}${u.search}`;
            } catch (_) {
                return `raw:${this.channelId}:${rawUrl}`;
            }
        }

        extractVariantKey(playlistUrl) {
            try {
                const u = new URL(playlistUrl, global.location.origin);
                if (u.pathname.includes('/api/stream/') && u.pathname.endsWith('/playlist')) {
                    const targetU = u.searchParams.get('u');
                    if (targetU) return `v:${this.channelId}:${targetU}`;
                }
                u.searchParams.delete('token');
                u.searchParams.delete('vid');
                return `v:${this.channelId}:${u.pathname}${u.search}`;
            } catch (_) {
                return `v:${this.channelId}:${playlistUrl}`;
            }
        }

        _refreshTokenInUrl(rawUrl) {
            try {
                const u = new URL(rawUrl, global.location.origin);
                const tok = this.getToken() || '';
                if (u.pathname.startsWith('/api/stream/')) {
                    if (tok) u.searchParams.set('token', tok);
                    else u.searchParams.delete('token');
                    return u.pathname + u.search;
                }
                return rawUrl;
            } catch (_) {
                return rawUrl;
            }
        }

        _parseMediaPlaylist(m3u8Text, basePlaylistUrl) {
            const lines = String(m3u8Text || '').split(/\r?\n/);
            let targetDuration = 6;
            let mediaSeq = 0;
            let curDuration = 0;
            let curTitle = '';
            let curKeyTag = '';
            let curMapTag = '';
            let pendingDiscontinuity = false;
            const segments = [];

            for (let i = 0; i < lines.length; i++) {
                const line = lines[i].trim();
                if (!line) continue;
                if (line.startsWith('#EXT-X-TARGETDURATION:')) {
                    const v = parseInt(line.split(':')[1], 10);
                    if (Number.isFinite(v) && v > 0) targetDuration = v;
                } else if (line.startsWith('#EXT-X-MEDIA-SEQUENCE:')) {
                    const v = parseInt(line.split(':')[1], 10);
                    if (Number.isFinite(v) && v >= 0) mediaSeq = v;
                } else if (line.startsWith('#EXT-X-KEY:')) {
                    curKeyTag = line;
                } else if (line.startsWith('#EXT-X-MAP:')) {
                    curMapTag = line;
                } else if (line === '#EXT-X-DISCONTINUITY') {
                    pendingDiscontinuity = true;
                } else if (line.startsWith('#EXTINF:')) {
                    const body = line.slice(8);
                    const commaIdx = body.indexOf(',');
                    if (commaIdx >= 0) {
                        curDuration = parseFloat(body.slice(0, commaIdx)) || targetDuration;
                        curTitle = body.slice(commaIdx + 1);
                    } else {
                        curDuration = parseFloat(body) || targetDuration;
                        curTitle = '';
                    }
                } else if (!line.startsWith('#')) {
                    let absUrl = line;
                    try {
                        const resolved = new URL(line, new URL(basePlaylistUrl, global.location.origin));
                        absUrl = resolved.origin === global.location.origin
                            ? (resolved.pathname + resolved.search)
                            : resolved.href;
                    } catch (_) {}

                    const key = this.extractCanonicalSegmentKey(absUrl);
                    segments.push({
                        key,
                        url: absUrl,
                        duration: curDuration > 0 ? curDuration : targetDuration,
                        title: curTitle,
                        keyTag: curKeyTag,
                        mapTag: curMapTag,
                        discontinuity: pendingDiscontinuity,
                        inUpstream: true,
                        ts: Date.now()
                    });
                    curDuration = 0;
                    curTitle = '';
                    pendingDiscontinuity = false;
                }
            }

            return { targetDuration, mediaSeq, segments };
        }

        _trimTimelineWindow(tl) {
            let total = 0;
            let clientExpiredSec = 0;
            for (let i = 0; i < tl.segments.length; i++) {
                const dur = tl.segments[i].duration || 4;
                total += dur;
                if (!tl.segments[i].inUpstream) {
                    clientExpiredSec += dur;
                }
            }
            const now = Date.now();
            // Never chop segments that the upstream CDN itself still provides (e.g. 24-hour rewind channels!).
            // Only trim older segments that rolled off upstream once client-retained history exceeds 60 minutes.
            while (
                tl.segments.length > 6 &&
                !tl.segments[0].inUpstream &&
                (clientExpiredSec > MAX_DVR_TIMELINE_SEC || (now - tl.segments[0].ts > MAX_DVR_AGE_MS))
            ) {
                const removed = tl.segments.shift();
                const dur = removed.duration || 4;
                total -= dur;
                clientExpiredSec -= dur;
                tl.baseMediaSeq++;
            }
            tl.totalDuration = Math.max(0, total);
        }

        mergeMediaPlaylist(playlistUrl, m3u8Text) {
            if (!m3u8Text || !m3u8Text.includes('#EXTINF:')) {
                return { isMediaPlaylist: false, text: m3u8Text, newSegments: [] };
            }

            const parsed = this._parseMediaPlaylist(m3u8Text, playlistUrl);
            if (parsed.segments.length === 0) {
                return { isMediaPlaylist: false, text: m3u8Text, newSegments: [] };
            }

            const vk = this.extractVariantKey(playlistUrl);
            this.activeMediaVariantKey = vk;
            this.activeMediaPlaylistUrl = playlistUrl;
            this.lastPlaylistMergeTs = Date.now();

            let tl = this.dvrTimelines.get(vk);
            if (!tl) {
                tl = {
                    targetDuration: parsed.targetDuration,
                    baseMediaSeq: parsed.mediaSeq,
                    totalDuration: 0,
                    segments: [],
                    lastRefreshTs: Date.now()
                };
                this.dvrTimelines.set(vk, tl);
            }
            tl.targetDuration = Math.max(tl.targetDuration, parsed.targetDuration);
            tl.lastRefreshTs = Date.now();

            const incomingKeySet = new Set();
            for (let j = 0; j < parsed.segments.length; j++) {
                incomingKeySet.add(parsed.segments[j].key);
            }
            for (let i = 0; i < tl.segments.length; i++) {
                tl.segments[i].inUpstream = incomingKeySet.has(tl.segments[i].key);
            }

            const newSegments = [];
            if (tl.segments.length === 0) {
                for (let i = 0; i < parsed.segments.length; i++) {
                    tl.segments.push(parsed.segments[i]);
                    newSegments.push(parsed.segments[i]);
                }
            } else {
                const existingIndexByKey = new Map();
                for (let i = 0; i < tl.segments.length; i++) {
                    existingIndexByKey.set(tl.segments[i].key, i);
                }

                let lastMatchedIncomingIdx = -1;
                for (let j = parsed.segments.length - 1; j >= 0; j--) {
                    if (existingIndexByKey.has(parsed.segments[j].key)) {
                        lastMatchedIncomingIdx = j;
                        break;
                    }
                }

                if (lastMatchedIncomingIdx >= 0) {
                    for (let j = lastMatchedIncomingIdx + 1; j < parsed.segments.length; j++) {
                        const seg = parsed.segments[j];
                        if (!existingIndexByKey.has(seg.key)) {
                            tl.segments.push(seg);
                            newSegments.push(seg);
                        }
                    }
                } else {
                    const expectedNextSeq = tl.baseMediaSeq + tl.segments.length;
                    const jumpedFar = parsed.mediaSeq > expectedNextSeq + 12;
                    for (let j = 0; j < parsed.segments.length; j++) {
                        const seg = Object.assign({}, parsed.segments[j]);
                        if (j === 0 && jumpedFar) seg.discontinuity = true;
                        tl.segments.push(seg);
                        newSegments.push(seg);
                    }
                }
            }

            this._trimTimelineWindow(tl);

            if (newSegments.length > 0 && newSegments.length <= 100) {
                this._broadcastTimelineEntries(vk, tl.targetDuration, newSegments);
            } else if (tl.segments.length > 0) {
                this._broadcastTimelineEntries(vk, tl.targetDuration, tl.segments.slice(-30));
            }

            return {
                isMediaPlaylist: true,
                variantKey: vk,
                text: this._serializeTimeline(tl),
                newSegments
            };
        }

        _broadcastTimelineEntries(vk, td, segments) {
            if (!segments || segments.length === 0) return;
            const payload = JSON.stringify({
                t: 'DVR_INDEX',
                vk,
                td,
                entries: segments.map(s => ({
                    k: s.key,
                    u: s.url,
                    d: s.duration,
                    ti: s.title || '',
                    dc: !!s.discontinuity,
                    ts: s.ts || Date.now()
                }))
            });
            for (const p of this.peers.values()) {
                if (p.dc && p.dc.readyState === 'open') {
                    try { p.dc.send(payload); } catch (_) {}
                }
            }
        }

        _mergePeerTimelineEntries(vk, td, entries) {
            if (!vk || !Array.isArray(entries) || entries.length === 0) return;
            let tl = this.dvrTimelines.get(vk);
            if (!tl) {
                tl = {
                    targetDuration: Number(td || 6),
                    baseMediaSeq: 0,
                    totalDuration: 0,
                    segments: [],
                    lastRefreshTs: Date.now()
                };
                this.dvrTimelines.set(vk, tl);
            }
            const knownKeys = new Set(tl.segments.map(s => s.key));
            let appended = false;
            if (tl.segments.length === 0) {
                for (let i = 0; i < entries.length; i++) {
                    const e = entries[i];
                    if (!e || !e.k || !e.u || knownKeys.has(e.k)) continue;
                    knownKeys.add(e.k);
                    tl.segments.push({
                        key: e.k,
                        url: e.u,
                        duration: Number(e.d || 4),
                        title: e.ti || '',
                        keyTag: '',
                        mapTag: '',
                        discontinuity: !!e.dc,
                        ts: Number(e.ts || Date.now())
                    });
                    appended = true;
                }
            } else {
                // Only prepend older history from peer if we match an overlap segment
                let firstOverlapTlIdx = -1;
                let firstOverlapPeerIdx = -1;
                const peerIdxByKey = new Map();
                for (let i = 0; i < entries.length; i++) {
                    if (entries[i] && entries[i].k) peerIdxByKey.set(entries[i].k, i);
                }
                for (let i = 0; i < tl.segments.length; i++) {
                    if (peerIdxByKey.has(tl.segments[i].key)) {
                        firstOverlapTlIdx = i;
                        firstOverlapPeerIdx = peerIdxByKey.get(tl.segments[i].key);
                        break;
                    }
                }
                if (firstOverlapTlIdx === 0 && firstOverlapPeerIdx > 0) {
                    const prefix = [];
                    for (let i = 0; i < firstOverlapPeerIdx; i++) {
                        const e = entries[i];
                        if (e && e.k && e.u && !knownKeys.has(e.k)) {
                            knownKeys.add(e.k);
                            prefix.push({
                                key: e.k,
                                url: e.u,
                                duration: Number(e.d || 4),
                                title: e.ti || '',
                                keyTag: '',
                                mapTag: '',
                                discontinuity: !!e.dc,
                                ts: Number(e.ts || Date.now())
                            });
                        }
                    }
                    if (prefix.length > 0) {
                        tl.segments = prefix.concat(tl.segments);
                        tl.baseMediaSeq = Math.max(0, tl.baseMediaSeq - prefix.length);
                        appended = true;
                    }
                }
            }
            if (appended) {
                this._trimTimelineWindow(tl);
            }
        }

        _serializeTimeline(tl) {
            const out = [
                '#EXTM3U',
                '#EXT-X-VERSION:3',
                `#EXT-X-TARGETDURATION:${Math.max(2, Math.ceil(tl.targetDuration || 4))}`,
                `#EXT-X-MEDIA-SEQUENCE:${tl.baseMediaSeq}`
            ];
            let activeKeyTag = '';
            let activeMapTag = '';
            for (let i = 0; i < tl.segments.length; i++) {
                const seg = tl.segments[i];
                if (seg.keyTag && seg.keyTag !== activeKeyTag) {
                    activeKeyTag = seg.keyTag;
                    out.push(activeKeyTag);
                }
                if (seg.mapTag && seg.mapTag !== activeMapTag) {
                    activeMapTag = seg.mapTag;
                    out.push(activeMapTag);
                }
                if (seg.discontinuity && i > 0) {
                    out.push('#EXT-X-DISCONTINUITY');
                }
                const durStr = Number(seg.duration || 4).toFixed(3).replace(/\.?0+$/, '');
                out.push(`#EXTINF:${durStr},${seg.title || ''}`);
                out.push(this._refreshTokenInUrl(seg.url));
            }
            return out.join('\n') + '\n';
        }

        /**
         * Background Client DVR Prefetcher (runs while <video> is paused):
         * Keeps fetching new .m3u8 updates and storing incoming .ts files into client IndexedDB
         * (via P2P first, HTTP fallback second) so pausing for up to 1 hour keeps every .ts frame on the client.
         */
        startPausedDvrPrefetch() {
            if (this.destroyed || this.pausedPrefetchTimer) return;
            this.pausedPrefetchTimer = setInterval(() => this._tickPausedPrefetch(), 3500);
            this._tickPausedPrefetch();
        }

        stopPausedDvrPrefetch() {
            if (this.pausedPrefetchTimer) {
                clearInterval(this.pausedPrefetchTimer);
                this.pausedPrefetchTimer = null;
            }
        }

        async _tickPausedPrefetch() {
            if (this.destroyed || this.pausedPrefetchInFlight || !this.activeMediaPlaylistUrl) return;
            this.pausedPrefetchInFlight = true;
            try {
                if (Date.now() - this.lastPlaylistMergeTs > 3000) {
                    const plUrl = this._refreshTokenInUrl(this.activeMediaPlaylistUrl);
                    const res = await fetch(plUrl);
                    if (res.ok) {
                        const txt = await res.text();
                        this.mergeMediaPlaylist(plUrl, txt);
                    }
                }

                const tl = this.dvrTimelines.get(this.activeMediaVariantKey);
                if (!tl || tl.segments.length === 0) return;

                // Prefetch up to 3 uncached recent segments per tick (preferring P2P peers first)
                const tail = tl.segments.slice(-12);
                let fetched = 0;
                for (let i = 0; i < tail.length && fetched < 3; i++) {
                    if (this.destroyed) break;
                    const seg = tail[i];
                    if (this.chunkCache.has(seg.key) || this.dvrCache.has(seg.key)) continue;

                    fetched++;
                    const fromPeer = await this.tryFetchFromPeers(seg.key, '');
                    if (!fromPeer) {
                        const segRes = await fetch(this._refreshTokenInUrl(seg.url));
                        if (segRes.ok) {
                            const buf = await segRes.arrayBuffer();
                            this.recordHttpDownload(seg.key, buf, '');
                        }
                    }
                }
            } catch (_) {
            } finally {
                this.pausedPrefetchInFlight = false;
            }
        }

        getActiveDvrSummary() {
            const tl = this.dvrTimelines.get(this.activeMediaVariantKey);
            const timelineSec = tl ? Math.round(tl.totalDuration || 0) : 0;
            const cacheStats = this.dvrCache.getChannelStats(this.channelId);
            return {
                timelineSec,
                segmentCount: cacheStats.count,
                bytes: cacheStats.bytes
            };
        }

        /**
         * Returns an hls.js compatible playlist loader (pLoader) that stitches incoming live .m3u8
         * updates onto the client's 1-hour rolling DVR timeline.
         */
        createHlsPlaylistLoaderClass(BaseLoader) {
            const engine = this;
            return class P2PDvrPlaylistLoader extends BaseLoader {
                constructor(config) {
                    super(config);
                    this._aborted = false;
                }

                abort() {
                    this._aborted = true;
                    super.abort();
                }

                destroy() {
                    this._aborted = true;
                    super.destroy();
                }

                load(context, config, callbacks) {
                    if (engine.destroyed || !context || typeof context.url !== 'string') {
                        return super.load(context, config, callbacks);
                    }
                    const reqUrl = context.url;
                    const wrappedCallbacks = Object.assign({}, callbacks, {
                        onSuccess: (response, stats, ctx, networkDetails) => {
                            if (this._aborted) return;
                            if (response && typeof response.data === 'string' && response.data.includes('#EXTINF:')) {
                                try {
                                    const merged = engine.mergeMediaPlaylist(reqUrl, response.data);
                                    if (merged.isMediaPlaylist) {
                                        response = Object.assign({}, response, { data: merged.text });
                                    }
                                } catch (_) {}
                            }
                            callbacks.onSuccess(response, stats, ctx, networkDetails);
                        }
                    });
                    super.load(context, config, wrappedCallbacks);
                }
            };
        }

        /**
         * Returns an hls.js compatible custom fragment loader (fLoader) that serves .ts segments
         * from Client RAM -> Client IndexedDB (1h DVR) -> WebRTC P2P Peers -> HTTP server,
         * and shares newly downloaded .ts files back to the swarm.
         */
        createHlsLoaderClass(BaseLoader) {
            const engine = this;
            return class P2PHlsFragmentLoader extends BaseLoader {
                constructor(config) {
                    super(config);
                    this._aborted = false;
                    this._raceTimer = null;
                }

                abort() {
                    this._aborted = true;
                    if (this._raceTimer) {
                        clearTimeout(this._raceTimer);
                        this._raceTimer = null;
                    }
                    super.abort();
                }

                destroy() {
                    this._aborted = true;
                    if (this._raceTimer) {
                        clearTimeout(this._raceTimer);
                        this._raceTimer = null;
                    }
                    super.destroy();
                }

                load(context, config, callbacks) {
                    const isMediaFrag = context && (
                        Boolean(context.frag) ||
                        context.type === 'fragment' ||
                        (typeof context.url === 'string' &&
                         (context.url.includes('/seg?') || context.url.includes('/liveseg/')) &&
                         !context.url.includes('.m3u8'))
                    );

                    if (!isMediaFrag || engine.destroyed) {
                        return super.load(context, config, callbacks);
                    }

                    const key = engine.extractCanonicalSegmentKey(context.url);
                    const tStart = performance.now();
                    const emitBufSuccess = (buf) => {
                        const tNow = performance.now();
                        const stats = {
                            trequest: tStart,
                            tfirst: tStart + 5,
                            tload: tNow,
                            tparsed: tNow,
                            loaded: buf.byteLength,
                            total: buf.byteLength,
                            chunkCount: 1,
                            bwEstimate: 0,
                            loading: { start: tStart, first: tStart + 5, end: tNow },
                            parsing: { start: tNow, end: tNow },
                            buffering: { start: tNow, end: tNow }
                        };
                        callbacks.onSuccess({ url: context.url, data: buf.slice(0) }, stats, context, null);
                    };

                    // 1. Instant hot RAM cache hit
                    const ramCached = engine.chunkCache.get(key);
                    if (ramCached && ramCached.buf && ramCached.buf.byteLength > 0) {
                        engine.p2pBytesIn += ramCached.size;
                        engine._emitStats();
                        emitBufSuccess(ramCached.buf);
                        return;
                    }

                    let settled = false;
                    let httpStarted = false;

                    const startHttpLoad = () => {
                        if (httpStarted || settled || this._aborted) return;
                        httpStarted = true;
                        const wrappedCallbacks = Object.assign({}, callbacks, {
                            onSuccess: (response, stats, ctx, networkDetails) => {
                                if (settled || this._aborted) return;
                                settled = true;
                                if (this._raceTimer) {
                                    clearTimeout(this._raceTimer);
                                    this._raceTimer = null;
                                }
                                if (response && response.data) {
                                    let copyBuf = null;
                                    if (response.data instanceof ArrayBuffer && response.data.byteLength > 0) {
                                        copyBuf = response.data.slice(0);
                                    } else if (ArrayBuffer.isView(response.data) && response.data.byteLength > 0) {
                                        copyBuf = response.data.buffer.slice(response.data.byteOffset, response.data.byteOffset + response.data.byteLength);
                                    }
                                    if (copyBuf) {
                                        engine.recordHttpDownload(key, copyBuf, '');
                                    }
                                }
                                callbacks.onSuccess(response, stats, ctx, networkDetails);
                            },
                            onError: (err, ctx, networkDetails, stats) => {
                                if (settled || this._aborted) return;
                                settled = true;
                                if (this._raceTimer) {
                                    clearTimeout(this._raceTimer);
                                    this._raceTimer = null;
                                }
                                if (callbacks.onError) callbacks.onError(err, ctx, networkDetails, stats);
                            },
                            onTimeout: (stats, ctx, networkDetails) => {
                                if (settled || this._aborted) return;
                                settled = true;
                                if (this._raceTimer) {
                                    clearTimeout(this._raceTimer);
                                    this._raceTimer = null;
                                }
                                if (callbacks.onTimeout) callbacks.onTimeout(stats, ctx, networkDetails);
                            }
                        });
                        super.load(context, config, wrappedCallbacks);
                    };

                    const beginPeerOrHttp = () => {
                        if (settled || this._aborted) return;
                        // If no open peer announces this segment, launch HTTP immediately (0ms penalty)
                        if (!engine.hasOpenPeerForKey(key)) {
                            startHttpLoad();
                            return;
                        }
                        // 350ms Parallel P2P-HTTP Race: P2P gets a 350ms head start; HTTP runs in parallel if not delivered yet
                        this._raceTimer = setTimeout(() => {
                            this._raceTimer = null;
                            startHttpLoad();
                        }, 350);

                        engine.tryFetchFromPeers(key, '').then(buf => {
                            if (this._raceTimer) {
                                clearTimeout(this._raceTimer);
                                this._raceTimer = null;
                            }
                            if (settled || this._aborted) return;
                            if (buf && buf.byteLength > 0) {
                                settled = true;
                                if (httpStarted) {
                                    try { super.abort(); } catch (_) {}
                                }
                                emitBufSuccess(buf);
                                return;
                            }
                            startHttpLoad();
                        }).catch(() => {
                            if (this._raceTimer) {
                                clearTimeout(this._raceTimer);
                                this._raceTimer = null;
                            }
                            startHttpLoad();
                        });
                    };

                    // 2. Check local IndexedDB 1h DVR cache first (~1-3ms) before starting network race
                    if (engine.dvrCache.has(key)) {
                        engine.dvrCache.get(key).then(localStored => {
                            if (settled || this._aborted) return;
                            if (localStored && localStored.buf && localStored.buf.byteLength > 0) {
                                settled = true;
                                engine._storeInHotRam(key, localStored.buf, localStored.sha256 || '');
                                engine._emitStats();
                                emitBufSuccess(localStored.buf);
                                return;
                            }
                            beginPeerOrHttp();
                        }).catch(() => beginPeerOrHttp());
                    } else {
                        beginPeerOrHttp();
                    }
                }
            };
        }

        async _pollWebRTCAndEmitStats() {
            if (this.destroyed) return;
            for (const peerObj of this.peers.values()) {
                if (!peerObj.pc || peerObj.pc.connectionState !== 'connected') continue;
                try {
                    const report = await peerObj.pc.getStats();
                    report.forEach(stat => {
                        if (stat.type === 'candidate-pair' && stat.state === 'succeeded' && stat.availableOutgoingBitrate) {
                            this.quota.updateEstimatedBandwidthBits(stat.availableOutgoingBitrate);
                        } else if (stat.type === 'remote-candidate' && isTailscaleIP(stat.address || stat.ip)) {
                            peerObj.isTailscale = true;
                            this.latestPeerTailscaleMap.set(peerObj.id, true);
                        }
                    });
                } catch (_) {}
            }
            this._emitStats();
        }

        _sendStatsReportToTracker() {
            if (this.destroyed) return;
            const q = this.quota.getStats();
            this._sendWS({
                type: 'P2P_STATS',
                stats: {
                    p2p_bytes_in: Math.round(this.p2pBytesIn),
                    http_bytes_in: Math.round(this.httpBytesIn),
                    p2p_out_free_bytes: Math.round(q.outFreeBytes),
                    p2p_out_paid_bytes: Math.round(q.outPaidBytes)
                }
            });
        }

        _emitStats() {
            const q = this.quota.getStats();
            const totalIn = this.p2pBytesIn + this.httpBytesIn;
            const offloadPct = totalIn > 0 ? Math.round((this.p2pBytesIn / totalIn) * 100) : 0;
            this.onStats({
                channelId: this.channelId,
                selfId: this.selfId,
                selfTier: this.selfTier,
                totalSwarmPeers: this.totalSwarmPeers,
                openP2PPeers: this.getOpenPeerCount(),
                p2pBytesIn: this.p2pBytesIn,
                httpBytesIn: this.httpBytesIn,
                offloadPct,
                dvr: this.getActiveDvrSummary(),
                quota: q
            });
        }

        destroy() {
            this.destroyed = true;
            this.stopPausedDvrPrefetch();
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            if (this._statsTimer) clearInterval(this._statsTimer);
            if (this._wsReportTimer) clearInterval(this._wsReportTimer);

            for (const pending of this.pendingDownloads.values()) {
                try { pending.reject(new Error('engine_destroyed')); } catch (_) {}
            }
            this.pendingDownloads.clear();

            for (const pid of Array.from(this.peers.keys())) {
                this._closePeer(pid);
            }
            this.chunkCache.clear();
            this.cacheBytes = 0;

            if (this.ws) {
                this.ws.onclose = null;
                try { this.ws.close(); } catch (_) {}
                this.ws = null;
            }
        }
    }

    global.IPTVP2PEngine = IPTVP2PEngine;
    global.getIPTVUploadQuota = getGlobalQuota;
})(window);
