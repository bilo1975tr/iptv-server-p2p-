(function (global) {
    'use strict';

    const FRAME_HEADER_BYTES = 12;
    const FRAME_PAYLOAD_SIZE = 16384; // 16 KB safe SCTP DataChannel frame
    const MAX_DC_BUFFERED = 1500000;  // 1.5 MB backpressure ceiling

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

    /**
     * Global browser-wide Upload Quota Coordinator:
     * Enforces 10% of upload capacity for Free+Guest peers and 10% for Paid peers (20% total max).
     */
    class UploadQuotaCoordinator {
        constructor() {
            this.mode = 'auto'; // 'auto' or numeric Mbps
            this.estimatedUploadBps = 2500000; // 20 Mbps = 2,500,000 bytes/sec initial estimate
            this.freeGuestBucket = 250000;
            this.paidBucket = 250000;
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
            const bytesPerSec = Math.max(250000, Math.min(62500000, bitsPerSec / 8)); // clamp 2 Mbps .. 500 Mbps
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
                freeGuestMaxBps: cap * 0.10, // 10% for Free + Guest
                paidMaxBps: cap * 0.10,      // 10% for Paid
                totalMaxBps: cap * 0.20      // 20% combined cap
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
                if (this.paidQueue.length > 24) this.paidQueue.shift();
                this.paidQueue.push(job);
            } else {
                if (this.freeGuestQueue.length > 24) this.freeGuestQueue.shift();
                this.freeGuestQueue.push(job);
            }
        }

        _tick() {
            const now = performance.now();
            const dt = Math.min(0.25, Math.max(0.001, (now - this.lastTickMs) / 1000));
            this.lastTickMs = now;

            const limits = this.getPoolLimits();
            const freeMaxBurst = Math.max(FRAME_PAYLOAD_SIZE * 4, limits.freeGuestMaxBps * 1.5);
            const paidMaxBurst = Math.max(FRAME_PAYLOAD_SIZE * 4, limits.paidMaxBps * 1.5);

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

            while (queue.length > 0 && guard < 16) {
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
     * WebRTC DataChannel P2P Engine per Channel:
     * Connects to /ws/signaling, builds WebRTC DataChannel mesh, caches & verifies chunks,
     * fetches segments P2P-first with instant HTTP fallback, and enforces %10 Free/Guest + %10 Paid upload quotas.
     */
    class IPTVP2PEngine {
        constructor(options) {
            this.channelId = options.channelId;
            this.getToken = options.getToken || (() => '');
            this.maxPeers = options.maxPeers || 8;
            this.onStatus = options.onStatus || (() => {});
            this.onStats = options.onStats || (() => {});

            this.quota = getGlobalQuota();
            this.selfId = '';
            this.selfTier = 'guest';
            this.ws = null;
            this.destroyed = false;
            this.reconnectAttempt = 0;
            this.reconnectTimer = null;

            this.peers = new Map(); // remotePeerId -> { id, tier, pc, dc, remoteHave: Map<key,{sz,h}>, pendingIce: [], inFlightReqs: 0, failCount: 0 }
            this.latestPeerTierMap = new Map();
            this.chunkCache = new Map(); // key -> { buf, sha256, ts, size }
            this.cacheBytes = 0;
            this.maxCacheEntries = 45;
            this.maxCacheBytes = 64 * 1024 * 1024;

            this.nextReqId = 1;
            this.pendingDownloads = new Map(); // reqId -> { key, expectedHash, frames, receivedCount, totalFrames, resolve, reject, firstByteTimer, fullTimer }
            this.inFlightKeys = new Map(); // key -> Promise<ArrayBuffer>

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
                    activeMap.set(p.id, { id: p.id, tier: norm });
                    this.latestPeerTierMap.set(p.id, norm);
                }
            });

            // Remove departed peers
            for (const [pid, peerObj] of this.peers.entries()) {
                if (!activeMap.has(pid)) {
                    this._closePeer(pid);
                } else {
                    peerObj.tier = activeMap.get(pid).tier;
                }
            }

            if (typeof global.RTCPeerConnection === 'undefined' || !this.selfId) {
                return;
            }

            // Connect to up to maxPeers peers
            for (const [pid, info] of activeMap.entries()) {
                if (this.peers.size >= this.maxPeers) break;
                if (this.peers.has(pid)) continue;
                const initiator = this.selfId < pid;
                if (initiator) {
                    this._createPeerConnection(pid, info.tier, true);
                }
            }
        }

        _createPeerConnection(remoteId, remoteTier, isInitiator) {
            if (this.destroyed || typeof global.RTCPeerConnection === 'undefined') return null;
            if (this.peers.has(remoteId)) return this.peers.get(remoteId);

            const pc = new RTCPeerConnection({ iceServers: this.iceServers });
            const peerObj = {
                id: remoteId,
                tier: normalizePeerTier(remoteTier),
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
                    peerObj = this._createPeerConnection(remoteId, verifiedTier, false);
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
                // Announce existing cached chunks to newly connected peer
                for (const [k, entry] of this.chunkCache.entries()) {
                    try {
                        dc.send(JSON.stringify({ t: 'HAVE', k, sz: entry.size, h: entry.sha256 || '' }));
                    } catch (_) {
                        break;
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

        _handleDCControl(peerObj, text) {
            try {
                const msg = JSON.parse(text);
                if (msg.t === 'HAVE' && msg.k) {
                    peerObj.remoteHave.set(msg.k, { sz: Number(msg.sz || 0), h: String(msg.h || '') });
                    if (peerObj.remoteHave.size > 90) {
                        const firstKey = peerObj.remoteHave.keys().next().value;
                        peerObj.remoteHave.delete(firstKey);
                    }
                } else if (msg.t === 'REQ' && msg.id && msg.k) {
                    const entry = this.chunkCache.get(msg.k);
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
            if (totalFrames === 0 || totalFrames > 2048 || frameIdx >= totalFrames) return;

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

        cacheAndAnnounce(key, arrayBuffer, knownSha256) {
            if (this.destroyed || !key || !arrayBuffer || arrayBuffer.byteLength === 0) return;
            const size = arrayBuffer.byteLength;
            if (size > 16 * 1024 * 1024) return; // skip unexpectedly huge blobs

            const store = (shaHex) => {
                if (this.destroyed) return;
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
            candidates.sort((a, b) => (a.peer.inFlightReqs - b.peer.inFlightReqs) || (a.peer.failCount - b.peer.failCount));
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

                pending.firstByteTimer = setTimeout(() => pending.reject(new Error('first_byte_timeout')), 950);
                pending.fullTimer = setTimeout(() => pending.reject(new Error('transfer_timeout')), 4000);

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

        /**
         * Returns an hls.js compatible custom loader class that serves media fragments via WebRTC P2P first,
         * falling back to the HTTP server and sharing newly downloaded segments back to the swarm.
         */
        createHlsLoaderClass(BaseLoader) {
            const engine = this;
            return class P2PHlsFragmentLoader extends BaseLoader {
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
                    const isMediaFrag = context && (
                        context.type === 'fragment' ||
                        (typeof context.url === 'string' && context.url.includes('/seg?') && !context.url.includes('.m3u8'))
                    );

                    if (!isMediaFrag || engine.destroyed) {
                        return super.load(context, config, callbacks);
                    }

                    const key = engine.extractCanonicalSegmentKey(context.url);
                    const tStart = performance.now();

                    engine.tryFetchFromPeers(key, '').then(buf => {
                        if (this._aborted) return;
                        if (buf && buf.byteLength > 0) {
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
                            callbacks.onSuccess({ url: context.url, data: buf }, stats, context, null);
                            return;
                        }

                        // HTTP fallback + cache & share to swarm
                        const wrappedCallbacks = Object.assign({}, callbacks, {
                            onSuccess: (response, stats, ctx, networkDetails) => {
                                if (response && response.data instanceof ArrayBuffer) {
                                    engine.recordHttpDownload(key, response.data, '');
                                }
                                callbacks.onSuccess(response, stats, ctx, networkDetails);
                            }
                        });
                        super.load(context, config, wrappedCallbacks);
                    }).catch(() => {
                        if (!this._aborted) {
                            super.load(context, config, callbacks);
                        }
                    });
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
                quota: q
            });
        }

        destroy() {
            this.destroyed = true;
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
