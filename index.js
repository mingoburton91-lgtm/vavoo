const { Command } = require('commander');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const express = require('express');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { createEpgService } = require('./epg');

const NodeCache = require('node-cache');

const program = new Command();

program
    .name('vavoo-iptv-stream-proxy')
    .description('Local proxy for Vavoo IPTV streams')
    .option('--http-host <host>', 'Local HTTP host for displayed URLs', '127.0.0.1')
    .option('--http-port <port>', 'Local HTTP port', '8888')
    .option('--vavoo-language <language>', 'Language sent to Vavoo APIs, e.g. de or optional en', 'de')
    .option('--vavoo-region <region>', 'Region sent to Vavoo APIs, default US for a broad catalog, optional DE which tends to prefilter strongly toward Germany', 'US')
    .option('--vavoo-url-list <selection>', 'URL list to use: primary, fallback, both', 'both')
    .option('--redirect', 'Redirect VAVOO user agents directly to resolved upstream URLs instead of proxying them', false)
    .parse(process.argv);

const options = program.opts();

function getBaseSites(selection) {
    const normalized = String(selection || 'both').trim().toLowerCase();

    if (normalized === 'primary') {
        return ['https://vavoo.to'];
    }

    if (normalized === 'fallback') {
        return ['https://kool.to'];
    }

    return ['https://vavoo.to', 'https://kool.to'];
}

const app = express();
const httpHost = process.env.HOST || options.httpHost;
const port = Number(process.env.PORT || options.httpPort);
const currentLanguage = options.vavooLanguage;
const currentRegion = options.vavooRegion;
const vavooUrlList = options.vavooUrlList;
const redirect = Boolean(options.redirect);
const baseSites = getBaseSites(vavooUrlList);

const cache = new NodeCache();

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const CHANNELS_CACHE_KEY = 'vavoo_channels';
const SIGNATURE_CACHE_KEY = 'vavoo_addon_sig';
const COUNTRY_SEPARATORS = ['➾', '⟾', '->', '→', '»', '›'];
const PING_URLS = [
    'https://www.vavoo.tv/api/app/ping'
];

function getLocalBaseUrl() {
    return `http://${httpHost}:${port}`;
}

function buildHomePage() {
    const baseUrl = getLocalBaseUrl();
    const allM3u = `${baseUrl}/channels.m3u8`;
    const germanyM3u = `${baseUrl}/channels.m3u8?country=Germany`;
    const italyM3u = `${baseUrl}/channels.m3u8?country=Italy`;
    const franceM3u = `${baseUrl}/channels.m3u8?country=France`;
    const spainM3u = `${baseUrl}/channels.m3u8?country=Spain`;
    const ukM3u = `${baseUrl}/channels.m3u8?country=${encodeURIComponent('United Kingdom')}`;
    const countriesUrl = `${baseUrl}/countries`;

    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Vavoo Proxy</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #111111;
      --text: #f3f3f3;
      --muted: #b8b8b8;
      --link: #8fd3ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    main {
      max-width: 760px;
      margin: 0 auto;
      padding: 24px 18px 40px;
    }
    h1 {
      margin: 0 0 10px;
      font-size: 28px;
    }
    p {
      margin: 0 0 18px;
      color: var(--muted);
    }
    ul {
      margin: 0;
      padding-left: 20px;
    }
    li { margin: 10px 0; }
    a {
      color: var(--link);
      word-break: break-all;
    }
    code {
      color: var(--text);
    }
  </style>
</head>
<body>
  <main>
    <h1>Vavoo Proxy</h1>
    <p>Local entry points for playlists and stream playback.</p>
    <ul>
      <li><a href="${baseUrl}/">${baseUrl}/</a></li>
      <li><a href="${allM3u}">${allM3u}</a></li>
      <li><a href="${germanyM3u}">${germanyM3u}</a></li>
      <li><a href="${italyM3u}">${italyM3u}</a></li>
      <li><a href="${franceM3u}">${franceM3u}</a></li>
      <li><a href="${spainM3u}">${spainM3u}</a></li>
      <li><a href="${ukM3u}">${ukM3u}</a></li>
      <li><a href="${countriesUrl}">${countriesUrl}</a></li>
    </ul>
  </main>
</body>
</html>`;
}

function normalize(value) {
    return String(value || '').trim().toLowerCase();
}

function normalizeChannelIdPart(value) {
    return normalize(value).replace(/\s+/g, ' ');
}

function getStableChannelId(name, country) {
    const seed = [
        normalizeChannelIdPart(country),
        normalizeChannelIdPart(name)
    ].join('|');

    return crypto.createHash('sha1').update(seed).digest('hex').slice(0, 22);
}

function extractCountry(group) {
    const rawGroup = String(group || '').trim();
    if (!rawGroup) {
        return 'default';
    }

    for (const separator of COUNTRY_SEPARATORS) {
        if (rawGroup.includes(separator)) {
            return rawGroup.split(separator)[0].trim() || 'default';
        }
    }

    return rawGroup;
}

function getCatalogHeaders(signature) {
    return {
        'content-type': 'application/json; charset=utf-8',
        'mediahubmx-signature': signature,
        'user-agent': 'MediaHubMX/2',
        'accept': '*/*',
        'Accept-Language': currentLanguage,
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'close',
    };
}

/**
 * Builds upstream playback headers and forwards byte ranges.
 * Example: Range `bytes=0-1023` is passed through to HLS segments.
 */
function getStreamHeaders(req) {
    const headers = {
        'User-Agent': 'VAVOO/2.6',
        'Connection': 'close'
    };

    if (req.headers.range) {
        headers.Range = req.headers.range;
    }

    return headers;
}

/**
 * Wraps an upstream HLS URL with the local HLS proxy endpoint.
 * Example: `https://host/live.m3u8` -> `/hls-proxy?url=...`.
 */
function getProxiedUpstreamUrl(req, upstreamUrl) {
    return `${req.protocol}://${req.headers.host}/hls-proxy?url=${encodeURIComponent(upstreamUrl)}`;
}

/**
 * Sends HLS playlists as uncached M3U8 responses.
 */
function setPlaylistHeaders(res) {
    res.type('application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
}

/**
 * Returns a stable local master playlist for an upstream media playlist.
 * Example body contains one `#EXT-X-STREAM-INF` entry pointing to `/hls-proxy`.
 */
function sendHlsMasterPlaylist(req, res, streamUrl) {
    setPlaylistHeaders(res);
    res.send([
        '#EXTM3U',
        '#EXT-X-VERSION:3',
        '#EXT-X-STREAM-INF:BANDWIDTH=8000000',
        getProxiedUpstreamUrl(req, streamUrl)
    ].join('\n') + '\n');
}

/**
 * Checks whether a URL path points at an M3U8 playlist.
 * Example: `/hls/index.m3u8` returns true.
 */
function isM3u8Url(upstreamUrl) {
    return new URL(upstreamUrl).pathname.toLowerCase().endsWith('.m3u8');
}

/**
 * Detects HLS playlists by content type or URL suffix.
 */
function isM3u8Response(upstreamUrl, contentType) {
    return String(contentType || '').toLowerCase().includes('mpegurl')
        || String(contentType || '').toLowerCase().includes('application/vnd.apple')
        || isM3u8Url(upstreamUrl);
}

/**
 * Keeps non-fetchable playlist URIs untouched.
 * Example: `skd://key-id` is not rewritten.
 */
function shouldRewritePlaylistUri(uri) {
    const trimmed = String(uri || '').trim();
    if (!trimmed) {
        return false;
    }

    return !/^(data|urn|skd):/i.test(trimmed);
}

/**
 * Resolves a playlist URI relative to its source and proxies it locally.
 * Example: `seg.ts` under `https://h/live/index.m3u8` becomes `/hls-proxy?.../live/seg.ts`.
 */
function rewritePlaylistUri(req, baseUrl, uri) {
    if (!shouldRewritePlaylistUri(uri)) {
        return uri;
    }

    return getProxiedUpstreamUrl(req, new URL(uri, baseUrl).toString());
}

/**
 * Rewrites HLS media, variant, key, and map URLs to local proxy URLs.
 * Example: segment lines and `URI="key.bin"` attributes are rewritten.
 */
function rewriteM3u8Playlist(req, upstreamUrl, playlist) {
    return String(playlist)
        .split(/\r?\n/)
        .map(function (line) {
            const trimmed = line.trim();

            if (!trimmed) {
                return line;
            }

            if (trimmed.startsWith('#')) {
                return line.replace(/URI="([^"]+)"/g, function (match, uri) {
                    return `URI="${rewritePlaylistUri(req, upstreamUrl, uri)}"`;
                });
            }

            return rewritePlaylistUri(req, upstreamUrl, trimmed);
        })
        .join('\n');
}

/**
 * Extracts compact debug details from an HLS playlist.
 * Example: `#EXT-X-MEDIA-SEQUENCE:42` returns sequence `42`.
 */
function getPlaylistDebugInfo(playlist) {
    const lines = String(playlist).split(/\r?\n/);
    const sequenceLine = lines.find((line) => line.startsWith('#EXT-X-MEDIA-SEQUENCE:'));
    const sequence = sequenceLine ? sequenceLine.split(':')[1] : 'n/a';
    const segments = lines.filter((line) => line.trim() && !line.trim().startsWith('#')).length;

    return { sequence, segments };
}

/**
 * Shortens an upstream URL for readable debug logs.
 * Example: `https://a.test/x/y.ts?token=...` -> `a.test/x/y.ts`.
 */
function describeUpstreamUrl(upstreamUrl) {
    const url = new URL(upstreamUrl);
    return `${url.hostname}${url.pathname}`;
}

function setUpstreamHeaders(res, upstream) {
    const contentType = upstream.headers.get('content-type');
    if (contentType) {
        res.setHeader('Content-Type', contentType);
    }

    const contentLength = upstream.headers.get('content-length');
    if (contentLength) {
        res.setHeader('Content-Length', contentLength);
    }

    const acceptRanges = upstream.headers.get('accept-ranges');
    if (acceptRanges) {
        res.setHeader('Accept-Ranges', acceptRanges);
    }

    const contentRange = upstream.headers.get('content-range');
    if (contentRange) {
        res.setHeader('Content-Range', contentRange);
    }
}

function getPingPayload() {
    const currentTimestamp = Date.now();

    return {
        reason: 'app-focus',
        locale: currentLanguage,
        theme: 'dark',
        metadata: {
            device: {
                type: 'desktop',
                uniqueId: `node-${currentTimestamp}`
            },
            os: {
                name: 'linux',
                version: 'Linux',
                abis: ['x64'],
                host: 'node'
            },
            app: {
                platform: 'electron'
            },
            version: {
                package: 'tv.vavoo.app',
                binary: '3.1.8',
                js: '3.1.8'
            }
        },
        appFocusTime: 0,
        playerActive: false,
        playDuration: 0,
        devMode: false,
        hasAddon: true,
        castConnected: false,
        package: 'tv.vavoo.app',
        version: '3.1.8',
        process: 'app',
        firstAppStart: currentTimestamp,
        lastAppStart: currentTimestamp,
        ipLocation: null,
        adblockEnabled: true,
        proxy: {
            supported: ['ss'],
            engine: 'Mu',
            enabled: false,
            autoServer: true
        },
        iap: {
            supported: false
        }
    };
}

async function requestJson(options) {
    const response = await fetch(options.url, {
        method: options.method || 'GET',
        headers: options.headers,
        body: options.body ? JSON.stringify(options.body) : undefined,
        signal: AbortSignal.timeout(options.timeout || 30000),
    });

    const body = await response.json();

    if (!response.ok) {
        const error = new Error(`HTTP ${response.status} for ${options.url}`);
        error.statusCode = response.status;
        error.body = body;
        throw error;
    }

    return body;
}

async function getAddonSignature() {
    const cached = cache.get(SIGNATURE_CACHE_KEY);
    if (cached) {
        return cached;
    }

    const payload = getPingPayload();

    for (const url of PING_URLS) {
        try {
            const body = await requestJson({
                method: 'POST',
                url,
                body: payload,
            });

            const signature = body?.addonSig;
            if (signature) {
                cache.set(SIGNATURE_CACHE_KEY, signature, 300);
                return signature;
            }
        } catch (error) {
            console.log(`[vavoo] addonSig request failed for ${url}: ${error.message}`);
        }
    }

    throw new Error('Unable to obtain addonSig');
}

function mapCatalogItem(item, sourceBase) {
    const name = item.name || 'Unknown Channel';
    const country = extractCountry(item.group);

    return {
        id: getStableChannelId(name, country),
        url: item.url,
        name,
        logo: item.logo || '',
        group: item.group || '',
        country,
        sourceBase
    };
}

async function loadCatalogFromBase(baseUrl, signature) {
    const catalogUrl = `${baseUrl.replace(/\/$/, '')}/mediahubmx-catalog.json`;
    const headers = getCatalogHeaders(signature);
    const channels = [];
    let cursor = null;

    while (true) {
        const body = await requestJson({
            method: 'POST',
            url: catalogUrl,
            headers,
            body: {
                language: currentLanguage,
                region: currentRegion,
                catalogId: 'iptv',
                id: 'iptv',
                adult: false,
                search: '',
                sort: '',
                filter: {},
                cursor,
                clientVersion: '3.0.2'
            }
        });

        const items = Array.isArray(body?.items) ? body.items : [];
        for (const item of items) {
            if (item?.type === 'iptv' && item?.url) {
                channels.push(mapCatalogItem(item, baseUrl));
            }
        }

        if (!body?.nextCursor) {
            break;
        }

        cursor = body.nextCursor;
    }
    return channels;
}

let lastLoadedChannels = [];
let channelsLoadPromise = null;

async function loadChannelsFresh() {
    const signature = await getAddonSignature();
    const merged = new Map();
    let successfulSources = 0;

    for (const baseUrl of baseSites) {
        try {
            const channels = await loadCatalogFromBase(baseUrl, signature);
            successfulSources += 1;
            for (const channel of channels) {
                if (!merged.has(channel.id)) {
                    merged.set(channel.id, {
                        ...channel,
                        candidates: [{ url: channel.url, sourceBase: channel.sourceBase }]
                    });
                    continue;
                }

                const existing = merged.get(channel.id);
                const duplicate = existing.candidates.some(candidate =>
                    candidate.url === channel.url && candidate.sourceBase === channel.sourceBase
                );
                if (!duplicate) {
                    existing.candidates.push({ url: channel.url, sourceBase: channel.sourceBase });
                }
            }
            console.log(`[vavoo] channels loaded from ${baseUrl}: ${channels.length}`);
        } catch (error) {
            console.log(`[vavoo] catalog load failed for ${baseUrl}: ${error.message}`);
        }
    }

    if (!successfulSources) {
        throw new Error('Unable to load channel catalog');
    }

    const channels = [...merged.values()];

    for (const channel of channels) {
        if (normalize(channel.country) === 'italy' && normalize(channel.name).includes('sky cinema uno')) {
            const candidates = Array.isArray(channel.candidates) ? channel.candidates : [];
            const summary = candidates.map((candidate, index) => ({
                index,
                source: candidate.sourceBase,
                urlHash: crypto.createHash('sha1').update(String(candidate.url)).digest('hex').slice(0, 10)
            }));
            console.log(`[vavoo] diagnostic "${channel.name}" candidates=${JSON.stringify(summary)}`);
        }
    }

    cache.set(CHANNELS_CACHE_KEY, channels, 3600);
    lastLoadedChannels = channels;
    const countryCounts = channels.reduce((counts, channel) => {
        const country = channel.country || 'default';
        counts[country] = (counts[country] || 0) + 1;
        return counts;
    }, {});
    console.log(`[vavoo] merged catalog: ${channels.length} channels from ${successfulSources} source(s)`);
    console.log(`[vavoo] countries: ${JSON.stringify(countryCounts)}`);
    return channels;
}

function loadChannelsOnce() {
    if (!channelsLoadPromise) {
        channelsLoadPromise = loadChannelsFresh().finally(() => {
            channelsLoadPromise = null;
        });
    }
    return channelsLoadPromise;
}

async function getChannels(forceRefresh = false) {
    if (forceRefresh) {
        cache.del(CHANNELS_CACHE_KEY);
        return loadChannelsOnce();
    }

    const cached = cache.get(CHANNELS_CACHE_KEY);
    if (cached) {
        return cached;
    }

    if (lastLoadedChannels.length) {
        loadChannelsOnce().catch(error => {
            console.log(`[vavoo] background catalog refresh failed: ${error.message}`);
        });
        return lastLoadedChannels;
    }

    return loadChannelsOnce();
}

async function getChannelsByCountry(country) {
    const channels = await getChannels();
    return channels.filter((channel) => normalize(channel.country) === normalize(country));
}

async function getCountries() {
    const channels = await getChannels();
    return [...new Set(
        channels
            .map((channel) => channel.country)
            .filter((country) => country && normalize(country) !== 'default')
    )].sort((left, right) => left.localeCompare(right));
}

async function findChannelById(id) {
    const channels = await getChannels();
    return channels.find((channel) => String(channel.id) === String(id));
}

/**
 * Strips player pipe options accidentally sent as part of the path.
 * Example: `123|User-Agent=VAVOO/2.6` -> `123`.
 */
function normalizeStreamId(id) {
    return String(id || '')
        .split('|')[0]
        .replace(/\.m3u8$/i, '');
}

async function resolveStreamUrl(channel) {
    const signature = await getAddonSignature();
    const preferredKey = `resolve_base_${channel.id}`;
    const preferredBase = cache.get(preferredKey);

    const rawCandidates = Array.isArray(channel.candidates) && channel.candidates.length
        ? channel.candidates
        : baseSites.map(sourceBase => ({ sourceBase, url: channel.url }));

    const orderedCandidates = preferredBase
        ? [
            ...rawCandidates.filter(candidate => candidate.sourceBase === preferredBase),
            ...rawCandidates.filter(candidate => candidate.sourceBase !== preferredBase)
        ]
        : rawCandidates;

    for (const candidate of orderedCandidates) {
        const baseUrl = candidate.sourceBase;
        const resolveUrl = `${baseUrl.replace(/\/$/, '')}/mediahubmx-resolve.json`;

        try {
            const body = await requestJson({
                method: 'POST',
                url: resolveUrl,
                headers: getCatalogHeaders(signature),
                body: {
                    language: currentLanguage,
                    region: currentRegion,
                    url: candidate.url,
                    clientVersion: '3.0.2'
                },
                timeout: 7000
            });

            const streamUrl = Array.isArray(body) && body[0]?.url
                ? body[0].url
                : (body?.url || body?.streamUrl);

            if (streamUrl) {
                cache.set(preferredKey, baseUrl, 900);
                return streamUrl;
            }

            console.log(`[vavoo] resolve returned no stream URL for ${channel.name} on ${baseUrl}`);
        } catch (error) {
            console.log(`[vavoo] resolve failed for ${channel.name} on ${baseUrl}: ${error.message}`);
        }
    }

    throw new Error(`Unable to resolve stream for channel ${channel.name}`);
}

async function proxyStream(req, res, streamUrl, channelName) {
    const connId = `${req.socket.remoteAddress}`;
    const controller = new AbortController();

    req.socket.on('close', function () {
        console.log(`[${connId}] connection closed`);
        controller.abort();
    });

    try {
        const upstream = await fetch(streamUrl, {
            signal: controller.signal,
            headers: getStreamHeaders(req)
        });

        if (!upstream.ok || !upstream.body) {
            throw new Error(`upstream returned HTTP ${upstream.status}`);
        }

        const contentType = upstream.headers.get('content-type');
        if (isM3u8Response(streamUrl, contentType)) {
            const playlist = await upstream.text();
            const rewrittenPlaylist = rewriteM3u8Playlist(req, streamUrl, playlist);
            setPlaylistHeaders(res);
            res.send(rewrittenPlaylist);
            return;
        }

        setUpstreamHeaders(res, upstream);
        console.log(`[${connId}] starting stream proxy "${channelName}"`);
        await pipeline(Readable.fromWeb(upstream.body), res);
    } catch (error) {
        if (controller.signal.aborted) {
            console.log(`[${connId}] stream ended "${channelName}"`);
            return;
        }

        console.log(`[${connId}] stream error "${channelName}": ${error.message}`);
        if (!res.headersSent) {
            res.status(400).send(`stream error: ${error.message}`);
        }
    }
}

function proxyHlsRequest(req, res, upstreamUrl) {
    const connId = `${req.socket.remoteAddress}`;
    const upstreamLabel = describeUpstreamUrl(upstreamUrl);
    const parsed = new URL(upstreamUrl);
    const transport = parsed.protocol === 'https:' ? https : http;
    let settled = false;

    const upstreamReq = transport.request(parsed, {
        method: 'GET',
        headers: getStreamHeaders(req),
        timeout: 30000
    }, upstream => {
        const status = upstream.statusCode || 502;
        const contentType = upstream.headers['content-type'] || '';

        if (status < 200 || status >= 300) {
            upstream.resume();
            if (!res.headersSent) res.status(status).send(`upstream returned HTTP ${status}`);
            settled = true;
            return;
        }

        if (isM3u8Response(upstreamUrl, contentType)) {
            const chunks = [];
            upstream.on('data', chunk => chunks.push(chunk));
            upstream.on('end', () => {
                if (settled || res.destroyed) return;
                const playlist = Buffer.concat(chunks).toString('utf8');
                const rewrittenPlaylist = rewriteM3u8Playlist(req, upstreamUrl, playlist);
                const debugInfo = getPlaylistDebugInfo(playlist);
                console.log(`[${connId}] hls playlist "${upstreamLabel}" status=${status} sequence=${debugInfo.sequence} entries=${debugInfo.segments}`);
                setPlaylistHeaders(res);
                res.send(rewrittenPlaylist);
                settled = true;
            });
            upstream.on('error', error => {
                console.log(`[${connId}] hls playlist error "${upstreamLabel}": ${error.message}`);
                if (!res.headersSent) res.status(502).send('upstream playlist error');
                settled = true;
            });
            return;
        }

        // Stream binary HLS assets directly with Node's native HTTP streams.
        // This intentionally avoids fetch()/Undici, whose parser assertion was
        // terminating the whole Node process during repeated LG webOS requests.
        for (const [key, value] of Object.entries(upstream.headers)) {
            if (value !== undefined && !['connection','transfer-encoding','content-length'].includes(key.toLowerCase())) {
                try { res.setHeader(key, value); } catch (_) {}
            }
        }
        res.status(status);
        console.log(`[${connId}] hls asset "${upstreamLabel}" status=${status} type="${contentType || 'unknown'}"`);
        upstream.pipe(res);
        upstream.on('end', () => { settled = true; });
        upstream.on('error', error => {
            console.log(`[${connId}] hls asset error "${upstreamLabel}": ${error.message}`);
            if (!res.headersSent) res.status(502).send('upstream asset error');
            else res.destroy();
            settled = true;
        });
    });

    upstreamReq.on('timeout', () => upstreamReq.destroy(new Error('upstream timeout')));
    upstreamReq.on('error', error => {
        if (settled || res.destroyed) return;
        console.log(`[${connId}] hls proxy error "${upstreamLabel}": ${error.message}`);
        if (!res.headersSent) res.status(502).send(`upstream proxy error: ${error.message}`);
        settled = true;
    });

    const closeUpstream = () => {
        if (!settled) upstreamReq.destroy();
    };
    req.once('aborted', closeUpstream);
    res.once('close', closeUpstream);
    upstreamReq.end();
}

async function proxyUpstreamUrl(req, res, upstreamUrl) {
    return proxyHlsRequest(req, res, upstreamUrl);
}

app.get('/', function (req, res) {
    res.type('html').send(buildHomePage());
});

app.get('/countries', async function (req, res) {
    try {
        res.json(await getCountries());
    } catch (error) {
        console.log('[vavoo] countries error', error.message);
        res.status(500).send(error.message);
    }
});

const epgService = createEpgService();
const EPG_COUNTRY_CODES = epgService.countryCodes;

async function getEpgMap(country) {
    return epgService.getMap(country);
}

function lookupEpgId(epgMap, name, country) {
    return epgService.lookupId(epgMap, name, country);
}

app.get('/epg.xml', async function (req, res) {
    try {
        const { italy, foreign } = loadCombinedWhitelist();
        const result = await epgService.getCombinedXml(italy, foreign);
        res.type('application/xml; charset=utf-8');
        res.setHeader('Cache-Control', 'public, max-age=900');
        res.setHeader('X-EPG-Channels', String(result.summary.channels || 0));
        res.setHeader('X-EPG-Programmes', String(result.summary.programmes || 0));
        res.send(result.xml);
    } catch (error) {
        console.log('[vavoo] epg.xml error', error.message);
        res.status(502).send(error.message);
    }
});

app.get('/epg/:country.xml', function (req, res) {
    const url = epgService.countrySourceUrl(req.params.country);
    if (!url) return res.status(404).send('EPG country not configured');
    res.redirect(302, url);
});


// Editable channel-name list, ordered roughly like Italian TV/satellite lineups.
app.get('/channels.txt', async function (req, res) {
    try {
        const country = String(req.query.country || 'Italy');
        let catalog = lastLoadedChannels;
        if (!catalog.length) catalog = cache.get(CHANNELS_CACHE_KEY) || [];
        if (!catalog.length) catalog = await getChannels();
        const channels = catalog.filter(ch => normalize(ch.country) === normalize(country));
        const clean = name => String(name || '').replace(/\s*\.\s*[cs]\s*$/i, '').trim();
        const rank = name => {
            const n = normalizeEpgName(clean(name));
            const exact = [
                ['rai 1',1],['rai 2',2],['rai 3',3],['rete 4',4],['canale 5',5],['italia 1',6],
                ['la7',7],['tv8',8],['nove',9],['20 mediaset',20],['rai 4',21],['iris',22],
                ['rai 5',23],['rai movie',24],['rai premium',25],['cielo',26],['27 twentyseven',27],
                ['tv2000',28],['la7d',29],['la5',30],['real time',31],['qvc',32],['food network',33],
                ['cine34',34],['focus',35],['rtl 102 5',36],['warner tv',37],['giallo',38],
                ['top crime',39],['boing',40],['k2',41],['rai gulp',42],['rai yoyo',43],
                ['frisbee',44],['cartoonito',46],['super',47],['rai news 24',48],['italia 2',49],
                ['sky tg24',50]
            ];
            for (const [key,val] of exact) if (n === key || n.startsWith(key + ' ')) return val;
            if (/sky.*cinema|cinema.*sky/.test(n)) return 1000;
            if (/cinema|movie|film/.test(n)) return 1100;
            if (/sky.*sport|sport|dazn|eurosport/.test(n)) return 2000;
            if (/news|tg24|tgcom|rainews/.test(n)) return 3000;
            if (/cartoon|boing|gulp|yoyo|frisbee|nick|disney|super/.test(n)) return 4000;
            if (/document|discovery|history|focus|national geographic|nat geo/.test(n)) return 5000;
            return 9000;
        };
        const displayName = value => {
            const raw = String(value || '').trim();
            const suffix = raw.match(/\s*\.\s*([cs])\s*$/i);
            const base = clean(raw);
            return suffix ? `${base} [${suffix[1].toUpperCase()}]` : base;
        };
        const names = channels.map(ch => ({
            raw: String(ch.name || ''),
            base: clean(ch.name),
            display: displayName(ch.name)
        })).filter(x => x.base).sort((a,b) =>
            rank(a.base)-rank(b.base) ||
            a.base.localeCompare(b.base, 'it', {numeric:true}) ||
            a.display.localeCompare(b.display, 'it', {numeric:true})
        ).map(x => x.display);
        res.type('text/plain; charset=utf-8').send(names.join('\n') + '\n');
    } catch (error) {
        res.status(500).type('text/plain').send(error.message);
    }
});

function channelVariantName(value) {
    const raw = String(value || '').trim();
    const suffix = raw.match(/\s*\.\s*([cs])\s*$/i);
    const base = raw.replace(/\s*\.\s*[cs]\s*$/i, '').trim();
    return suffix ? `${base} [${suffix[1].toUpperCase()}]` : base;
}

function definitiveFallbackLogo(cleanName, country) {
    const name = normalize(String(cleanName || '')
        .replace(/\s*\[[cs]\]\s*$/i, '')
        .replace(/\s*\[[^\]]+\]\s*/g, ' ')
        .replace(/\b(?:hd|fhd|uhd|4k)\b/gi, ' ')
        .replace(/\s+/g, ' ')
        .trim());
    const countryKey = normalize(country);

    const tvLogoBase = 'https://raw.githubusercontent.com/tv-logo/tv-logos/main/';

    if (countryKey === 'italy') {
        if (name === 'rai 3') return tvLogoBase + 'countries/italy/rai-3-it.png';
        if (name.startsWith('sky primafila')) return tvLogoBase + 'countries/italy/sky-primafila-it.png';
        if (name.startsWith('sky sport football')) return tvLogoBase + 'countries/italy/sky-sport-football-it.png';
        if (name === 'discovery k2') return tvLogoBase + 'countries/italy/k2-it.png';
        if (name === 'nat geo') return tvLogoBase + 'countries/italy/national-geographic-it.png';
        if (name === 'rsi la 1') return tvLogoBase + 'countries/switzerland/rsi-la1-ch.png';
        if (name === 'rsi la 2') return tvLogoBase + 'countries/switzerland/rsi-la2-ch.png';
        if (name.startsWith('radio number one')) return tvLogoBase + 'countries/italy/radio-number-one-tv-it.png';
        if (name.startsWith('radio studio delta')) return tvLogoBase + 'countries/italy/radio-studio-delta-it.png';
        if (name.startsWith('radio 105')) return 'https://commons.wikimedia.org/wiki/Special:Redirect/file/Radio%20105%20italy%202023.png';
        if (name.startsWith('sky cinema stories')) return 'https://jaruba.github.io/channel-logos/export/transparent-color/eia3wASVi2KULA1hgy0gXIeSihs.png';
        if (name.startsWith('eurosport 4 timvision') || name.startsWith('eurosport 6 timvision')) {
            return 'https://jaruba.github.io/channel-logos/export/transparent-color/AfhbW2Y6X9uwoZAgoP0cOfSPoH7.png';
        }
        if (name === 'discovery science') {
            return 'https://jaruba.github.io/channel-logos/export/transparent-color/afxnsEk7jsfQlVbK0scU6gcuUeu.png';
        }
    }

    if (countryKey === 'france' && name.startsWith('canal+ live')) {
        return tvLogoBase + 'countries/france/canal-plus-sport-fr.png';
    }
    if (countryKey === 'france' && name === 'dazn 2') {
        return 'https://jaruba.github.io/channel-logos/export/transparent-color/kGipWQCpZcafdYmF0NICQmKw0uQ.png';
    }
    if (countryKey === 'germany' && name === 'sky sport mix') {
        return tvLogoBase + 'countries/germany/sky-sport/sky-sport-mix-de.png';
    }
    if (countryKey === 'germany' && (name === 'sky sports f1' || name === 'sky sport f1')) {
        return tvLogoBase + 'countries/germany/sky-sport/sky-sport-f1-de.png';
    }
    if (countryKey === 'albania' && name.startsWith('arena sport 3')) {
        return tvLogoBase + 'countries/croatia/arena-sport-3-hr.png';
    }
    if (countryKey === 'netherlands' && name.startsWith('ziggo sport')) {
        return tvLogoBase + 'countries/netherlands/ziggo-sport-nl.png';
    }

    // Final safety net for rare/local feeds without a published artwork source:
    // keep every definitive-playlist entry visually identifiable instead of blank.
    return 'https://placehold.co/512x288/111111/FFFFFF/png?text=' + encodeURIComponent(String(cleanName || 'TV'));
}

function loadCombinedWhitelist() {
    const lines = fs.readFileSync(require('node:path').join(__dirname, 'combined-whitelist.txt'), 'utf8')
        .split(/\r?\n/)
        .map(x => x.trim())
        .filter(x => x && !x.startsWith('#'));
    const italy = [];
    const foreign = new Map();
    for (const line of lines) {
        const separator = line.indexOf('|');
        if (separator === -1) {
            italy.push(line);
            continue;
        }
        const country = line.slice(0, separator).trim();
        const name = line.slice(separator + 1).trim();
        if (country && name) foreign.set(`${normalize(country)}|${normalize(name)}`, { country, name });
    }
    return { italy, foreign };
}

function loadItalyWhitelist() {
    return fs.readFileSync(require('node:path').join(__dirname, 'italy-whitelist.txt'), 'utf8')
        .split(/\r?\n/)
        .map(x => x.trim())
        .filter(Boolean);
}

// Test playlist: same existing catalog/streams, filtered only by the definitive Italy whitelist.
app.get('/italia-test.m3u8', async function (req, res) {
    try {
        const whitelist = loadItalyWhitelist();
        const wanted = new Map(whitelist.map((name, index) => [normalize(name), index]));
        const channels = await getChannelsByCountry('Italy');
        const epgMap = await getEpgMap('Italy');

        const selected = channels
            .map(channel => ({ channel, display: channelVariantName(channel.name) }))
            .filter(item => wanted.has(normalize(item.display)))
            .sort((a, b) => wanted.get(normalize(a.display)) - wanted.get(normalize(b.display)));

        const epgUrl = String(req.headers['x-forwarded-proto'] || req.protocol).split(',')[0] + '://' + req.headers.host + '/epg.xml';
        const output = ['#EXTM3U x-tvg-url="' + epgUrl + '" url-tvg="' + epgUrl + '"'];
        for (const { channel, display } of selected) {
            const cleanName = String(channel.name || '').replace(/\s*\.\s*[cs]\s*$/i, '').trim();
            const epgId = lookupEpgId(epgMap, channel.name, channel.country) || lookupEpgId(epgMap, cleanName, channel.country) || '';
            output.push(`#EXTINF:-1 tvg-name="${cleanName}" group-title="Italy" tvg-logo="${channel.logo}" tvg-id="${epgId}",${display}`);
            output.push('#EXTVLCOPT:http-user-agent=VAVOO/2.6');
            output.push('#EXTVLCOPT:no-ssl-verify');
            output.push(`${req.protocol}://${req.headers.host}/stream/${encodeURIComponent(channel.id)}.m3u8`);
        }

        setPlaylistHeaders(res);
        res.send(output.join('\n'));
    } catch (error) {
        console.log('[vavoo] italia-test.m3u8 error', error.message);
        res.status(500).send(error.message);
    }
});

// Combined test playlist: definitive filtered Italy + approved SPORT MONDO whitelist.
app.get('/lista-test.m3u8', async function (req, res) {
    try {
        const { italy, foreign } = loadCombinedWhitelist();
        const italyWanted = new Map(italy.map((name, index) => [normalize(name), index]));
        const allChannels = await getChannels();
        const italyEpgMap = await getEpgMap('Italy');
        const foreignCountries = [...new Set(allChannels.map(channel => channel.country).filter(country => EPG_COUNTRY_CODES[normalize(country)]))];
        const foreignEpgMaps = new Map();
        await Promise.all(foreignCountries.map(async country => {
            try { foreignEpgMaps.set(normalize(country), await getEpgMap(country)); }
            catch (error) { console.log(`[vavoo] EPG map unavailable for ${country}: ${error.message}`); }
        }));

        const italySelected = allChannels
            .filter(channel => normalize(channel.country) === 'italy')
            .map(channel => ({ channel, display: channelVariantName(channel.name) }))
            .filter(item => italyWanted.has(normalize(item.display)))
            .sort((a, b) => italyWanted.get(normalize(a.display)) - italyWanted.get(normalize(b.display)));

        const foreignSelected = allChannels
            .map(channel => ({ channel, cleanName: String(channel.name || '').replace(/\s*\.\s*[cs]\s*$/i, '').trim() }))
            .filter(item => foreign.has(`${normalize(item.channel.country)}|${normalize(item.cleanName)}`));

        const italyRows = italySelected.map(({ channel, display }) => {
            const cleanName = String(channel.name || '').replace(/\s*\.\s*[cs]\s*$/i, '').trim();
            const epgId = lookupEpgId(italyEpgMap, channel.name, 'Italy') || lookupEpgId(italyEpgMap, cleanName, 'Italy') || '';
            return { channel, display, cleanName, epgId, country: 'Italy' };
        });

        const foreignRows = foreignSelected.map(({ channel, cleanName }) => {
            const epgMap = foreignEpgMaps.get(normalize(channel.country)) || {};
            const epgId = lookupEpgId(epgMap, channel.name, channel.country) || lookupEpgId(epgMap, cleanName, channel.country) || '';
            return { channel, display: cleanName, cleanName, epgId, country: channel.country };
        });

        const allRows = italyRows.concat(foreignRows);
        const siblingLogos = new Map();
        for (const row of allRows) {
            if (!row.channel.logo) continue;
            siblingLogos.set(`${normalize(row.country)}|${normalize(row.cleanName)}`, row.channel.logo);
        }

        const logoRequests = new Map();
        for (const row of allRows) {
            const siblingLogo = siblingLogos.get(`${normalize(row.country)}|${normalize(row.cleanName)}`);
            if (row.channel.logo || siblingLogo || !row.epgId) continue;
            const key = normalize(row.country);
            if (!logoRequests.has(key)) logoRequests.set(key, { country: row.country, ids: new Set() });
            logoRequests.get(key).ids.add(row.epgId);
        }

        const epgLogos = new Map();
        await Promise.all(Array.from(logoRequests.entries()).map(async ([key, request]) => {
            try {
                epgLogos.set(key, await epgService.getLogos(request.country, request.ids));
            } catch (error) {
                console.log(`[vavoo] EPG logos unavailable for ${request.country}: ${error.message}`);
            }
        }));

        let siblingFallbacks = 0;
        let epgFallbacks = 0;
        let missingLogos = 0;

        function resolveLogo(row) {
            if (row.channel.logo) return row.channel.logo;

            const siblingLogo = siblingLogos.get(`${normalize(row.country)}|${normalize(row.cleanName)}`);
            if (siblingLogo) {
                siblingFallbacks += 1;
                return siblingLogo;
            }

            const countryLogos = epgLogos.get(normalize(row.country));
            const epgLogo = countryLogos && row.epgId ? countryLogos.get(row.epgId) : '';
            if (epgLogo) {
                epgFallbacks += 1;
                return epgLogo;
            }

            const fallbackLogo = definitiveFallbackLogo(row.cleanName, row.country);
            if (fallbackLogo) {
                missingLogos += 1;
                return fallbackLogo;
            }

            missingLogos += 1;
            return '';
        }

        const epgUrl = String(req.headers['x-forwarded-proto'] || req.protocol).split(',')[0] + '://' + req.headers.host + '/epg.xml';
        const output = ['#EXTM3U x-tvg-url="' + epgUrl + '" url-tvg="' + epgUrl + '"'];

        for (const row of italyRows) {
            const logo = resolveLogo(row);
            output.push(`#EXTINF:-1 tvg-name="${row.cleanName}" group-title="Italy" tvg-logo="${logo}" tvg-id="${row.epgId}",${row.display}`);
            output.push('#EXTVLCOPT:http-user-agent=VAVOO/2.6');
            output.push('#EXTVLCOPT:no-ssl-verify');
            output.push(`${req.protocol}://${req.headers.host}/stream/${encodeURIComponent(row.channel.id)}.m3u8`);
        }

        for (const row of foreignRows) {
            const logo = resolveLogo(row);
            output.push(`#EXTINF:-1 tvg-name="${row.cleanName}" group-title="${row.country}" tvg-logo="${logo}" tvg-id="${row.epgId}",${row.display}`);
            output.push('#EXTVLCOPT:http-user-agent=VAVOO/2.6');
            output.push('#EXTVLCOPT:no-ssl-verify');
            output.push(`${req.protocol}://${req.headers.host}/stream/${encodeURIComponent(row.channel.id)}.m3u8`);
        }

        console.log(
            `[vavoo] lista-test selected Italy=${italyRows.length} SPORT_MONDO=${foreignRows.length} `
            + `logos sibling=${siblingFallbacks} epg=${epgFallbacks} finalFallback=${missingLogos}`
        );
        setPlaylistHeaders(res);
        res.send(output.join('\n'));
    } catch (error) {
        console.log('[vavoo] lista-test.m3u8 error', error.message);
        res.status(500).send(error.message);
    }
});

app.get('/channels.m3u8', async function (req, res) {
    try {
        const country = req.query.country;
        const channels = country ? await getChannelsByCountry(country) : await getChannels();
        const epgMap = country ? await getEpgMap(country) : {};
        const output = ['#EXTM3U'];

        for (const channel of channels) {
            const cleanName = String(channel.name || '').replace(/\s*\.\s*[cs]\s*$/i, '').trim();
            const epgId = lookupEpgId(epgMap, channel.name, channel.country) || lookupEpgId(epgMap, cleanName, channel.country) || '';
            output.push(`#EXTINF:-1 tvg-name="${cleanName}" group-title="${channel.country}" tvg-logo="${channel.logo}" tvg-id="${epgId}",${cleanName}`);
            output.push('#EXTVLCOPT:http-user-agent=VAVOO/2.6');
            output.push('#EXTVLCOPT:no-ssl-verify');
            output.push(`${req.protocol}://${req.headers.host}/stream/${encodeURIComponent(channel.id)}.m3u8`);
        }

        setPlaylistHeaders(res);
        res.send(output.join('\n'));
    } catch (error) {
        console.log('[vavoo] channels.m3u8 error', error.message);
        res.status(500).send(error.message);
    }
});

app.get('/hls-proxy', async function (req, res) {
    const upstreamUrl = req.query.url;
    const connId = `${req.socket.remoteAddress}`;

    if (!upstreamUrl) {
        console.log(`[${connId}] hls proxy error: missing url`);
        res.status(400).send('missing url');
        return;
    }

    try {
        const parsedUrl = new URL(upstreamUrl);
        if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
            console.log(`[${connId}] hls proxy error "${upstreamUrl}": unsupported protocol`);
            res.status(400).send('unsupported upstream protocol');
            return;
        }

        console.log(`[${connId}] hls proxy opened "${describeUpstreamUrl(parsedUrl.toString())}"`);
        await proxyUpstreamUrl(req, res, parsedUrl.toString());
    } catch (error) {
        console.log(`[${connId}] hls proxy error: invalid upstream url: ${error.message}`);
        res.status(400).send(`invalid upstream url: ${error.message}`);
    }
});

app.get('/stream/:id', async function (req, res) {
    const connId = `${req.socket.remoteAddress}`;
    const userAgent = req.headers['user-agent'] ?? 'unknown';

    try {
        console.log(`[${connId}] connection opened: "${userAgent}"`);

        const channelId = normalizeStreamId(req.params.id);
        const channel = await findChannelById(channelId);
        if (!channel) {
            res.status(404).send(`unknown channel: ${channelId}`);
            return;
        }

        const streamUrl = await resolveStreamUrl(channel);
        console.log(`[${connId}] resolved "${channel.name}": ${streamUrl}`);

        if (redirect && userAgent.toLowerCase().includes('vavoo')) {
            res.redirect(streamUrl);
            return;
        }

        if (isM3u8Url(streamUrl)) {
            console.log(`[${connId}] hls playlist proxy "${channel.name}"`);
            await proxyUpstreamUrl(req, res, streamUrl);
            return;
        }

        await proxyStream(req, res, streamUrl, channel.name);
    } catch (error) {
        console.log(`[${connId}] playback error`, error.message);
        res.status(500).send(error.message);
    }
});

// Railway source deploy trigger: lista-test follows the same main-branch deployment flow as italia-test.
app.listen(port, httpHost, () => {
    const baseUrl = getLocalBaseUrl();
    console.log(`Listening on ${baseUrl}/`);
    console.log(`M3U: ${baseUrl}/channels.m3u8`);
    console.log(`Example filtered M3U: ${baseUrl}/channels.m3u8?country=Germany`);
    console.log(`Countries: ${baseUrl}/countries`);
    async function warmCatalogWithRetry(attempt = 1) {
        try {
            const channels = await loadChannelsOnce();
            console.log(`[vavoo] startup catalog ready: ${channels.length} channels (attempt ${attempt})`);
            return channels;
        } catch (error) {
            console.log(`[vavoo] startup catalog warmup failed attempt ${attempt}: ${error.message}`);
            if (attempt >= 5) return [];
            await new Promise(resolve => setTimeout(resolve, 15000));
            return warmCatalogWithRetry(attempt + 1);
        }
    }
    warmCatalogWithRetry();
    setTimeout(function () {
        try {
            const lists = loadCombinedWhitelist();
            epgService.getCombinedXml(lists.italy, lists.foreign)
                .then(function (result) {
                    console.log('[vavoo] startup EPG ready: ' + result.summary.channels + ' channels, ' + result.summary.programmes + ' programmes');
                })
                .catch(function (error) {
                    console.log('[vavoo] startup EPG warmup failed: ' + error.message);
                });
        } catch (error) {
            console.log('[vavoo] startup EPG warmup failed: ' + error.message);
        }
    }, 5000);
});