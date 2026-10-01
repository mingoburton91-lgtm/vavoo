const crypto = require('node:crypto');
const { Readable } = require('node:stream');
const { createGunzip } = require('node:zlib');

const CONFIG = {
    'italy': { cc: 'it', source: 'IT' },
    'united kingdom': { cc: 'gb', source: 'UK' },
    'uk': { cc: 'gb', source: 'UK' },
    'germany': { cc: 'de', source: 'DE' },
    'france': { cc: 'fr', source: 'FR' },
    'netherlands': { cc: 'nl', source: 'NL' },
    'romania': { cc: 'ro', source: 'RO' },
    'portugal': { cc: 'pt', source: 'PT' },
    'bulgaria': { cc: 'bg', source: 'BG' },
    'poland': { cc: 'pl', source: 'PL' },
    'turkey': { cc: 'tr', source: 'TR' },
    'albania': { cc: 'al', source: 'AL' },
    'croatia': { cc: 'hr', source: 'HR' },
    'serbia': { cc: 'rs', source: 'RS' },
    'austria': { cc: 'at', source: 'AT' },
    'switzerland': { cc: 'ch', source: 'CH' }
};

const CONFIG_BY_SOURCE = new Map(Object.values(CONFIG).map(function (cfg) {
    return [cfg.source, cfg];
}));

const ALIASES = {
    IT: {
        'tgcom 24': 'TGCom.it',
        'sky super tennis': 'SuperTennis.HD.it',
        'discovery focus': 'Focus.it',
        'discovery giallo': 'Giallo.TV.it',
        'discovery k2': 'K2.it',
        'discovery nove': 'Nove.it',
        'mediaset 20': '20.it',
        'mediaset extra': 'Mediaset.Extra.it',
        'mediaset iris': 'Iris.it',
        'mediaset italia 2': 'Italia.2.it',
        'radio freccia': 'RADIOFRECCIA.HD.it'
    }
};

function baseNormalize(value) {
    return String(value || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .trim();
}

function normalizeName(value) {
    return baseNormalize(value)
        .replace(/\s*\.\s*[cs]\s*$/i, '')
        .replace(/\s*\[[cs]\]\s*$/i, '')
        .replace(/\s*[\[(](?:backup|live during events only)[^\])]*[\])]\s*/gi, ' ')
        .replace(/\b(?:hd|fhd|uhd|4k)\b/gi, ' ')
        .replace(/\bsports\b/gi, 'sport')
        .replace(/\bmoto\s+gp\b/gi, 'motogp')
        .replace(/\btg\s+com\b/gi, 'tgcom')
        .replace(/[^a-z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function compactName(value) {
    return normalizeName(value).replace(/\s+/g, '');
}

function getConfig(country) {
    const key = baseNormalize(country);
    if (CONFIG[key]) return CONFIG[key];

    const upper = String(country || '').trim().toUpperCase();
    if (CONFIG_BY_SOURCE.has(upper)) return CONFIG_BY_SOURCE.get(upper);

    for (const cfg of Object.values(CONFIG)) {
        if (cfg.cc.toUpperCase() === upper) return cfg;
    }
    return null;
}

function sourceUrl(source, extension) {
    return 'https://epgshare01.online/epgshare01/epg_ripper_' + source + '1.' + extension;
}

function idNameVariants(id, cfg) {
    let base = String(id || '').trim();
    const suffixes = [cfg.source.toLowerCase(), cfg.cc.toLowerCase()];
    let changed = true;

    while (changed) {
        changed = false;
        for (const suffix of suffixes) {
            const re = new RegExp('\\.' + suffix + '$', 'i');
            if (re.test(base)) {
                base = base.replace(re, '');
                changed = true;
            }
        }
    }

    base = base.replace(/\.+/g, ' ').trim();

    const noQuality = base
        .replace(/\b(?:hd|fhd|uhd|4k)\b/gi, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    return Array.from(new Set([base, noQuality].filter(Boolean)));
}

function createEpgService() {
    const mapCache = new Map();
    const filteredCache = new Map();
    let combinedCache = { xml: '', expiresAt: 0, promise: null, summary: null };

    async function getMap(country) {
        const cfg = getConfig(country);
        if (!cfg) {
            return { byName: new Map(), byCompact: new Map(), ids: new Set(), source: null };
        }

        const cached = mapCache.get(cfg.source);
        if (cached && cached.expiresAt > Date.now()) return cached.data;

        const response = await fetch(sourceUrl(cfg.source, 'txt'), {
            signal: AbortSignal.timeout(15000)
        });
        if (!response.ok) {
            throw new Error('EPGShare ' + cfg.source + ' index HTTP ' + response.status);
        }

        const textBody = await response.text();
        const ids = textBody
            .split(/\r?\n/)
            .map(function (line) { return line.trim(); })
            .filter(function (line) {
                return line && !line.startsWith('--') && !/^\d{10,14}$/.test(line);
            });

        const data = {
            byName: new Map(),
            byCompact: new Map(),
            ids: new Set(ids),
            source: cfg.source
        };

        for (const id of ids) {
            for (const variant of idNameVariants(id, cfg)) {
                const normal = normalizeName(variant);
                const compact = compactName(variant);
                if (normal) data.byName.set(normal, id);
                if (compact) data.byCompact.set(compact, id);
            }
        }

        mapCache.set(cfg.source, {
            expiresAt: Date.now() + 6 * 60 * 60 * 1000,
            data: data
        });
        return data;
    }

    function lookupId(epgMap, name, country) {
        if (!epgMap || !name) return '';

        const cfg = getConfig(country) || CONFIG_BY_SOURCE.get(epgMap.source);
        const normal = normalizeName(name);
        const compact = compactName(name);
        const alias = cfg && ALIASES[cfg.source] ? ALIASES[cfg.source][normal] : '';

        if (alias && epgMap.ids.has(alias)) return alias;
        return epgMap.byName.get(normal) || epgMap.byCompact.get(compact) || '';
    }

    async function getWantedIds(italy, foreign) {
        const grouped = new Map();
        grouped.set('Italy', Array.isArray(italy) ? italy : []);

        const foreignItems = foreign && typeof foreign.values === 'function'
            ? Array.from(foreign.values())
            : (Array.isArray(foreign) ? foreign : []);

        for (const item of foreignItems) {
            if (!item || !item.country || !item.name) continue;
            if (!grouped.has(item.country)) grouped.set(item.country, []);
            grouped.get(item.country).push(item.name);
        }

        const wantedBySource = new Map();
        const stats = [];

        await Promise.all(Array.from(grouped.entries()).map(async function (entry) {
            const country = entry[0];
            const names = entry[1];
            const cfg = getConfig(country);
            if (!cfg) return;

            try {
                const epgMap = await getMap(country);
                const ids = new Set();
                let matched = 0;

                for (const rawName of names) {
                    const cleanName = String(rawName || '').replace(/\s*\[[CS]\]\s*$/i, '').trim();
                    const id = lookupId(epgMap, cleanName, country);
                    if (id) {
                        ids.add(id);
                        matched += 1;
                    }
                }

                wantedBySource.set(cfg.source, ids);
                stats.push({
                    country: country,
                    source: cfg.source,
                    total: names.length,
                    matched: matched,
                    uniqueIds: ids.size
                });
            } catch (error) {
                stats.push({
                    country: country,
                    source: cfg.source,
                    total: names.length,
                    matched: 0,
                    uniqueIds: 0,
                    error: error.message
                });
            }
        }));

        stats.sort(function (a, b) { return a.source.localeCompare(b.source); });
        console.log('[vavoo] EPG matching ' + stats.map(function (s) {
            return s.source + '=' + s.matched + '/' + s.total;
        }).join(' '));

        return { wantedBySource: wantedBySource, stats: stats };
    }

    async function responseToMaybeGunzipStream(response) {
        if (!response.body) throw new Error('EPG source returned an empty body');

        const source = Readable.fromWeb(response.body);
        const iterator = source[Symbol.asyncIterator]();
        const first = await iterator.next();
        if (first.done) return Readable.from([]);

        const firstBuffer = Buffer.from(first.value);

        async function* replay() {
            yield firstBuffer;
            while (true) {
                const next = await iterator.next();
                if (next.done) break;
                yield next.value;
            }
        }

        const replayStream = Readable.from(replay());
        if (firstBuffer.length >= 2 && firstBuffer[0] === 0x1f && firstBuffer[1] === 0x8b) {
            return replayStream.pipe(createGunzip());
        }
        return replayStream;
    }

    function xmlAttribute(block, attribute) {
        const match = String(block || '').match(new RegExp("\\b" + attribute + "=(['\"])(.*?)\\1", 'i'));
        return match ? match[2] : '';
    }

    async function filterSource(source, wantedIds) {
        if (!wantedIds || !wantedIds.size) {
            return { source: source, channels: [], programmes: [] };
        }

        const wantedHash = crypto
            .createHash('sha1')
            .update(Array.from(wantedIds).sort().join('\n'))
            .digest('hex')
            .slice(0, 12);

        const cacheKey = source + ':' + wantedHash;
        const cached = filteredCache.get(cacheKey);
        if (cached && cached.expiresAt > Date.now()) return cached.data;

        const response = await fetch(sourceUrl(source, 'xml.gz'), {
            signal: AbortSignal.timeout(45000)
        });
        if (!response.ok) {
            throw new Error('EPGShare ' + source + ' XML HTTP ' + response.status);
        }

        const stream = await responseToMaybeGunzipStream(response);
        let pending = '';
        const channels = [];
        const programmes = [];

        for await (const chunk of stream) {
            pending += Buffer.from(chunk).toString('utf8');

            while (true) {
                const channelStart = pending.indexOf('<channel');
                const programmeStart = pending.indexOf('<programme');
                let start = -1;
                let type = '';

                if (channelStart >= 0 && (programmeStart < 0 || channelStart < programmeStart)) {
                    start = channelStart;
                    type = 'channel';
                } else if (programmeStart >= 0) {
                    start = programmeStart;
                    type = 'programme';
                }

                if (start < 0) {
                    if (pending.length > 1024) pending = pending.slice(-1024);
                    break;
                }

                if (start > 0) pending = pending.slice(start);

                const closeTag = type === 'channel' ? '</channel>' : '</programme>';
                const closeIndex = pending.indexOf(closeTag);

                if (closeIndex < 0) {
                    if (pending.length > 5 * 1024 * 1024) {
                        throw new Error('EPGShare ' + source + ' XML block too large');
                    }
                    break;
                }

                const block = pending.slice(0, closeIndex + closeTag.length);
                pending = pending.slice(closeIndex + closeTag.length);

                const id = type === 'channel'
                    ? xmlAttribute(block, 'id')
                    : xmlAttribute(block, 'channel');

                if (id && wantedIds.has(id)) {
                    if (type === 'channel') channels.push(block);
                    else programmes.push(block);
                }
            }
        }

        const data = {
            source: source,
            channels: channels,
            programmes: programmes
        };

        filteredCache.set(cacheKey, {
            expiresAt: Date.now() + 15 * 60 * 1000,
            data: data
        });

        return data;
    }

    async function buildCombinedXml(italy, foreign) {
        const wantedResult = await getWantedIds(italy, foreign);
        const sources = Array.from(wantedResult.wantedBySource.entries())
            .filter(function (entry) { return entry[1].size > 0; });

        if (!sources.length) {
            throw new Error('No EPG ids matched the playlist whitelist');
        }

        const results = [];
        const failed = [];

        for (let i = 0; i < sources.length; i += 3) {
            const batch = sources.slice(i, i + 3);
            const settled = await Promise.all(batch.map(async function (entry) {
                const source = entry[0];
                const ids = entry[1];
                try {
                    return await filterSource(source, ids);
                } catch (error) {
                    failed.push(source + ':' + error.message);
                    return null;
                }
            }));

            for (const result of settled) {
                if (result) results.push(result);
            }
        }

        if (!results.length) {
            throw new Error('No EPG sources available');
        }

        const channels = [];
        const programmes = [];

        for (const result of results) {
            channels.push.apply(channels, result.channels);
            programmes.push.apply(programmes, result.programmes);
        }

        const xml = '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n'
            + channels.join('\n')
            + (channels.length ? '\n' : '')
            + programmes.join('\n')
            + (programmes.length ? '\n' : '')
            + '</tv>\n';

        const summary = {
            stats: wantedResult.stats,
            sources: results.map(function (result) { return result.source; }),
            failed: failed,
            channels: channels.length,
            programmes: programmes.length,
            bytes: Buffer.byteLength(xml)
        };

        console.log(
            '[vavoo] EPG built sources=' + summary.sources.join(',')
            + ' failed=' + (failed.join(',') || 'none')
            + ' channels=' + summary.channels
            + ' programmes=' + summary.programmes
            + ' bytes=' + summary.bytes
        );

        return { xml: xml, summary: summary };
    }

    async function getCombinedXml(italy, foreign) {
        if (combinedCache.xml && combinedCache.expiresAt > Date.now()) {
            return { xml: combinedCache.xml, summary: combinedCache.summary };
        }

        if (combinedCache.promise) return combinedCache.promise;

        combinedCache.promise = buildCombinedXml(italy, foreign)
            .then(function (result) {
                combinedCache = {
                    xml: result.xml,
                    expiresAt: Date.now() + 15 * 60 * 1000,
                    promise: null,
                    summary: result.summary
                };
                return result;
            })
            .catch(function (error) {
                combinedCache.promise = null;
                throw error;
            });

        return combinedCache.promise;
    }

    const countryCodes = {};
    for (const entry of Object.entries(CONFIG)) {
        countryCodes[entry[0]] = entry[1].cc;
    }

    return {
        countryCodes: countryCodes,
        getMap: getMap,
        lookupId: lookupId,
        getCombinedXml: getCombinedXml,
        countrySourceUrl: function (country) {
            const cfg = getConfig(country);
            return cfg ? sourceUrl(cfg.source, 'xml.gz') : '';
        }
    };
}

module.exports = { createEpgService: createEpgService };
