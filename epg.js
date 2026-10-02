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

const SOURCE_LANGUAGE = {
    UK: 'en',
    DE: 'de',
    AT: 'de',
    CH: 'de',
    FR: 'fr',
    NL: 'nl',
    RO: 'ro',
    PT: 'pt',
    BG: 'bg',
    PL: 'pl',
    TR: 'tr'
};

function decodeXmlText(value) {
    return String(value || '')
        .replace(/&#x([0-9a-f]+);/gi, function (_, hex) { return String.fromCodePoint(parseInt(hex, 16)); })
        .replace(/&#([0-9]+);/g, function (_, dec) { return String.fromCodePoint(parseInt(dec, 10)); })
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&');
}

function encodeXmlText(value) {
    return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function forceItalianLang(attrs) {
    const raw = String(attrs || '');
    if (/\blang\s*=\s*["'][^"']*["']/i.test(raw)) {
        return raw.replace(/\blang\s*=\s*["'][^"']*["']/i, 'lang="it"');
    }
    return raw + ' lang="it"';
}

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
    const translationCache = new Map();
    const translatorBaseUrl = String(process.env.EPG_TRANSLATOR_URL || '').replace(/\/+$/, '');
    let combinedCache = { xml: '', expiresAt: 0, promise: null, summary: null, results: null };
    let translatedCache = { xml: '', expiresAt: 0, summary: null };
    let translationPromise = null;

    function trimTranslationCache() {
        if (translationCache.size <= 50000) return;
        const removeCount = translationCache.size - 40000;
        const keys = translationCache.keys();
        for (let i = 0; i < removeCount; i += 1) {
            const next = keys.next();
            if (next.done) break;
            translationCache.delete(next.value);
        }
    }

    async function translateTextBatch(source, language, texts) {
        const output = new Map();
        const missing = [];

        for (const text of texts) {
            const clean = String(text || '').trim();
            if (!clean) continue;
            const key = source + '|' + clean;
            if (translationCache.has(key)) {
                output.set(clean, translationCache.get(key));
            } else {
                missing.push(clean);
            }
        }

        if (!missing.length || !translatorBaseUrl) return output;

        const unique = Array.from(new Set(missing));
        const batches = [];
        let batch = [];
        let chars = 0;

        for (const text of unique) {
            if (batch.length >= 40 || chars + text.length > 12000) {
                batches.push(batch);
                batch = [];
                chars = 0;
            }
            batch.push(text);
            chars += text.length;
        }
        if (batch.length) batches.push(batch);

        console.log('[vavoo] EPG translation source=' + source + ' language=' + language + ' texts=' + unique.length + ' batches=' + batches.length);

        for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
            const items = batches[batchIndex];
            if (batchIndex === 0 || (batchIndex + 1) % 10 === 0 || batchIndex === batches.length - 1) {
                console.log('[vavoo] EPG translation progress source=' + source + ' batch=' + (batchIndex + 1) + '/' + batches.length);
            }
            let lastError = null;
            let translated = null;

            for (let attempt = 1; attempt <= 3; attempt += 1) {
                const controller = new AbortController();
                const timer = setTimeout(function () { controller.abort(); }, 120000);
                try {
                    const response = await fetch(translatorBaseUrl + '/translate', {
                        method: 'POST',
                        headers: { 'content-type': 'application/json' },
                        body: JSON.stringify({
                            q: items,
                            source: language,
                            target: 'it',
                            format: 'text'
                        }),
                        signal: controller.signal
                    });

                    if (!response.ok) {
                        throw new Error('HTTP ' + response.status);
                    }

                    const body = await response.json();
                    translated = Array.isArray(body.translatedText)
                        ? body.translatedText
                        : [body.translatedText];
                    lastError = null;
                    break;
                } catch (error) {
                    lastError = error;
                    console.log('[vavoo] EPG translation retry source=' + source + ' batch=' + (batchIndex + 1) + '/' + batches.length + ' attempt=' + attempt + ' error=' + error.message);
                    if (attempt < 3) {
                        await new Promise(function (resolve) { setTimeout(resolve, attempt * 2000); });
                    }
                } finally {
                    clearTimeout(timer);
                }
            }

            if (lastError || !translated) throw lastError || new Error('translation failed');

            for (let i = 0; i < items.length; i += 1) {
                const original = items[i];
                const value = String(translated[i] || original);
                output.set(original, value);
                translationCache.set(source + '|' + original, value);
            }
            trimTranslationCache();
        }

        return output;
    }

    function collectProgrammeTexts(programmes) {
        const texts = new Set();
        const tagRe = /<(title|sub-title|desc)\b([^>]*)>([\s\S]*?)<\/\1>/gi;

        for (const block of programmes) {
            let match;
            while ((match = tagRe.exec(block)) !== null) {
                const inner = match[3];
                if (!inner || /<[^>]+>/.test(inner)) continue;
                const plain = decodeXmlText(inner).trim();
                if (plain.length >= 2) texts.add(plain);
            }
        }
        return Array.from(texts);
    }

    function applyProgrammeTranslations(programmes, translations) {
        const tagRe = /<(title|sub-title|desc)\b([^>]*)>([\s\S]*?)<\/\1>/gi;

        return programmes.map(function (block) {
            return block.replace(tagRe, function (full, tag, attrs, inner) {
                if (!inner || /<[^>]+>/.test(inner)) return full;
                const plain = decodeXmlText(inner).trim();
                const translated = translations.get(plain);
                if (!translated || translated === plain) return full;
                return '<' + tag + forceItalianLang(attrs) + '>' + encodeXmlText(translated) + '</' + tag + '>';
            });
        });
    }

    async function translateResult(result) {
        if (!result || result.source === 'IT') return result;
        const language = SOURCE_LANGUAGE[result.source];
        if (!language || !translatorBaseUrl) return result;

        const texts = collectProgrammeTexts(result.programmes);
        if (!texts.length) return result;

        const translations = await translateTextBatch(result.source, language, texts);
        return {
            source: result.source,
            channels: result.channels,
            programmes: applyProgrammeTranslations(result.programmes, translations)
        };
    }

    function composeXml(results, summary) {
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

        const nextSummary = Object.assign({}, summary, {
            channels: channels.length,
            programmes: programmes.length,
            bytes: Buffer.byteLength(xml)
        });

        return { xml: xml, summary: nextSummary };
    }

    function startTranslation(results, summary) {
        if (!translatorBaseUrl || translationPromise) return;

        translationPromise = (async function () {
            const translatedResults = [];
            let translatedSources = 0;

            for (const result of results) {
                try {
                    const translated = await translateResult(result);
                    translatedResults.push(translated);
                    if (translated !== result) translatedSources += 1;
                } catch (error) {
                    console.log('[vavoo] EPG translation failed for ' + result.source + ': ' + error.message);
                    translatedResults.push(result);
                }
            }

            const translated = composeXml(translatedResults, Object.assign({}, summary, {
                translated: true,
                translatedSources: translatedSources
            }));

            translatedCache = {
                xml: translated.xml,
                expiresAt: Date.now() + 12 * 60 * 60 * 1000,
                summary: translated.summary
            };

            console.log(
                '[vavoo] EPG translation ready sources=' + translatedSources
                + ' cache=' + translationCache.size
                + ' programmes=' + translated.summary.programmes
            );
        })().catch(function (error) {
            console.log('[vavoo] EPG translation error: ' + error.message);
        }).finally(function () {
            translationPromise = null;
        });
    }

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

        const summary = {
            stats: wantedResult.stats,
            sources: results.map(function (result) { return result.source; }),
            failed: failed,
            translated: false
        };

        const composed = composeXml(results, summary);

        console.log(
            '[vavoo] EPG built sources=' + composed.summary.sources.join(',')
            + ' failed=' + (failed.join(',') || 'none')
            + ' channels=' + composed.summary.channels
            + ' programmes=' + composed.summary.programmes
            + ' bytes=' + composed.summary.bytes
        );

        return {
            xml: composed.xml,
            summary: composed.summary,
            results: results
        };
    }

    async function getCombinedXml(italy, foreign) {
        if (translatedCache.xml) {
            if (translatedCache.expiresAt <= Date.now() && !combinedCache.promise && !translationPromise) {
                combinedCache.promise = buildCombinedXml(italy, foreign)
                    .then(function (result) {
                        combinedCache = {
                            xml: result.xml,
                            expiresAt: Date.now() + 15 * 60 * 1000,
                            promise: null,
                            summary: result.summary,
                            results: result.results
                        };
                        startTranslation(result.results, result.summary);
                        return { xml: result.xml, summary: result.summary };
                    })
                    .catch(function (error) {
                        combinedCache.promise = null;
                        console.log('[vavoo] EPG background refresh failed: ' + error.message);
                        return { xml: translatedCache.xml, summary: translatedCache.summary };
                    });
            }
            return { xml: translatedCache.xml, summary: translatedCache.summary };
        }

        if (combinedCache.xml && combinedCache.expiresAt > Date.now()) {
            if (combinedCache.results) startTranslation(combinedCache.results, combinedCache.summary);
            return { xml: combinedCache.xml, summary: combinedCache.summary };
        }

        if (combinedCache.promise) return combinedCache.promise;

        combinedCache.promise = buildCombinedXml(italy, foreign)
            .then(function (result) {
                combinedCache = {
                    xml: result.xml,
                    expiresAt: Date.now() + 15 * 60 * 1000,
                    promise: null,
                    summary: result.summary,
                    results: result.results
                };
                startTranslation(result.results, result.summary);
                return { xml: result.xml, summary: result.summary };
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
