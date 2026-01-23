#!/usr/bin/env node
/**
 * Incrementally build/update NAVD88 peak events for USGS 01412150 (param 72279)
 * Writes to: data/bivalve_peaks_navd88.json
 *
 * Modes:
 *   node tools/update_bivalve_peaks_navd88.js
 *     -> incremental update from lastProcessedISO (with buffer) to now
 *
 *   node tools/update_bivalve_peaks_navd88.js --backfill-year=2000
 *     -> backfill exactly that calendar year (UTC) and advance lastProcessedISO
 */

const fs = require("fs");
const path = require("path");

// -------------------------
// Config (matches your dashboard)
// -------------------------
const CACHE_PATH = path.join(__dirname, "..", "data", "bivalve_peaks_navd88.json");
const SITE = "01412150";
const PARAM = "72279";

// IMPORTANT: match your current decluster spacing
const PEAK_MIN_SEP_MINUTES = 300; // :contentReference[oaicite:1]{index=1}

const BUFFER_HOURS = 12; // overlap so boundary peaks don't get missed

// -------------------------
// Helpers
// -------------------------
function die(msg) {
  console.error(msg);
  process.exit(1);
}

function loadJSON(p) {
  if (!fs.existsSync(p)) die(`Missing cache file: ${p}`);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function saveJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function isoNow() {
  return new Date().toISOString();
}

function addHoursISO(iso, hours) {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return null;
  return new Date(t + hours * 3600 * 1000).toISOString();
}

function clampISO(iso) {
  const t = new Date(iso).getTime();
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

function parseArg(name) {
  const a = process.argv.find(x => x.startsWith(name + "="));
  return a ? a.split("=").slice(1).join("=") : null;
}

function roundFt(x) {
  // rounding helps dedupe if USGS returns tiny float noise
  return Math.round(x * 1000) / 1000;
}

// -------------------------
// USGS IV fetch (15-min)
// -------------------------
async function fetchUSGSIV({ startISO, endISO }) {
  // USGS IV JSON endpoint (parameter 72279 is "Tidal elevation, NOS averaged" in your usage)
  // Dates must be ISO-ish; USGS accepts YYYY-MM-DD or full ISO.
  const url =
    "https://waterservices.usgs.gov/nwis/iv/?" +
    new URLSearchParams({
      format: "json",
      sites: SITE,
      parameterCd: PARAM,
      startDT: startISO,
      endDT: endISO,
      siteStatus: "all"
    }).toString();

  const res = await fetch(url, { headers: { "User-Agent": "bivalve-peaks-cache/1.0" } });
  if (!res.ok) throw new Error(`USGS IV fetch failed: ${res.status} ${res.statusText}`);
  const j = await res.json();

  const ts = j?.value?.timeSeries?.[0];
  const vals = ts?.values?.[0]?.value || [];

  // Normalize to your series shape: [{t, ft}]
  const series = vals
    .map(v => ({ t: v.dateTime, ft: Number(v.value) }))
    .filter(p => p.t && Number.isFinite(p.ft));

  // Ensure time ascending (your extractor sorts anyway, but keep consistent)
  series.sort((a, b) => new Date(a.t) - new Date(b.t));
  return series;
}

// -------------------------
// Peak extraction (COPY of your dashboard logic)
// -------------------------
// This mirrors extractFloodPeaks_NAVD() in your code :contentReference[oaicite:2]{index=2}
function extractFloodPeaks_NAVD(series, thresholdsNAVD88) {
  if (!series || series.length < 3) return [];

  const T = thresholdsNAVD88;

  const pts = [...series]
    .map(p => ({ t: p.t, ft: Number(p.ft) }))
    .filter(p => p.t && Number.isFinite(p.ft))
    .sort((a, b) => new Date(a.t) - new Date(b.t));

  if (pts.length < 3) return [];

  const candidates = [];
  for (let i = 1; i < pts.length - 1; i++) {
    const a = pts[i - 1], b = pts[i], c = pts[i + 1];
    if (b.ft >= a.ft && b.ft >= c.ft) {
      if (!(b.ft === a.ft && b.ft === c.ft)) candidates.push(b);
    }
  }
  if (!candidates.length) return [];

  const minSepMs = PEAK_MIN_SEP_MINUTES * 60 * 1000;

  const kept = [];
  let cur = candidates[0];

  for (let i = 1; i < candidates.length; i++) {
    const p = candidates[i];

    const pt = new Date(p.t).getTime();
    const ct = new Date(cur.t).getTime();

    if (Number.isFinite(pt) && Number.isFinite(ct) && (pt - ct) <= minSepMs) {
      if (p.ft > cur.ft) cur = p;
    } else {
      kept.push(cur);
      cur = p;
    }
  }
  kept.push(cur);

  return kept.map(p => {
    let type = "Below";
    if (p.ft >= T.majorLow) type = "Major";
    else if (p.ft >= T.moderateLow) type = "Moderate";
    else if (p.ft >= T.minorLow) type = "Minor";
    return { t: p.t, ft: p.ft, type };
  });
}

// -------------------------
// Main update logic
// -------------------------
async function main() {
  const cache = loadJSON(CACHE_PATH);

  // You already have NAVD thresholds in your dashboard under THRESH.NAVD88; define the same here:
  // If you want, we can literally read them from a small constants JSON later — for now, keep in sync manually.
  // NOTE: Replace these with your exact NAVD88 thresholds from the dashboard.
  const THRESH_NAVD88 = cache?.thresholdsNAVD88 || null;
  if (!THRESH_NAVD88) {
    // If you don’t store thresholds in cache, hardcode them here.
    // I’m not guessing your NAVD88 values; put the exact ones from your THRESH.NAVD88 block.
    die(
      "Missing NAVD88 thresholds. Add thresholdsNAVD88 to data/bivalve_peaks_navd88.json, e.g.\n" +
      '  "thresholdsNAVD88": {"minorLow": X, "moderateLow": Y, "majorLow": Z}\n' +
      "…using the same NAVD88 numbers as your dashboard."
    );
  }

  const backfillYear = parseArg("--backfill-year");
  let startISO, endISO;

  if (backfillYear) {
    const y = Number(backfillYear);
    if (!Number.isFinite(y) || y < 1900 || y > 3000) die("Invalid --backfill-year=YYYY");
    startISO = new Date(Date.UTC(y, 0, 1, 0, 0, 0)).toISOString();
    endISO = new Date(Date.UTC(y + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill year ${y}: ${startISO} → ${endISO}`);
  } else {
    const last = clampISO(cache.lastProcessedISO || "2000-01-01T00:00:00Z");
    if (!last) die("Cache lastProcessedISO is invalid ISO.");
    // overlap window to handle boundary/decluster
    startISO = addHoursISO(last, -BUFFER_HOURS);
    endISO = isoNow();
    console.log(`Incremental: ${startISO} → ${endISO}`);
  }

  const series = await fetchUSGSIV({ startISO, endISO });
  if (!series.length) {
    console.log("No series points returned; nothing to do.");
    return;
  }

  const peaks = extractFloodPeaks_NAVD(series, THRESH_NAVD88)
    .map(p => ({ t: new Date(p.t).toISOString(), ft: roundFt(p.ft), type: p.type }));

  // Deduplicate against existing events
  const existing = Array.isArray(cache.events) ? cache.events : [];
  const seen = new Set(existing.map(e => `${e.t}|${roundFt(Number(e.ft))}`));

  let added = 0;
  for (const p of peaks) {
    const k = `${p.t}|${roundFt(Number(p.ft))}`;
    if (!seen.has(k)) {
      existing.push(p);
      seen.add(k);
      added++;
    }
  }

  // Keep chronological order
  existing.sort((a, b) => new Date(a.t) - new Date(b.t));
  cache.events = existing;

  // Advance lastProcessedISO to newest timestamp in the fetched series (not just peaks)
  const newestT = series[series.length - 1]?.t;
  if (newestT) cache.lastProcessedISO = new Date(newestT).toISOString();

  saveJSON(CACHE_PATH, cache);

  console.log(`Fetched points: ${series.length}`);
  console.log(`Peaks found:   ${peaks.length}`);
  console.log(`Peaks added:   ${added}`);
  console.log(`New lastProcessedISO: ${cache.lastProcessedISO}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

