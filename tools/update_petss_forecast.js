/**
 * Fetch + parse NOAA PETSS (P-ETSS) table embedded in HTML,
 * then write a compact JSON for the next 48h.
 *
 * Output: data/petss_forecast_8536889_mllw.json
 *
 * Run via GitHub Actions nightly (midnight America/New_York).
 */

import fs from "fs";
import path from "path";

const STID = process.env.PETSS_STID || "8536889";       // Bivalve NJ
const DATUM = process.env.PETSS_DATUM || "MLLW";        // page parameter
const HOURS = Number(process.env.PETSS_HOURS || "48");  // next 48h

const URL =
  `https://slosh.nws.noaa.gov/petss/index.php?stid=${STID}&datum=${DATUM}&show=1-1-1-1-0-1-1-1`;

const OUT_PATH = path.join("data", `petss_forecast_${STID}_${DATUM.toLowerCase()}.json`);

// ---- Helpers ----
function extractPreBlock(html) {
  // PETSS pages contain a <pre> with the CSV-like table.
  // We'll grab the chunk containing "Date(GMT)" and "Fcst".
  const m = html.match(/<pre[^>]*>([\s\S]*?)<\/pre>/gi);
  if (!m) return null;

  for (const block of m) {
    const inner = block
      .replace(/<pre[^>]*>/i, "")
      .replace(/<\/pre>/i, "")
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">");

    if (inner.includes("Date(GMT)") && inner.includes("Fcst")) return inner;
  }
  return null;
}

function parsePetssTable(text) {
  const lines = text
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(Boolean);

  // Find header line "Date(GMT), Surge, Tide, Obs, Fcst, ..."
  const headerIdx = lines.findIndex(l => l.startsWith("Date(GMT)"));
  if (headerIdx < 0) throw new Error("Could not find PETSS header row (Date(GMT)...)");

  const header = lines[headerIdx].split(",").map(s => s.trim());
  const rows = [];

  for (let i = headerIdx + 1; i < lines.length; i++) {
    const parts = lines[i].split(",").map(s => s.trim());
    if (parts.length < header.length) continue;

    const rec = {};
    for (let j = 0; j < header.length; j++) rec[header[j]] = parts[j];

    // Date looks like "01/31 18Z" (month/day hourZ) – assume CURRENT YEAR from table context:
    rows.push(rec);
  }

  return { header, rows };
}

function inferYearFromHtml(html) {
  // The graph title includes "Model Time(UTC): 2026-01-31 18:00"
  const m = html.match(/Model Time\(UTC\):\s*([0-9]{4})-([0-9]{2})-([0-9]{2})/);
  if (!m) return new Date().getUTCFullYear();
  return Number(m[1]);
}

function dateGmtToISO(dateGmt, year) {
  // "01/31 18Z" -> ISO "YYYY-01-31T18:00:00Z"
  const m = String(dateGmt).match(/^(\d{2})\/(\d{2})\s+(\d{2})Z$/);
  if (!m) return null;
  const mm = Number(m[1]);
  const dd = Number(m[2]);
  const hh = Number(m[3]);
  const d = new Date(Date.UTC(year, mm - 1, dd, hh, 0, 0));
  return isNaN(d.getTime()) ? null : d.toISOString();
}

function clipNextHours(points, hours) {
  if (!points.length) return [];
  const t0 = new Date(points[0].t).getTime();
  const t1 = t0 + hours * 3600 * 1000;
  return points.filter(p => {
    const tt = new Date(p.t).getTime();
    return Number.isFinite(tt) && tt <= t1;
  });
}

// ---- Main ----
async function main() {
  const res = await fetch(URL, { cache: "no-store" });
  if (!res.ok) throw new Error(`PETSS fetch failed: HTTP ${res.status}`);

  const html = await res.text();
  const year = inferYearFromHtml(html);

  const pre = extractPreBlock(html);
  if (!pre) throw new Error("Could not locate <pre> table on PETSS page.");

  const { rows } = parsePetssTable(pre);

  // Build time series from Fcst (and uncertainty if present)
  const points = rows
    .map(r => {
      const t = dateGmtToISO(r["Date(GMT)"], year);
      if (!t) return null;

      const fcst = Number(r["Fcst"]);
      const fst10 = Number(r["Fst10%"]);
      const fst90 = Number(r["Fst90%"]);
      const surge = Number(r["Surge"]);
      const tide = Number(r["Tide"]);

      if (!Number.isFinite(fcst)) return null;

      return {
        t,
        fcst,
        tide: Number.isFinite(tide) ? tide : null,
        surge: Number.isFinite(surge) ? surge : null,
        p10: Number.isFinite(fst10) ? fst10 : null,
        p90: Number.isFinite(fst90) ? fst90 : null
      };
    })
    .filter(Boolean)
    .sort((a, b) => new Date(a.t) - new Date(b.t));

  const clipped = clipNextHours(points, HOURS);

  // Model time for metadata
  const modelTimeMatch = html.match(/Model Time\(UTC\):\s*([0-9:\-\s]{16,})/);
  const modelTimeUtc = modelTimeMatch ? modelTimeMatch[1].trim() : null;

  const out = {
    station: STID,
    datum: DATUM,
    source: "NOAA PETSS (P-ETSS) embedded table",
    fetched_utc: new Date().toISOString(),
    model_time_utc: modelTimeUtc,
    hours: HOURS,
    points: clipped
  };

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2));
  console.log(`Wrote ${OUT_PATH} with ${clipped.length} points`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
