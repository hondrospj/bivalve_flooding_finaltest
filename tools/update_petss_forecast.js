// tools/update_petss_forecast.js
// Robust PETSS scraper (static=1 + tolerant parsing).
//
// Writes:
//   data/petss_forecast_<stid>_<datum>.json
//
// On parse failure, writes:
//   data/petss_raw.html

import fs from "fs";
import path from "path";

const STID = process.env.PETSS_STID || "8536889"; // ✅ aligned default
const DATUM = process.env.PETSS_DATUM || "MLLW";
const SHOW = process.env.PETSS_SHOW || "1-1-1-1-0-1-1-1";

// ✅ IMPORTANT: static=1 makes PETSS include the plain-text table in HTML
const URL = `https://slosh.nws.noaa.gov/petss/index.php?stid=${encodeURIComponent(
  STID
)}&datum=${encodeURIComponent(DATUM)}&show=${encodeURIComponent(SHOW)}&static=1`;

const outDir = "data";
const outJson = path.join(outDir, `petss_forecast_${STID}_${DATUM.toLowerCase()}.json`);
const outRaw = path.join(outDir, "petss_raw.html");

function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|tr|li|h1|h2|h3|h4|h5|h6)>/gi, "\n")
    .replace(/<[^>]+>/g, "");
}

function extractDataText(html) {
  // 1) Prefer <pre> if present
  const preMatch = html.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
  if (preMatch && preMatch[1]) {
    return preMatch[1].trim();
  }

  // 2) Fallback: strip tags and find the "Date(GMT)" header
  const text = stripTags(html);

  const idx = text.search(/Date\s*\(GMT\)/i);
  if (idx !== -1) {
    // take a generous slice starting at Date(GMT)
    return text.slice(idx).trim();
  }

  return null;
}

function parsePetssTable(textBlock) {
  // PETSS header usually includes:
  // Date(GMT), Surge, Tide, Obs, Fcst, Anom, Fst90%, Fst10%
  // then rows like:
  // 01/25 06Z, -0.3, 3.9, 4.3, 4.3, 0.7, 4.2, 4.3

  const lines = textBlock
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  // ✅ Find header line even if it isn't at column 0
  const headerIdx = lines.findIndex((l) => /Date\s*\(GMT\)/i.test(l));
  if (headerIdx === -1) return null;

  // ✅ Normalize header line to start at Date(GMT)
  let headerLine = lines[headerIdx];
  const m = headerLine.match(/Date\s*\(GMT\)[\s\S]*/i);
  if (m) headerLine = m[0];

  const header = headerLine
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const rows = [];

  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i];

    // stop at obvious footers
    if (/copyright|noaa|national weather service|last updated/i.test(line)) break;

    // ✅ PETSS row signature: "MM/DD HHZ,"
    if (!/^\d{2}\/\d{2}\s+\d{2}Z\s*,/i.test(line)) continue;

    const parts = line.split(",").map((s) => s.trim());
    if (parts.length < 2) continue;

    const obj = {};
    for (let c = 0; c < Math.min(header.length, parts.length); c++) {
      const key = header[c];
      const val = parts[c];

      if (/Date\s*\(GMT\)/i.test(key)) {
        obj.date_gmt = val;
      } else {
        const num = Number(val);
        obj[key.toLowerCase().replace(/[^a-z0-9]+/g, "_")] = Number.isFinite(num) ? num : val;
      }
    }

    rows.push(obj);
  }

  if (!rows.length) return null;

  return {
    source: "PETSS",
    url: URL,
    stid: STID,
    datum: DATUM,
    fetched_utc: new Date().toISOString(),
    rows
  };
}

async function main() {
  console.log("Running PETSS forecast script…");
  console.log("URL:", URL);

  fs.mkdirSync(outDir, { recursive: true });

  const res = await fetch(URL, {
    headers: {
      "User-Agent": "Mozilla/5.0 (GitHub Actions) PETSS-scraper",
      "Accept": "text/html,application/xhtml+xml"
    }
  });

  const html = await res.text();

  if (!res.ok) {
    fs.writeFileSync(outRaw, html);
    throw new Error(`HTTP ${res.status} from PETSS. Saved raw HTML to ${outRaw}`);
  }

  const textBlock = extractDataText(html);
  if (!textBlock) {
    fs.writeFileSync(outRaw, html);
    throw new Error(
      `Could not find PETSS data block (no <pre>, no Date(GMT)). Saved raw HTML to ${outRaw}`
    );
  }

  const parsed = parsePetssTable(textBlock);
  if (!parsed) {
    fs.writeFileSync(outRaw, html);
    throw new Error(`Found text but could not parse PETSS rows. Saved raw HTML to ${outRaw}`);
  }

  fs.writeFileSync(outJson, JSON.stringify(parsed, null, 2));
  console.log(`✅ Wrote ${outJson} with ${parsed.rows.length} rows`);
}

main().catch((err) => {
  console.error("PETSS fetch/parse failed:", err.message);
  process.exit(1);
});
