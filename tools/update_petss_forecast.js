// tools/update_petss_forecast.js
// Robust PETSS scraper: handles <pre> and non-<pre> layouts.
// Writes:
//   data/petss_forecast_<stid>_mllw.json
// On parse failure, writes:
//   data/petss_raw.html

import fs from "fs";
import path from "path";

const STID = process.env.PETSS_STID || "8531804"; // change default if you want
const DATUM = process.env.PETSS_DATUM || "MLLW";
const SHOW = process.env.PETSS_SHOW || "1-1-1-1-0-1-1-1";

// Example:
// https://slosh.nws.noaa.gov/petss/index.php?stid=8531804&datum=MLLW&show=1-1-1-1-0-1-1-1
const URL = `https://slosh.nws.noaa.gov/petss/index.php?stid=${encodeURIComponent(
  STID
)}&datum=${encodeURIComponent(DATUM)}&show=${encodeURIComponent(SHOW)}`;

const outDir = "data";
const outJson = path.join(outDir, `petss_forecast_${STID}_${DATUM.toLowerCase()}.json`);
const outRaw = path.join(outDir, "petss_raw.html");

function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|tr)>/gi, "\n")
    .replace(/<[^>]+>/g, "");
}

function extractDataText(html) {
  // 1) Prefer <pre> if present
  const preMatch = html.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
  if (preMatch && preMatch[1]) {
    return preMatch[1].trim();
  }

  // 2) Fallback: strip tags and look for the "Date(GMT)" block in plain text
  const text = stripTags(html);
  const idx = text.indexOf("Date(GMT)");
  if (idx !== -1) {
    // Take a generous slice after the header
    const slice = text.slice(idx).trim();
    return slice;
  }

  return null;
}

function parsePetssTable(textBlock) {
  // We expect a header line with columns like:
  // Date(GMT), Surge, Tide, Obs, Fcst, Anom, Fst90%, Fst10%
  // Then many CSV-like rows.
  const lines = textBlock
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const headerIdx = lines.findIndex((l) => l.startsWith("Date(GMT)"));
  if (headerIdx === -1) return null;

  const header = lines[headerIdx]
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i];

    // Stop if we hit obvious footer content
    if (/copyright|noaa|national weather service|last updated/i.test(line)) break;

    // PETSS rows usually look like:
    // 01/25 06Z, -0.3, 3.9, 4.3, 4.3, 0.7, 4.2, 4.3
    if (!/^\d{2}\/\d{2}\s+\d{2}Z,/.test(line)) continue;

    const parts = line.split(",").map((s) => s.trim());

    // If columns mismatch, skip
    if (parts.length < 2) continue;

    const obj = {};
    for (let c = 0; c < Math.min(header.length, parts.length); c++) {
      const key = header[c];
      const val = parts[c];

      if (key === "Date(GMT)") {
        obj.date_gmt = val;
      } else {
        // numeric fields
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
      // Some sites serve different HTML without a UA
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
    throw new Error(`Could not find PETSS data block (no <pre>, no Date(GMT)). Saved raw HTML to ${outRaw}`);
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
