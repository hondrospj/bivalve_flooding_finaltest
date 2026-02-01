#!/usr/bin/env node
/**
 * Update PETSS station forecast by pulling station CSV from NOMADS PETSS production tarballs.
 *
 * Outputs:
 *  - data/petss_forecast.csv        (station file as-is)
 *  - data/petss_forecast.json       (parsed rows)
 *  - data/petss_meta.json           (source folder/cycle/stid/datum + timestamps)
 *  - data/petss_station_debug.txt   (only written if parse fails; first lines for debugging)
 *
 * Env:
 *  - PETSS_STID   (required) e.g. 8536889
 *  - PETSS_DATUM  (optional) e.g. MLLW (default; metadata only)
 */

const fs = require("fs");
const path = require("path");
const os = require("os");
const { execSync } = require("child_process");
const https = require("https");

const STID = (process.env.PETSS_STID || "").trim();
const DATUM = (process.env.PETSS_DATUM || "MLLW").trim();

if (!STID) {
  console.error("ERROR: PETSS_STID env var is required (e.g., 8536889).");
  process.exit(1);
}

const OUT_DIR = path.resolve("data");
const OUT_CSV = path.join(OUT_DIR, "petss_forecast.csv");
const OUT_JSON = path.join(OUT_DIR, "petss_forecast.json");
const OUT_META = path.join(OUT_DIR, "petss_meta.json");
const OUT_DEBUG = path.join(OUT_DIR, "petss_station_debug.txt");

const NOMADS_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod/";

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          "User-Agent": "petss-forecast-updater/1.1 (github-actions)",
          "Accept": "text/html,application/octet-stream,*/*",
        },
      },
      (res) => {
        const { statusCode } = res;
        if (statusCode < 200 || statusCode >= 300) {
          const chunks = [];
          res.on("data", (d) => chunks.push(d));
          res.on("end", () => {
            const body = Buffer.concat(chunks).toString("utf8");
            reject(
              new Error(
                `HTTP ${statusCode} for ${url}\n--- body (first 500) ---\n${body.slice(
                  0,
                  500
                )}`
              )
            );
          });
          return;
        }
        const data = [];
        res.on("data", (d) => data.push(d));
        res.on("end", () => resolve(Buffer.concat(data)));
      }
    );
    req.on("error", reject);
  });
}

function parseApacheListingForPetssDirs(htmlText) {
  const re = /href="(petss\.(\d{8})\/)"/g;
  const out = [];
  let m;
  while ((m = re.exec(htmlText)) !== null) out.push({ folder: m[1], yyyymmdd: m[2] });
  out.sort((a, b) => a.yyyymmdd.localeCompare(b.yyyymmdd));
  return out;
}

function parseApacheListingForCycles(htmlText) {
  const re = /href="(petss\.t(\d{2})z\.csv\.tar\.gz)"/g;
  const out = [];
  let m;
  while ((m = re.exec(htmlText)) !== null) out.push({ file: m[1], hh: m[2] });

  const order = ["18", "12", "06", "00"]; // prefer latest
  out.sort((a, b) => order.indexOf(a.hh) - order.indexOf(b.hh));
  return out;
}

function findStationCsvFile(extractRoot, stid) {
  const matches = [];
  function walk(dir) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) walk(full);
      else if (ent.isFile()) {
        const n = ent.name.toLowerCase();
        if (n.endsWith(".csv") && n.includes(String(stid))) matches.push(full);
      }
    }
  }
  walk(extractRoot);

  if (!matches.length) {
    const allCsv = [];
    function walk2(dir) {
      for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, ent.name);
        if (ent.isDirectory()) walk2(full);
        else if (ent.isFile()) {
          const n = ent.name.toLowerCase();
          if (n.endsWith(".csv")) allCsv.push(full);
        }
      }
    }
    walk2(extractRoot);
    return { primary: null, allCsv };
  }

  matches.sort((a, b) => a.length - b.length);
  return { primary: matches[0], allCsv: matches };
}

function looksLikeDataRowFirstCol(x) {
  const s = String(x || "").trim();
  if (!s) return false;

  // PETSS-ish examples:
  // "01/25 06Z"
  // "2026-01-31 18:00"
  // "20260131 18Z" / "20260131 18"
  if (/^\d{2}\/\d{2}\s+\d{2}Z$/i.test(s)) return true;
  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(s)) return true;
  if (/^\d{8}\s+\d{2}Z?$/i.test(s)) return true;
  return false;
}

function detectDelimiter(sampleLine) {
  if (sampleLine.includes(",")) return ",";
  // lots of PETSS dumps are whitespace-separated
  if (/\s+/.test(sampleLine)) return "ws";
  return ",";
}

function splitLine(line, delim) {
  if (delim === ",") return line.split(",").map((s) => s.trim());
  // whitespace
  return line.trim().split(/\s+/).map((s) => s.trim());
}

function toNum(x) {
  if (x === undefined || x === null) return null;
  const v = String(x).trim();
  if (!v || v.toLowerCase() === "nan" || v === "--") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseStationFile(text) {
  // Normalize and keep non-empty lines
  const rawLines = text.split(/\r?\n/);
  const lines = rawLines.map((l) => l.trim()).filter((l) => l.length > 0);

  if (lines.length < 2) throw new Error("Station file appears empty.");

  // Find a header line if it exists (Date(GMT) or similar)
  let headerIdx = lines.findIndex((l) => /date\s*\(\s*gmt\s*\)/i.test(l));
  if (headerIdx === -1) headerIdx = lines.findIndex((l) => /^date\b/i.test(l));
  if (headerIdx === -1) headerIdx = lines.findIndex((l) => /valid/i.test(l) && /time/i.test(l));

  // If header exists, use it. If not, infer schema from the first data-ish line.
  if (headerIdx !== -1) {
    const headerLine = lines[headerIdx];
    const delim = detectDelimiter(headerLine);
    const header = splitLine(headerLine, delim).filter(Boolean);

    const dataLines = lines.slice(headerIdx + 1);
    const rows = [];

    for (const l of dataLines) {
      const parts = splitLine(l, delim);
      if (parts.length < 2) continue;

      const obj = {};
      for (let i = 0; i < header.length && i < parts.length; i++) obj[header[i]] = parts[i];

      const dateStr =
        obj["Date(GMT)"] ||
        obj["Date (GMT)"] ||
        obj["Date"] ||
        obj["ValidTime"] ||
        obj["Valid_Time"] ||
        parts[0];

      rows.push({
        date_gmt: dateStr,
        surge_ft: toNum(obj["Surge"] ?? obj["Surge(ft)"] ?? parts[1]),
        tide_ft: toNum(obj["Tide"] ?? obj["Tide(ft)"] ?? parts[2]),
        obs_ft: toNum(obj["Obs"] ?? obj["Obs(ft)"] ?? parts[3]),
        fcst_ft: toNum(obj["Fcst"] ?? obj["Fcst(ft)"] ?? parts[4]),
        anom_ft: toNum(obj["Anom"] ?? obj["Anom(ft)"] ?? parts[5]),
        fcst90_ft: toNum(obj["Fst90%"] ?? obj["Fcst90%"] ?? obj["Fst90"] ?? parts[6]),
        fcst10_ft: toNum(obj["Fst10%"] ?? obj["Fcst10%"] ?? obj["Fst10"] ?? parts[7]),
        raw: obj,
      });
    }

    if (!rows.length) throw new Error("Parsed 0 rows from header-based station file.");
    return { header, rows, mode: "header" };
  }

  // No header: infer from first data-like line.
  // Find first line whose first token looks like a date/time.
  const firstDataIdx = lines.findIndex((l) => {
    const delim = detectDelimiter(l);
    const parts = splitLine(l, delim);
    return looksLikeDataRowFirstCol(parts[0]);
  });

  if (firstDataIdx === -1) {
    throw new Error("Could not locate any recognizable data rows (no header, no date-like first column).");
  }

  const sample = lines[firstDataIdx];
  const delim = detectDelimiter(sample);

  // Assume canonical order when headerless:
  // date, surge, tide, obs, fcst, anom, fcst90, fcst10
  const header = ["date_gmt", "surge_ft", "tide_ft", "obs_ft", "fcst_ft", "anom_ft", "fcst90_ft", "fcst10_ft"];

  const rows = [];
  for (let i = firstDataIdx; i < lines.length; i++) {
    const l = lines[i];
    const parts = splitLine(l, delim);

    // Some formats split date into 2 columns (e.g., "01/25" "06Z")
    // If first two tokens make the "01/25 06Z" pattern, stitch them.
    let dateStr = parts[0];
    let offset = 0;

    if (parts.length >= 2 && /^\d{2}\/\d{2}$/.test(parts[0]) && /^\d{2}Z$/i.test(parts[1])) {
      dateStr = `${parts[0]} ${parts[1]}`;
      offset = 1;
    }

    // If "YYYYMMDD HH" style, stitch too
    if (parts.length >= 2 && /^\d{8}$/.test(parts[0]) && /^\d{2}Z?$/i.test(parts[1])) {
      dateStr = `${parts[0]} ${parts[1].toUpperCase().replace(/Z?$/, "Z")}`;
      offset = 1;
    }

    // Need at least date + 4 numbers to be useful
    if (!looksLikeDataRowFirstCol(dateStr) && !/^\d{2}\/\d{2}\s+\d{2}Z$/i.test(dateStr) && !/^\d{8}\s+\d{2}Z$/i.test(dateStr)) {
      // skip junk lines
      continue;
    }

    const nums = parts.slice(1 + offset);

    rows.push({
      date_gmt: dateStr,
      surge_ft: toNum(nums[0]),
      tide_ft: toNum(nums[1]),
      obs_ft: toNum(nums[2]),
      fcst_ft: toNum(nums[3]),
      anom_ft: toNum(nums[4]),
      fcst90_ft: toNum(nums[5]),
      fcst10_ft: toNum(nums[6]),
      raw: { parts },
    });
  }

  if (!rows.length) throw new Error("Parsed 0 rows from headerless station file.");
  return { header, rows, mode: "inferred" };
}

async function main() {
  console.log("Running PETSS forecast updater via NOMADS…");
  console.log(`STID: ${STID}`);
  console.log(`DATUM (for metadata only): ${DATUM}`);
  console.log(`Base: ${NOMADS_BASE}`);

  ensureDir(OUT_DIR);

  // 1) Find latest petss.YYYYMMDD directory
  const prodListing = (await httpGet(NOMADS_BASE)).toString("utf8");
  const dirs = parseApacheListingForPetssDirs(prodListing);

  if (!dirs.length) throw new Error("Could not find any petss.YYYYMMDD directories on NOMADS.");

  const latestDir = dirs[dirs.length - 1].folder;
  const dirUrl = NOMADS_BASE + latestDir;
  console.log(`Latest PETSS prod dir: ${latestDir}`);

  // 2) Pick latest available cycle tarball
  const dirListing = (await httpGet(dirUrl)).toString("utf8");
  const cycles = parseApacheListingForCycles(dirListing);
  if (!cycles.length) throw new Error(`No petss.t??z.csv.tar.gz found in ${dirUrl}`);

  const chosen = cycles[0];
  const tarUrl = dirUrl + chosen.file;
  console.log(`Chosen cycle tarball: ${chosen.file}`);
  console.log(`Downloading: ${tarUrl}`);

  // 3) Download tarball to temp
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "petss-"));
  const tarPath = path.join(tmpRoot, chosen.file);
  fs.writeFileSync(tarPath, await httpGet(tarUrl));

  // 4) Extract tarball
  const extractDir = path.join(tmpRoot, "extract");
  fs.mkdirSync(extractDir);
  execSync(`tar -xzf "${tarPath}" -C "${extractDir}"`, { stdio: "pipe" });

  // 5) Locate station csv
  const found = findStationCsvFile(extractDir, STID);
  if (!found.primary) {
    const sample = found.allCsv.slice(0, 30).map((p) => path.basename(p));
    throw new Error(
      `Could not find a station CSV containing "${STID}" in filename after extracting tarball.\n` +
        `Found CSV files (sample): ${sample.join(", ")}`
    );
  }
  console.log(`Station CSV file: ${found.primary}`);

  const stationText = fs.readFileSync(found.primary, "utf8");

  // Write raw station file as-is (so you can inspect in repo if needed)
  fs.writeFileSync(OUT_CSV, stationText, "utf8");

  // 6) Parse & write outputs (with debug-on-fail)
  let parsed;
  try {
    parsed = parseStationFile(stationText);
  } catch (err) {
    // Save debug snippet for easy troubleshooting from Actions artifacts / committed file (if you want to commit it)
    const lines = stationText.split(/\r?\n/);
    const head = lines.slice(0, 120).join("\n");
    fs.writeFileSync(
      OUT_DEBUG,
      [
        `PARSE FAILED for STID=${STID}`,
        `Error: ${err?.message || err}`,
        "",
        "----- FIRST 120 LINES OF STATION FILE -----",
        head,
        "",
      ].join("\n"),
      "utf8"
    );
    throw err;
  }

  fs.writeFileSync(OUT_JSON, JSON.stringify(parsed.rows, null, 2) + "\n", "utf8");

  const meta = {
    stid: STID,
    datum: DATUM,
    nomads_prod_dir: latestDir.replace(/\/$/, ""),
    cycle: `t${chosen.hh}z`,
    tarball: chosen.file,
    source_dir_url: dirUrl,
    source_tar_url: tarUrl,
    updated_utc_iso: new Date().toISOString(),
    row_count: parsed.rows.length,
    parse_mode: parsed.mode,
    header: parsed.header,
  };
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2) + "\n", "utf8");

  console.log("Wrote:");
  console.log(`- ${OUT_CSV}`);
  console.log(`- ${OUT_JSON}`);
  console.log(`- ${OUT_META}`);
  console.log(`Rows: ${parsed.rows.length}`);
  console.log(`Parse mode: ${parsed.mode}`);

  // Cleanup temp
  try {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  } catch (_) {}
}

main().catch((err) => {
  console.error("PETSS update failed:", err && err.stack ? err.stack : err);
  process.exit(1);
});
