#!/usr/bin/env node
/**
 * Update PETSS station forecast by pulling station CSV from NOMADS PETSS production tarballs.
 *
 * Outputs:
 *  - data/petss_forecast.csv   (station CSV as-is)
 *  - data/petss_forecast.json  (parsed rows)
 *  - data/petss_meta.json      (source folder/cycle/stid/datum + timestamps)
 *
 * Env:
 *  - PETSS_STID   (required) e.g. 8536889
 *  - PETSS_DATUM  (optional) e.g. MLLW (default)
 *
 * Notes:
 *  - Uses NOMADS directory listing to find latest petss.YYYYMMDD and cycle tarball petss.t??z.csv.tar.gz
 *  - Uses system `tar` (available on ubuntu-latest) to extract.
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
          "User-Agent": "petss-forecast-updater/1.0 (github-actions)",
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
  // Expect entries like: petss.20260131/
  const re = /href="(petss\.(\d{8})\/)"/g;
  const out = [];
  let m;
  while ((m = re.exec(htmlText)) !== null) {
    out.push({ folder: m[1], yyyymmdd: m[2] });
  }
  out.sort((a, b) => a.yyyymmdd.localeCompare(b.yyyymmdd));
  return out;
}

function parseApacheListingForCycles(htmlText) {
  // Expect entries like: petss.t00z.csv.tar.gz
  const re = /href="(petss\.t(\d{2})z\.csv\.tar\.gz)"/g;
  const out = [];
  let m;
  while ((m = re.exec(htmlText)) !== null) {
    out.push({ file: m[1], hh: m[2] });
  }
  // prefer latest cycle
  const order = ["18", "12", "06", "00"];
  out.sort((a, b) => order.indexOf(a.hh) - order.indexOf(b.hh));
  return out;
}

function findStationCsvFile(extractRoot, stid) {
  // Search extracted files for any .csv containing the stid in filename
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

  if (matches.length === 0) {
    // fallback: maybe the tarball is organized with a stations.csv + filter inside
    // try any csv that looks like "petss_stations.csv" or "stations.csv"
    const fallback = [];

    function walk2(dir) {
      for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, ent.name);
        if (ent.isDirectory()) walk2(full);
        else if (ent.isFile()) {
          const n = ent.name.toLowerCase();
          if (n.endsWith(".csv")) fallback.push(full);
        }
      }
    }

    walk2(extractRoot);

    return { primary: null, allCsv: fallback };
  }

  // If multiple, prefer shortest path / most direct name.
  matches.sort((a, b) => a.length - b.length);
  return { primary: matches[0], allCsv: matches };
}

function parseStationCsv(csvText) {
  // Expected header similar to:
  // Date(GMT), Surge,  Tide,   Obs,  Fcst,  Anom,Fst90%,Fst10%
  const lines = csvText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    throw new Error("Station CSV appears empty.");
  }

  const headerIdx = lines.findIndex((l) => /date\s*\(gmt\)/i.test(l));
  if (headerIdx === -1) {
    // Sometimes header could be "Date(GMT),Surge,Tide,Obs,Fcst,Anom,Fst90%,Fst10%"
    // or "Date (GMT)" with a space.
    const altIdx = lines.findIndex((l) => /date\s*\(?\s*gmt\s*\)?/i.test(l));
    if (altIdx === -1) {
      throw new Error("Could not locate a Date(GMT) header line in station CSV.");
    }
    return parseStationCsv(lines.slice(altIdx).join("\n"));
  }

  const header = lines[headerIdx]
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const dataLines = lines.slice(headerIdx + 1);

  const rows = [];
  for (const l of dataLines) {
    // Some rows might have repeated spaces after commas; split by comma first.
    const parts = l.split(",").map((s) => s.trim());
    if (parts.length < 2) continue;

    const obj = {};
    for (let i = 0; i < header.length && i < parts.length; i++) {
      obj[header[i]] = parts[i];
    }

    // Normalize fields into a stable schema:
    // Keep the original header keys too (useful for debugging), but provide canonical names.
    const dateStr =
      obj["Date(GMT)"] ||
      obj["Date (GMT)"] ||
      obj["Date"] ||
      parts[0];

    function toNum(x) {
      if (x === undefined || x === null) return null;
      const v = String(x).trim();
      if (!v || v.toLowerCase() === "nan") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }

    const canonical = {
      date_gmt: dateStr,          // e.g. "01/25 06Z"
      surge_ft: toNum(obj["Surge"] ?? obj["Surge(ft)"] ?? parts[1]),
      tide_ft: toNum(obj["Tide"] ?? obj["Tide(ft)"]),
      obs_ft: toNum(obj["Obs"] ?? obj["Obs(ft)"]),
      fcst_ft: toNum(obj["Fcst"] ?? obj["Fcst(ft)"]),
      anom_ft: toNum(obj["Anom"] ?? obj["Anom(ft)"]),
      fcst90_ft: toNum(obj["Fst90%"] ?? obj["Fcst90%"] ?? obj["Fst90"]),
      fcst10_ft: toNum(obj["Fst10%"] ?? obj["Fcst10%"] ?? obj["Fst10"]),
      raw: obj, // preserve raw mapping for safety
    };

    rows.push(canonical);
  }

  if (rows.length === 0) {
    throw new Error("Parsed 0 rows from station CSV.");
  }

  return { header, rows };
}

async function main() {
  console.log("Running PETSS forecast updater via NOMADS…");
  console.log(`STID: ${STID}`);
  console.log(`DATUM (for metadata only): ${DATUM}`);
  console.log(`Base: ${NOMADS_BASE}`);

  ensureDir(OUT_DIR);

  // 1) Find latest petss.YYYYMMDD directory
  const prodListingBuf = await httpGet(NOMADS_BASE);
  const prodListing = prodListingBuf.toString("utf8");
  const dirs = parseApacheListingForPetssDirs(prodListing);

  if (!dirs.length) {
    throw new Error("Could not find any petss.YYYYMMDD directories on NOMADS.");
  }

  const latestDir = dirs[dirs.length - 1].folder; // already sorted
  const dirUrl = NOMADS_BASE + latestDir;
  console.log(`Latest PETSS prod dir: ${latestDir}`);

  // 2) Pick latest available cycle tarball (t18z > t12z > t06z > t00z)
  const dirListingBuf = await httpGet(dirUrl);
  const dirListing = dirListingBuf.toString("utf8");
  const cycles = parseApacheListingForCycles(dirListing);

  if (!cycles.length) {
    throw new Error(`No petss.t??z.csv.tar.gz found in ${dirUrl}`);
  }

  // cycles sorted by our preference ordering already (18 first)
  const chosen = cycles[0];
  const tarUrl = dirUrl + chosen.file;
  console.log(`Chosen cycle tarball: ${chosen.file}`);
  console.log(`Downloading: ${tarUrl}`);

  // 3) Download tarball to temp
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "petss-"));
  const tarPath = path.join(tmpRoot, chosen.file);
  const tarBuf = await httpGet(tarUrl);
  fs.writeFileSync(tarPath, tarBuf);

  // 4) Extract tarball
  const extractDir = path.join(tmpRoot, "extract");
  fs.mkdirSync(extractDir);
  try {
    execSync(`tar -xzf "${tarPath}" -C "${extractDir}"`, { stdio: "pipe" });
  } catch (e) {
    throw new Error(
      `Failed to extract tarball via tar. ${e?.message || e}`
    );
  }

  // 5) Locate station csv
  const found = findStationCsvFile(extractDir, STID);
  if (!found.primary) {
    const sample = found.allCsv.slice(0, 20).map((p) => path.basename(p));
    throw new Error(
      `Could not find a station CSV containing "${STID}" in filename after extracting tarball.\n` +
        `Found CSV files (sample): ${sample.join(", ")}`
    );
  }

  console.log(`Station CSV file: ${found.primary}`);

  const stationCsvText = fs.readFileSync(found.primary, "utf8");

  // 6) Parse & write outputs
  const parsed = parseStationCsv(stationCsvText);

  fs.writeFileSync(OUT_CSV, stationCsvText, "utf8");
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
    header: parsed.header,
  };
  fs.writeFileSync(OUT_META, JSON.stringify(meta, null, 2) + "\n", "utf8");

  console.log("Wrote:");
  console.log(`- ${OUT_CSV}`);
  console.log(`- ${OUT_JSON}`);
  console.log(`- ${OUT_META}`);
  console.log(`Rows: ${parsed.rows.length}`);

  // Clean up temp (best-effort)
  try {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  } catch (_) {}
}

main().catch((err) => {
  console.error("PETSS update failed:", err && err.stack ? err.stack : err);
  process.exit(1);
});
