#!/usr/bin/env node
"use strict";

/**
 * PETSS station updater via NOMADS.
 *
 * This version supports the NOMADS station CSV format like:
 *   TIME, TIDE, OB, SURGE, BIAS, TWL, SURGE90p, TWL90p, SURGE10p, TWL10p
 *   202601261800, 3.376, 102.385, 0.900, 0.000, 4.276, ...
 *
 * We treat TWL as the "ensemble mean" (total water level).
 *
 * Outputs:
 *  - data/petss_ensemble_mean.csv   (time_utc,twl_ft_mllw)
 *  - data/petss_ensemble_mean.json  ({ meta, series[] })
 *  - data/petss_meta.json           (metadata)
 *  - data/petss_station_raw.txt     (raw station file text for inspection)
 *  - data/petss_debug_info.json     (debug/tracing info)
 *  - data/petss_error.txt           (if failure)
 */

const fs = require("fs");
const path = require("path");
const os = require("os");
const https = require("https");
const { execFileSync } = require("child_process");

const STID = process.env.PETSS_STID || "";
const DATUM = process.env.PETSS_DATUM || "MLLW"; // metadata only
const OUT_DIR = path.resolve("data");

if (!STID || !/^\d+$/.test(STID)) {
  console.error("ERROR: PETSS_STID env var is required and must be numeric (e.g., 8536889).");
  process.exit(1);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeText(fp, text) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, text, "utf8");
}

function writeJSON(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          "User-Agent": "petss-forecast-updater/2.0 (github-actions)",
          "Accept": "text/html,application/octet-stream,*/*",
        },
      },
      (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(data);
          } else {
            reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          }
        });
      }
    );
    req.on("error", reject);
  });
}

function fetchBuffer(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          "User-Agent": "petss-forecast-updater/2.0 (github-actions)",
          "Accept": "application/octet-stream,*/*",
        },
      },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(buf);
          } else {
            reject(new Error(`HTTP ${res.statusCode} for ${url} (bytes=${buf.length})`));
          }
        });
      }
    );
    req.on("error", reject);
  });
}

function splitCsvLine(line) {
  // Simple CSV splitter with quotes support
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQ = !inQ;
      continue;
    }
    if (ch === "," && !inQ) {
      out.push(cur.trim());
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur.trim());
  return out;
}

function looksLikeHtml(text) {
  const t = (text || "").trim().toLowerCase();
  return t.startsWith("<!doctype") || t.startsWith("<html") || t.includes("<head") || t.includes("</html>");
}

function listDirsFromIndexHtml(html) {
  const re = /petss\.(\d{8})\//g;
  const dirs = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    dirs.push({ name: `petss.${m[1]}/`, ymd: m[1] });
  }
  const uniq = new Map();
  for (const d of dirs) uniq.set(d.name, d);
  return Array.from(uniq.values()).sort((a, b) => (a.ymd < b.ymd ? -1 : 1));
}

function listTarballsFromIndexHtml(html) {
  const re = /petss\.t(\d{2})z\.csv\.tar\.gz/g;
  const tars = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    tars.push({ hour: m[1], name: `petss.t${m[1]}z.csv.tar.gz` });
  }
  const uniq = new Map();
  for (const t of tars) uniq.set(t.name, t);
  return Array.from(uniq.values());
}

function chooseBestCycleTarball(tars) {
  const prefer = ["18", "12", "06", "00"];
  for (const h of prefer) {
    const found = tars.find((x) => x.hour === h);
    if (found) return found;
  }
  tars.sort((a, b) => Number(a.hour) - Number(b.hour));
  return tars[tars.length - 1] || null;
}

function findFilesRecursive(rootDir) {
  const out = [];
  function walk(d) {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const full = path.join(d, e.name);
      if (e.isDirectory()) walk(full);
      else out.push(full);
    }
  }
  walk(rootDir);
  return out;
}

function parseTimeYYYYMMDDHHMM(s) {
  // Example: 202601261800 (YYYYMMDDHHMM)
  const m = String(s).trim().match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})$/);
  if (!m) return null;
  const yyyy = Number(m[1]);
  const mm = Number(m[2]);
  const dd = Number(m[3]);
  const hh = Number(m[4]);
  const min = Number(m[5]);
  const dt = new Date(Date.UTC(yyyy, mm - 1, dd, hh, min, 0));
  if (isNaN(dt.getTime())) return null;
  return dt.toISOString();
}

function isMissingValue(x) {
  // PETSS missing convention often uses 9999.000
  return !Number.isFinite(x) || Math.abs(x - 9999.0) < 1e-6;
}

function parseStationCsvFormat(text) {
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  if (lines.length < 2) throw new Error("Station file too short to parse.");

  // Find header line containing TIME and TWL
  let headerIdx = -1;
  let header = null;
  for (let i = 0; i < Math.min(lines.length, 20); i++) {
    const low = lines[i].toLowerCase().replace(/\s+/g, "");
    if (low.startsWith("time,") && low.includes(",twl")) {
      headerIdx = i;
      header = splitCsvLine(lines[i]);
      break;
    }
  }
  if (headerIdx < 0 || !header) {
    throw new Error('Could not find a header line like "TIME,...,TWL,..."');
  }

  const norm = header.map((h) => h.toLowerCase().replace(/\s+/g, ""));
  const timeIdx = norm.indexOf("time");
  const twlIdx = norm.indexOf("twl");

  if (timeIdx < 0) throw new Error("Header present but TIME column not found.");
  if (twlIdx < 0) throw new Error("Header present but TWL column not found.");

  const series = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (cols.length <= Math.max(timeIdx, twlIdx)) continue;

    const iso = parseTimeYYYYMMDDHHMM(cols[timeIdx]);
    if (!iso) continue;

    const twl = Number(String(cols[twlIdx]).trim());
    if (isMissingValue(twl)) continue;

    series.push({ time_utc: iso, twl_ft_mllw: twl });
  }

  if (series.length === 0) {
    throw new Error("No valid rows parsed (all missing/invalid?).");
  }

  series.sort((a, b) => (a.time_utc < b.time_utc ? -1 : 1));
  return series;
}

async function main() {
  ensureDir(OUT_DIR);

  const base = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod/";
  console.log("Running PETSS forecast updater via NOMADS...");
  console.log("STID:", STID);
  console.log("DATUM (for metadata only):", DATUM);
  console.log("Base:", base);

  const debug = {
    stid: STID,
    datum: DATUM,
    base,
    steps: [],
    chosen: {},
    files: {},
    notes: [],
  };

  try {
    debug.steps.push("fetch base index");
    const baseIndex = await fetchText(base);

    debug.steps.push("parse latest petss.YYYYMMDD/ dir");
    const dirs = listDirsFromIndexHtml(baseIndex);
    if (dirs.length === 0) throw new Error("No petss.YYYYMMDD/ directories found at base.");

    const latestDir = dirs[dirs.length - 1];
    const latestDirUrl = base + latestDir.name;

    console.log("Latest PETSS prod dir:", latestDir.name);
    debug.chosen.latestDir = latestDir;

    debug.steps.push("fetch latest dir index");
    const dirIndex = await fetchText(latestDirUrl);

    debug.steps.push("parse available cycle tarballs");
    const tars = listTarballsFromIndexHtml(dirIndex);
    if (tars.length === 0) throw new Error("No petss.t??z.csv.tar.gz tarballs found in latest prod dir.");

    const chosenTar = chooseBestCycleTarball(tars);
    if (!chosenTar) throw new Error("Could not choose a cycle tarball.");

    console.log("Chosen cycle tarball:", chosenTar.name);
    debug.chosen.tarball = chosenTar;

    const tarUrl = latestDirUrl + chosenTar.name;
    console.log("Downloading:", tarUrl);
    debug.chosen.tarUrl = tarUrl;

    debug.steps.push("download tarball bytes");
    const buf = await fetchBuffer(tarUrl);

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "petss-"));
    const tarPath = path.join(tmpDir, chosenTar.name);
    fs.writeFileSync(tarPath, buf);

    const extractDir = path.join(tmpDir, "extract");
    ensureDir(extractDir);

    debug.steps.push("extract tarball");
    execFileSync("tar", ["-xzf", tarPath, "-C", extractDir], { stdio: "ignore" });

    debug.steps.push("find station file in extracted tree");
    const allFiles = findFilesRecursive(extractDir);

    let stationFile = allFiles.find((f) => path.basename(f) === `${STID}.csv`);
    if (!stationFile) stationFile = allFiles.find((f) => f.includes(STID) && f.toLowerCase().endsWith(".csv"));

    if (!stationFile) {
      debug.files.extractedSample = allFiles.slice(0, 60);
      throw new Error(`Could not find station file for STID ${STID} in extracted tarball.`);
    }

    console.log("Station CSV file:", stationFile);
    debug.files.stationFile = stationFile;

    debug.steps.push("read station text");
    const stationText = fs.readFileSync(stationFile, "utf8");

    // Save raw station file always
    writeText(path.join(OUT_DIR, "petss_station_raw.txt"), stationText);

    if (looksLikeHtml(stationText)) {
      throw new Error("Station file looks like HTML (blocked/error response), not data.");
    }

    debug.steps.push("parse station CSV format (TIME/TWL)");
    const series = parseStationCsvFormat(stationText);
    debug.notes.push("Parsed NOMADS station format using TIME + TWL (ensemble mean total water level).");

    const generatedAt = new Date().toISOString();

    const meta = {
      station_id: STID,
      datum: DATUM,
      source: "NOMADS PETSS prod",
      prod_dir: latestDir.name.replace(/\/$/, ""),
      cycle_tarball: chosenTar.name,
      tar_url: tarUrl,
      generated_at_utc: generatedAt,
      units: "feet (MLLW)",
      ensemble_mean_definition: "TWL column from PETSS station output",
      points: series.length,
      time_start_utc: series[0]?.time_utc || null,
      time_end_utc: series[series.length - 1]?.time_utc || null,
    };

    // CSV output
    const csvLines = ["time_utc,twl_ft_mllw"];
    for (const p of series) {
      csvLines.push(`${p.time_utc},${p.twl_ft_mllw}`);
    }
    writeText(path.join(OUT_DIR, "petss_ensemble_mean.csv"), csvLines.join("\n") + "\n");

    // JSON output
    writeJSON(path.join(OUT_DIR, "petss_ensemble_mean.json"), { meta, series });

    // Meta output
    writeJSON(path.join(OUT_DIR, "petss_meta.json"), meta);

    // Debug info
    debug.steps.push("write debug info");
    writeJSON(path.join(OUT_DIR, "petss_debug_info.json"), debug);

    console.log(`Wrote ${series.length} points to data/petss_ensemble_mean.csv and .json`);
    process.exit(0);
  } catch (err) {
    const msg = `PETSS update failed: ${err && err.message ? err.message : String(err)}`;
    console.error(msg);

    try {
      debug.error = msg;
      writeJSON(path.join(OUT_DIR, "petss_debug_info.json"), debug);
      writeText(path.join(OUT_DIR, "petss_error.txt"), msg + "\n");
    } catch (_) {}

    process.exit(1);
  }
}

main();
