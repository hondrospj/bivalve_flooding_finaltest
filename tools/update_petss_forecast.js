#!/usr/bin/env node
"use strict";

/**
 * Update PETSS station "ensemble mean" forecast via NOMADS PETSS prod directory.
 * We treat the PETSS "Fcst" column as the ensemble mean total water forecast.
 *
 * Outputs:
 *  - data/petss_ensemble_mean.csv   (time_utc, fcst_ft_mllw)
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

function writeText(filePath, text) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, text, "utf8");
}

function writeJSON(filePath, obj) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function fetchText(url) {
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
          "User-Agent": "petss-forecast-updater/1.0 (github-actions)",
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

// Very small CSV splitter that handles commas + optional quotes.
function splitCsvLine(line) {
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
  // NOMADS directory listings typically include links like: petss.20260131/
  const re = /petss\.(\d{8})\//g;
  const dirs = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    dirs.push({ name: `petss.${m[1]}/`, ymd: m[1] });
  }
  // unique
  const uniq = new Map();
  for (const d of dirs) uniq.set(d.name, d);
  return Array.from(uniq.values()).sort((a, b) => (a.ymd < b.ymd ? -1 : 1));
}

function listTarballsFromIndexHtml(html) {
  // Expect tarballs like: petss.t18z.csv.tar.gz
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
  // Prefer latest typical cycle order
  const prefer = ["18", "12", "06", "00"];
  for (const h of prefer) {
    const found = tars.find((x) => x.hour === h);
    if (found) return found;
  }
  // fallback: pick max hour
  tars.sort((a, b) => Number(a.hour) - Number(b.hour));
  return tars[tars.length - 1] || null;
}

function findFilesRecursive(rootDir) {
  const out = [];
  function walk(d) {
    const entries = fs.readdirSync(d, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(d, e.name);
      if (e.isDirectory()) walk(full);
      else out.push(full);
    }
  }
  walk(rootDir);
  return out;
}

function parseMMDD_HHZ(token) {
  // token example: "01/28 18Z"
  // Allow "01/28 18Z" or "01/28 18Z," (caller should trim)
  const m = token.match(/^(\d{2})\/(\d{2})\s+(\d{2})Z$/i);
  if (!m) return null;
  return { mm: Number(m[1]), dd: Number(m[2]), hh: Number(m[3]) };
}

function toIsoUtc(year, mm, dd, hh) {
  // month in JS Date is 0-based
  const dt = new Date(Date.UTC(year, mm - 1, dd, hh, 0, 0));
  return dt.toISOString();
}

function inferYearForSeries(baseYmd, points) {
  // baseYmd is like "20260131" from the prod dir name
  // station rows have no year; we infer around base date and handle year rollover (Dec/Jan).
  const baseYear = Number(baseYmd.slice(0, 4));
  const baseMonth = Number(baseYmd.slice(4, 6));
  // default year
  let year = baseYear;

  // If base is January and we see month=12 in the series => previous year
  const months = new Set(points.map((p) => p.mm));
  if (baseMonth === 1 && months.has(12)) {
    // Some products may span late Dec -> early Jan
    // We'll: If most points are Jan, keep baseYear, but allow Dec to be baseYear-1.
    return { baseYear, decYear: baseYear - 1, janYear: baseYear };
  }

  // If base is December and we see month=1 => next year
  if (baseMonth === 12 && months.has(1)) {
    return { baseYear, decYear: baseYear, janYear: baseYear + 1 };
  }

  // Otherwise all same year
  return { baseYear, decYear: baseYear, janYear: baseYear };
}

function parseStationTextBlock(stationText, baseYmd) {
  // Supports the shown PETSS format:
  // Line 1: "Bivalve NJ 8536889 (Height in Feet MLLW)"
  // Line 2: "Date(GMT),  Surge,  Tide,  Obs,  Fcst,  Anom,Fst90%,Fst10%"
  // Lines: "01/28 18Z,  0.0,  0.7,  102.4,  0.7,  0.0,  0.6,  0.7"
  const lines = stationText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  // Find header line containing Date(GMT)
  let headerIdx = -1;
  let header = null;
  for (let i = 0; i < Math.min(lines.length, 25); i++) {
    if (/date\s*\(gmt\)/i.test(lines[i])) {
      headerIdx = i;
      header = splitCsvLine(lines[i]).map((x) => x.trim());
      break;
    }
  }
  if (headerIdx < 0 || !header) {
    throw new Error("Could not find Date(GMT) header line in station text block.");
  }

  const lower = header.map((h) => h.toLowerCase().replace(/\s+/g, ""));
  const dateIdx = lower.findIndex((h) => h.includes("date(gmt)") || h === "date(gmt)");
  const fcstIdx = lower.findIndex((h) => h === "fcst");
  if (dateIdx < 0) throw new Error("Header found, but no Date(GMT) column index.");
  if (fcstIdx < 0) throw new Error("Header found, but no Fcst column index (expected PETSS ensemble mean).");

  // Parse rows after headerIdx
  const rawPoints = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (!cols || cols.length < Math.max(dateIdx, fcstIdx) + 1) continue;

    const dateToken = (cols[dateIdx] || "").trim();
    const parsed = parseMMDD_HHZ(dateToken);
    if (!parsed) continue;

    const fcstStr = (cols[fcstIdx] || "").trim();
    const fcst = Number(fcstStr);
    if (!Number.isFinite(fcst)) continue;

    rawPoints.push({ ...parsed, fcst });
  }

  if (rawPoints.length === 0) {
    throw new Error("Found Date(GMT) header but could not parse any data rows in text-block format.");
  }

  const yearMap = inferYearForSeries(baseYmd, rawPoints);
  const series = rawPoints.map((p) => {
    const y = p.mm === 12 ? yearMap.decYear : p.mm === 1 ? yearMap.janYear : yearMap.baseYear;
    return { time_utc: toIsoUtc(y, p.mm, p.dd, p.hh), fcst_ft_mllw: p.fcst };
  });

  // Sort time ascending
  series.sort((a, b) => (a.time_utc < b.time_utc ? -1 : 1));
  return series;
}

function parseAlternateCsv(stationText, baseYmd) {
  // Fallback parser for “real CSV” cases where first column is date-like.
  // Accepts:
  //  - ISO timestamps in first col
  //  - Or Date(GMT) as first header column
  const lines = stationText
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  if (lines.length < 2) throw new Error("CSV fallback: too few lines.");

  const header = splitCsvLine(lines[0]).map((x) => x.trim());
  const lower = header.map((h) => h.toLowerCase().replace(/\s+/g, ""));

  let timeIdx = 0;
  // Prefer explicit date headers if present
  const explicitDate = lower.findIndex((h) => h.includes("date") || h.includes("time"));
  if (explicitDate >= 0) timeIdx = explicitDate;

  // Ensemble mean column: prefer Fcst
  let meanIdx = lower.findIndex((h) => h === "fcst");
  if (meanIdx < 0) meanIdx = lower.findIndex((h) => h.includes("mean") || (h.includes("ens") && h.includes("mean")));
  if (meanIdx < 0) throw new Error("CSV fallback: could not locate a mean column (Fcst/mean).");

  const series = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (cols.length <= Math.max(timeIdx, meanIdx)) continue;
    const t = (cols[timeIdx] || "").trim();
    const v = Number((cols[meanIdx] || "").trim());
    if (!Number.isFinite(v)) continue;

    // Try ISO
    const dt = new Date(t);
    if (!isNaN(dt.getTime())) {
      series.push({ time_utc: dt.toISOString(), fcst_ft_mllw: v });
      continue;
    }

    // Try MM/DD HHZ format
    const parsed = parseMMDD_HHZ(t);
    if (parsed) {
      const yearMap = inferYearForSeries(baseYmd, [parsed]);
      const y = parsed.mm === 12 ? yearMap.decYear : parsed.mm === 1 ? yearMap.janYear : yearMap.baseYear;
      series.push({ time_utc: toIsoUtc(y, parsed.mm, parsed.dd, parsed.hh), fcst_ft_mllw: v });
    }
  }

  if (series.length === 0) throw new Error("CSV fallback: no recognizable data rows.");
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

    // Write tarball to tmp
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "petss-"));
    const tarPath = path.join(tmpDir, chosenTar.name);
    fs.writeFileSync(tarPath, buf);

    const extractDir = path.join(tmpDir, "extract");
    ensureDir(extractDir);

    debug.steps.push("extract tarball");
    // tar -xzf tarPath -C extractDir
    execFileSync("tar", ["-xzf", tarPath, "-C", extractDir], { stdio: "ignore" });

    debug.steps.push("find station file in extracted tree");
    const allFiles = findFilesRecursive(extractDir);

    // Prefer exact match: .../8536889.csv
    let stationFile = allFiles.find((f) => path.basename(f) === `${STID}.csv`);
    // Fallback: any file containing STID and .csv
    if (!stationFile) stationFile = allFiles.find((f) => f.includes(STID) && f.toLowerCase().endsWith(".csv"));

    if (!stationFile) {
      debug.files.extractedSample = allFiles.slice(0, 40);
      throw new Error(`Could not find station file for STID ${STID} in extracted tarball.`);
    }

    console.log("Station file:", stationFile);
    debug.files.stationFile = stationFile;

    debug.steps.push("read station text");
    const stationText = fs.readFileSync(stationFile, "utf8");

    // Always save raw station text for you to inspect in repo
    writeText(path.join(OUT_DIR, "petss_station_raw.txt"), stationText);

    if (looksLikeHtml(stationText)) {
      throw new Error("Station file looks like HTML (likely an error/blocked response), not data.");
    }

    // Parse series
    const baseYmd = latestDir.ymd;
    let series;
    debug.steps.push("parse station text (preferred text-block format)");
    try {
      series = parseStationTextBlock(stationText, baseYmd);
      debug.notes.push("Parsed using PETSS text-block format with Date(GMT) header; Fcst used as ensemble mean.");
    } catch (e1) {
      debug.notes.push(`Text-block parse failed: ${e1.message}`);
      debug.steps.push("parse station text (alternate CSV fallback)");
      series = parseAlternateCsv(stationText, baseYmd);
      debug.notes.push("Parsed using alternate CSV fallback; Fcst/mean used as ensemble mean.");
    }

    // Build outputs
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
      ensemble_mean_definition: "Fcst column from PETSS station output",
      points: series.length,
      time_start_utc: series[0]?.time_utc || null,
      time_end_utc: series[series.length - 1]?.time_utc || null,
    };

    // CSV
    const csvLines = ["time_utc,fcst_ft_mllw"];
    for (const p of series) {
      csvLines.push(`${p.time_utc},${p.fcst_ft_mllw}`);
    }
    writeText(path.join(OUT_DIR, "petss_ensemble_mean.csv"), csvLines.join("\n") + "\n");

    // JSON
    writeJSON(path.join(OUT_DIR, "petss_ensemble_mean.json"), { meta, series });

    // Meta only
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
    } catch (_) {
      // ignore
    }

    process.exit(1);
  }
}

main();
