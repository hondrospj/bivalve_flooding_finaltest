/**
 * Update PETSS forecast from NOMADS station CSV tarballs.
 *
 * Outputs:
 *  - data/petss_forecast.csv   (time_utc, ensemble_mean_ft)
 *  - data/petss_forecast.json  ({ updated_utc, stid, cycle, prod_dir, rows:[...] })
 *  - data/petss_meta.json      (metadata + status)
 *
 * Behavior:
 *  - Tries latest prod dir from NOMADS listing (petss.YYYYMMDD/)
 *  - Tries cycles in order: t18z, t12z, t06z, t00z
 *  - Downloads tarball: petss.<cycle>.csv.tar.gz
 *  - Extracts and locates <stid>.csv anywhere in extracted tree
 *  - Parses time column + ensemble mean column (or computes mean across member cols)
 *  - If parsing finds 0 rows, writes meta with error but exits 0 (so Actions doesn't hard-fail)
 */

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const os = require("os");
const https = require("https");
const { execFileSync } = require("child_process");

const BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod/";

const STID = process.env.PETSS_STID?.trim() || "8536889";
const DATUM = process.env.PETSS_DATUM?.trim() || "MLLW"; // metadata only

const OUT_DIR = "data";
const OUT_CSV = path.join(OUT_DIR, "petss_forecast.csv");
const OUT_JSON = path.join(OUT_DIR, "petss_forecast.json");
const OUT_META = path.join(OUT_DIR, "petss_meta.json");

function log(...a) {
  console.log(...a);
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "User-Agent": "petss-updater/1.0" } }, (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve(data);
          } else {
            reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          }
        });
      })
      .on("error", reject);
  });
}

function fetchToFile(url, filepath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filepath);
    https
      .get(url, { headers: { "User-Agent": "petss-updater/1.0" } }, (res) => {
        if (!(res.statusCode && res.statusCode >= 200 && res.statusCode < 300)) {
          file.close(() => {});
          fs.unlink(filepath, () => {});
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          return;
        }
        res.pipe(file);
        file.on("finish", () => file.close(resolve));
      })
      .on("error", (err) => {
        file.close(() => {});
        fs.unlink(filepath, () => {});
        reject(err);
      });
  });
}

async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

function parseProdDirsFromListing(html) {
  // NOMADS listing is simple HTML with links like petss.20260131/
  const re = /petss\.(\d{8})\//g;
  const out = [];
  let m;
  while ((m = re.exec(html))) out.push(m[1]);
  // unique + sort desc
  return Array.from(new Set(out)).sort((a, b) => (a < b ? 1 : -1));
}

async function pickLatestProdDir() {
  const html = await fetchText(BASE);
  const dirs = parseProdDirsFromListing(html);
  if (!dirs.length) throw new Error("Could not locate any petss.YYYYMMDD/ directories in NOMADS listing.");
  return `petss.${dirs[0]}/`;
}

async function urlExists(url) {
  // lightweight HEAD-ish: use GET but abort early is messy; just GET and accept status
  return new Promise((resolve) => {
    https
      .get(url, { method: "GET", headers: { "User-Agent": "petss-updater/1.0" } }, (res) => {
        // consume minimal
        res.resume();
        resolve(!!(res.statusCode && res.statusCode >= 200 && res.statusCode < 300));
      })
      .on("error", () => resolve(false));
  });
}

async function pickCycleTarball(prodDir) {
  const cycles = ["t18z", "t12z", "t06z", "t00z"];
  for (const cyc of cycles) {
    const url = `${BASE}${prodDir}petss.${cyc}.csv.tar.gz`;
    if (await urlExists(url)) return { cycle: cyc, url };
  }
  throw new Error(`No cycle tarball found in ${prodDir} (tried t18z/t12z/t06z/t00z).`);
}

async function findFileRecursive(root, filename) {
  const stack = [root];
  while (stack.length) {
    const cur = stack.pop();
    const ents = await fsp.readdir(cur, { withFileTypes: true });
    for (const e of ents) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(full);
      else if (e.isFile() && e.name === filename) return full;
    }
  }
  return null;
}

function splitCsvLine(line) {
  // Simple CSV split supporting quotes (good enough for numeric station files)
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      // double quote inside quoted field
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQ = !inQ;
      }
    } else if (ch === "," && !inQ) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

function looksLikeDateToken(s) {
  const t = s.trim();
  if (!t) return false;
  // examples:
  // 01/25 06Z
  // 2026-01-31 18:00
  // 2026-01-31T18:00Z
  // 2026013118
  if (/^\d{2}\/\d{2}\s+\d{2}Z$/i.test(t)) return true;
  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}(:\d{2})?(:\d{2})?(Z)?$/i.test(t)) return true;
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?Z$/i.test(t)) return true;
  if (/^\d{10}$/.test(t)) return true; // yyyymmddhh
  return false;
}

function parseTimeToken(token, prodDateYYYYMMDD) {
  const s = token.trim();

  // MM/DD HHZ (assume year from prod dir)
  if (/^\d{2}\/\d{2}\s+\d{2}Z$/i.test(s)) {
    const [md, hz] = s.split(/\s+/);
    const [mm, dd] = md.split("/").map((x) => parseInt(x, 10));
    const hh = parseInt(hz.replace(/Z/i, ""), 10);

    const year = parseInt(prodDateYYYYMMDD.slice(0, 4), 10);
    // UTC date
    return new Date(Date.UTC(year, mm - 1, dd, hh, 0, 0));
  }

  // YYYY-MM-DD HH:mm(:ss)
  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}/.test(s)) {
    // Normalize space to T, and ensure Z if not present (these files are typically GMT)
    const norm = s.includes("T") ? s : s.replace(" ", "T");
    const withZ = /Z$/i.test(norm) ? norm : `${norm}Z`;
    const d = new Date(withZ);
    if (!isNaN(d)) return d;
  }

  // YYYY-MM-DDTHH:mmZ already handled above
  // yyyymmddhh
  if (/^\d{10}$/.test(s)) {
    const y = parseInt(s.slice(0, 4), 10);
    const m = parseInt(s.slice(4, 6), 10);
    const d = parseInt(s.slice(6, 8), 10);
    const h = parseInt(s.slice(8, 10), 10);
    return new Date(Date.UTC(y, m - 1, d, h, 0, 0));
  }

  return null;
}

function toIsoUtc(d) {
  return d.toISOString().replace(".000Z", "Z");
}

function isNumeric(x) {
  if (x === null || x === undefined) return false;
  const s = String(x).trim();
  if (!s) return false;
  const n = Number(s);
  return Number.isFinite(n);
}

function pickTimeColumnIndex(header) {
  if (!header) return 0;
  const lower = header.map((h) => h.toLowerCase());
  let idx = lower.findIndex((h) => h.includes("date"));
  if (idx >= 0) return idx;
  idx = lower.findIndex((h) => h.includes("time"));
  if (idx >= 0) return idx;
  return 0;
}

function pickEnsembleMeanIndex(header) {
  if (!header) return -1;
  const lower = header.map((h) => h.toLowerCase().replace(/\s+/g, ""));
  // common candidates
  const candidates = [
    "ensemblemean",
    "ensmean",
    "mean",
    "mean(ft)",
    "meanfeet",
    "ens_mean",
    "ensmean(ft)",
    "ens_mean(ft)",
  ];
  for (const c of candidates) {
    const i = lower.findIndex((h) => h === c);
    if (i >= 0) return i;
  }
  // looser match: contains both ens and mean, or exactly "mean"
  let i = lower.findIndex((h) => h.includes("ens") && h.includes("mean"));
  if (i >= 0) return i;
  i = lower.findIndex((h) => h === "mean");
  if (i >= 0) return i;

  return -1;
}

function findMemberColumns(header, timeIdx) {
  // If no explicit mean column, we’ll compute mean over numeric columns that look like members.
  // Heuristics: header contains "ens" or "member" or looks like "p01"/"m01", etc.
  if (!header) return [];
  const lower = header.map((h) => h.toLowerCase());
  const idxs = [];
  for (let i = 0; i < header.length; i++) {
    if (i === timeIdx) continue;
    const h = lower[i];
    if (h.includes("ens") || h.includes("member") || /^m\d+$/i.test(h) || /^ens\d+$/i.test(h)) {
      idxs.push(i);
    }
  }
  return idxs;
}

function parseStationCsv(text, prodDateYYYYMMDD) {
  const linesRaw = text.split(/\r?\n/);

  // Remove obvious junk lines
  const lines = linesRaw
    .map((l) => l.trim())
    .filter((l) => l.length > 0)
    .filter((l) => !l.startsWith("#"));

  if (!lines.length) {
    return { header: null, rows: [], error: "Empty station file after trimming." };
  }

  // Determine if first useful line is header or data
  const first = splitCsvLine(lines[0]);

  let header = null;
  let startData = 0;

  const firstHasDateWord = first.some((c) => /date/i.test(c));
  const firstLooksData = looksLikeDateToken(first[0]);

  if (firstHasDateWord && !firstLooksData) {
    header = first;
    startData = 1;
  } else {
    // Sometimes there are 1–3 metadata lines before header; search for header containing "date"
    let foundHeaderAt = -1;
    for (let i = 0; i < Math.min(lines.length, 10); i++) {
      const cols = splitCsvLine(lines[i]);
      if (cols.some((c) => /date/i.test(c)) && !looksLikeDateToken(cols[0])) {
        foundHeaderAt = i;
        header = cols;
        startData = i + 1;
        break;
      }
    }

    if (foundHeaderAt === -1) {
      // treat as data-only, header unknown
      header = null;
      startData = 0;
    }
  }

  const timeIdx = pickTimeColumnIndex(header);
  const meanIdx = pickEnsembleMeanIndex(header);
  const memberIdxs = meanIdx === -1 ? findMemberColumns(header, timeIdx) : [];

  const out = [];

  for (let i = startData; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (!cols.length) continue;

    const tTok = cols[timeIdx] ?? cols[0];
    if (!looksLikeDateToken(tTok)) continue;

    const d = parseTimeToken(tTok, prodDateYYYYMMDD);
    if (!d || isNaN(d)) continue;

    let mean = null;

    if (meanIdx !== -1 && cols[meanIdx] !== undefined && isNumeric(cols[meanIdx])) {
      mean = Number(cols[meanIdx]);
    } else if (memberIdxs.length) {
      const nums = memberIdxs.map((ix) => cols[ix]).filter(isNumeric).map(Number);
      if (nums.length) {
        mean = nums.reduce((a, b) => a + b, 0) / nums.length;
      }
    } else {
      // fallback: if header unknown, attempt "last numeric column"
      const nums = cols.filter(isNumeric).map(Number);
      if (nums.length) mean = nums[nums.length - 1];
    }

    if (mean === null || !Number.isFinite(mean)) continue;

    out.push({ time_utc: toIsoUtc(d), ensemble_mean_ft: mean });
  }

  // de-dupe by time, keep last
  const map = new Map();
  for (const r of out) map.set(r.time_utc, r.ensemble_mean_ft);

  const rows = Array.from(map.entries())
    .map(([time_utc, ensemble_mean_ft]) => ({ time_utc, ensemble_mean_ft }))
    .sort((a, b) => (a.time_utc < b.time_utc ? -1 : 1));

  if (!rows.length) {
    return {
      header,
      rows: [],
      error:
        "Could not locate any recognizable data rows (no header match + no date-like first column rows parsed).",
    };
  }

  return { header, rows, error: null };
}

async function main() {
  const updatedUtc = new Date().toISOString();

  log("Running PETSS forecast updater via NOMADS…");
  log("STID:", STID);
  log("DATUM (for metadata only):", DATUM);
  log("Base:", BASE);

  await ensureDir(OUT_DIR);

  const meta = {
    updated_utc: updatedUtc,
    stid: STID,
    datum: DATUM,
    source: "nomads_petss_station_csv",
    status: "ok",
    message: "",
    prod_dir: "",
    cycle: "",
    tarball_url: "",
    station_file: "",
    rows: 0,
  };

  try {
    const prodDir = await pickLatestProdDir();
    meta.prod_dir = prodDir;
    log("Latest PETSS prod dir:", prodDir);

    const prodDateYYYYMMDD = prodDir.match(/petss\.(\d{8})\//)?.[1] || "";

    const { cycle, url } = await pickCycleTarball(prodDir);
    meta.cycle = cycle;
    meta.tarball_url = url;
    log("Chosen cycle tarball:", path.basename(url));
    log("Downloading:", url);

    const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), "petss-"));
    const tarPath = path.join(tmpRoot, path.basename(url));

    await fetchToFile(url, tarPath);

    const extractDir = path.join(tmpRoot, "extract");
    await ensureDir(extractDir);

    // Use system tar (available on ubuntu-latest)
    execFileSync("tar", ["-xzf", tarPath, "-C", extractDir], { stdio: "inherit" });

    const stationName = `${STID}.csv`;
    const stationPath = await findFileRecursive(extractDir, stationName);

    if (!stationPath) {
      throw new Error(`Could not find station CSV ${stationName} inside extracted tarball.`);
    }

    meta.station_file = stationPath;
    log("Station CSV file:", stationPath);

    const stationText = await fsp.readFile(stationPath, "utf8");
    const parsed = parseStationCsv(stationText, prodDateYYYYMMDD);

    if (parsed.error) {
      meta.status = "error";
      meta.message = parsed.error;
      meta.rows = 0;

      // Still write meta + preserve pipeline (don’t fail Actions hard)
      await fsp.writeFile(OUT_META, JSON.stringify(meta, null, 2), "utf8");

      // Also write an empty but valid forecast artifacts
      await fsp.writeFile(OUT_CSV, "time_utc,ensemble_mean_ft\n", "utf8");
      await fsp.writeFile(
        OUT_JSON,
        JSON.stringify(
          {
            updated_utc: updatedUtc,
            stid: STID,
            datum: DATUM,
            prod_dir: meta.prod_dir,
            cycle: meta.cycle,
            tarball_url: meta.tarball_url,
            status: meta.status,
            message: meta.message,
            rows: [],
          },
          null,
          2
        ),
        "utf8"
      );

      log("PETSS parse warning:", parsed.error);
      log("Wrote empty outputs + meta (status=error). Exiting 0 so workflow doesn’t hard-fail.");
      return;
    }

    const rows = parsed.rows;
    meta.rows = rows.length;

    // Write CSV
    const csvLines = ["time_utc,ensemble_mean_ft", ...rows.map((r) => `${r.time_utc},${r.ensemble_mean_ft}`)];
    await fsp.writeFile(OUT_CSV, csvLines.join("\n") + "\n", "utf8");

    // Write JSON
    await fsp.writeFile(
      OUT_JSON,
      JSON.stringify(
        {
          updated_utc: updatedUtc,
          stid: STID,
          datum: DATUM,
          prod_dir: meta.prod_dir,
          cycle: meta.cycle,
          tarball_url: meta.tarball_url,
          status: "ok",
          message: "",
          rows,
        },
        null,
        2
      ),
      "utf8"
    );

    await fsp.writeFile(OUT_META, JSON.stringify(meta, null, 2), "utf8");

    log(`Success: wrote ${rows.length} rows to ${OUT_CSV} and ${OUT_JSON}`);
  } catch (err) {
    meta.status = "error";
    meta.message = String(err?.message || err);
    meta.rows = 0;

    await ensureDir(OUT_DIR);
    await fsp.writeFile(OUT_META, JSON.stringify(meta, null, 2), "utf8");
    await fsp.writeFile(OUT_CSV, "time_utc,ensemble_mean_ft\n", "utf8");
    await fsp.writeFile(
      OUT_JSON,
      JSON.stringify(
        {
          updated_utc: updatedUtc,
          stid: STID,
          datum: DATUM,
          prod_dir: meta.prod_dir,
          cycle: meta.cycle,
          tarball_url: meta.tarball_url,
          status: meta.status,
          message: meta.message,
          rows: [],
        },
        null,
        2
      ),
      "utf8"
    );

    // IMPORTANT: exit 0 here too, so the site stays alive even if PETSS glitches
    console.error("PETSS update failed:", meta.message);
    console.error("Wrote empty outputs + meta (status=error). Exiting 0.");
    return;
  }
}

main();
