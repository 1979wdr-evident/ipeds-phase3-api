/* server.cjs
   IPEDS Phase 3 Comps API (Railway-safe, lazy-load completions)
*/

const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const Papa = require("papaparse");

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

/* -----------------------------
   Config
------------------------------*/

const PORT = process.env.PORT || 3000;

// Data paths (adjust if your repo uses a different structure)
const DATA_DIR = path.join(__dirname, "data", "ipeds");

// Institution file (HD)
const HD_FILE = path.join(DATA_DIR, "HD_2023.csv");

// Completions files (C)
const COMPLETIONS_BY_YEAR = {
  2019: path.join(DATA_DIR, "C_2019.csv"),
  2020: path.join(DATA_DIR, "C_2020.csv"),
  2021: path.join(DATA_DIR, "C_2021.csv"),
  2022: path.join(DATA_DIR, "C_2022.csv"),
  2023: path.join(DATA_DIR, "C_2023.csv"),
};

const YEARS = Object.keys(COMPLETIONS_BY_YEAR)
  .map((y) => parseInt(y, 10))
  .sort((a, b) => a - b);

// Simple in-memory cache (per container)
const CACHE_MAX = 50; // keep small
const cache = new Map();

/* -----------------------------
   Helpers
------------------------------*/

function normalizeCip(input) {
  if (!input) return "";
  // keep digits + dot only
  let s = String(input).trim().replace(/[^\d.]/g, "");
  // if someone passes 512001 => 51.2001 (best-effort)
  if (!s.includes(".") && s.length >= 4) {
    // heuristic: first 2 digits, then the rest
    s = s.slice(0, 2) + "." + s.slice(2);
  }
  // trim to 2 + 4 pattern when possible
  const m = s.match(/^(\d{2})\.(\d{1,4})/);
  if (!m) return s;
  const left = m[1];
  const right = m[2].padEnd(4, "0").slice(0, 4);
  return `${left}.${right}`;
}

function toInt(v) {
  const n = parseInt(String(v ?? "").trim(), 10);
  return Number.isFinite(n) ? n : null;
}

function existsOrThrow(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Missing file: ${filePath}`);
  }
}

function safeGet(obj, ...keys) {
  for (const k of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k)) return obj[k];
  }
  return undefined;
}

/**
 * Parse a CSV file as a stream and call `onRow(row)` for each row.
 * Uses PapaParse's Node stream mode.
 */
function streamCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    existsOrThrow(filePath);

    const readStream = fs.createReadStream(filePath);

    Papa.parse(readStream, {
      header: true,
      skipEmptyLines: true,
      // dynamicTyping can increase memory; keep false
      dynamicTyping: false,
      step: (results) => {
        try {
          onRow(results.data);
        } catch (err) {
          reject(err);
        }
      },
      complete: () => resolve(),
      error: (err) => reject(err),
    });
  });
}

/* -----------------------------
   Load HD once (small)
------------------------------*/

let instByUnitid = new Map();

function loadInstitutions() {
  console.log("📦 Loading HD_2023.csv…");
  existsOrThrow(HD_FILE);

  const csvText = fs.readFileSync(HD_FILE, "utf8");
  const parsed = Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true,
  });

  const map = new Map();
  for (const row of parsed.data) {
    const unitid = String(safeGet(row, "UNITID", "unitid") ?? "").trim();
    if (!unitid) continue;

    // IPEDS HD fields vary; keep what we can
    const instnm = String(safeGet(row, "INSTNM", "instnm") ?? "").trim();
    const stabbr = String(safeGet(row, "STABBR", "stabbr") ?? "").trim();
    const city = String(safeGet(row, "CITY", "city") ?? "").trim();
    const zip = String(safeGet(row, "ZIP", "zip") ?? "").trim();
    const webaddr = String(safeGet(row, "WEBADDR", "webaddr") ?? "").trim();

    // Sector (CONTROL often: 1 public, 2 private nonprofit, 3 private for-profit)
    const control = toInt(safeGet(row, "CONTROL", "control"));
    // Carnegie (varies depending on file version; may be missing)
    const carnegie = safeGet(row, "CARNEGIE", "carnegie");

    map.set(unitid, {
      unitid,
      instnm,
      stabbr,
      city,
      zip,
      webaddr,
      control,
      carnegie,
    });
  }

  instByUnitid = map;
  console.log(`✅ Institutions loaded: ${instByUnitid.size}`);
}

/* -----------------------------
   Routes
------------------------------*/

// Root route so Railway shows something besides "Cannot GET /"
app.get("/", (req, res) => {
  res.status(200).send(
    "IPEDS Phase 3 Comps API is running. Try /health or /api/comps?cip=51.2001&awlevel=7"
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "ipeds-phase3-api",
    institutionsLoaded: instByUnitid.size,
    years: YEARS,
  });
});

/**
 * GET /api/comps?cip=51.2001&awlevel=7
 *
 * Returns:
 * {
 *   cip: "51.2001",
 *   awlevel: 7,
 *   years: [2019..2023],
 *   results: [
 *     {
 *       unitid, instnm, stabbr, control, webaddr,
 *       completions: { "2019": 12, "2020": 10, ... },
 *       total: 55
 *     }, ...
 *   ]
 * }
 */
app.get("/api/comps", async (req, res) => {
  try {
    const cip = normalizeCip(req.query.cip);
    const awlevel = toInt(req.query.awlevel);

    if (!cip) {
      return res.status(400).json({ error: "Missing required query param: cip" });
    }
    if (!awlevel) {
      return res
        .status(400)
        .json({ error: "Missing or invalid query param: awlevel" });
    }

    const cacheKey = `${cip}|${awlevel}`;
    if (cache.has(cacheKey)) {
      return res.json(cache.get(cacheKey));
    }

    // Accumulator: unitid -> { completionsByYear, total }
    const acc = new Map();

    // Stream each year file, filter only matching CIP+AWLEVEL
    for (const year of YEARS) {
      const filePath = COMPLETIONS_BY_YEAR[year];
      console.log(`📦 Streaming completions ${year} for cip=${cip} awlevel=${awlevel}…`);

      await streamCsv(filePath, (row) => {
        // IPEDS C file fields typically: UNITID, CIPCODE, AWLEVEL, CTOTALT (or similar)
        // Some releases may use CIPCODE/CIPCODE6 or CIPCODE2/CIP
        const unitidRaw = safeGet(row, "UNITID", "unitid");
        const unitid = String(unitidRaw ?? "").trim();
        if (!unitid) return;

        const cipRaw = safeGet(row, "CIPCODE", "cipcode", "CIPCODE6", "cip");
        const rowCip = normalizeCip(cipRaw);
        if (rowCip !== cip) return;

        const awRaw = safeGet(row, "AWLEVEL", "awlevel");
        const rowAw = toInt(awRaw);
        if (rowAw !== awlevel) return;

        // completions count field varies; common: CTOTALT
        const cRaw =
          safeGet(row, "CTOTALT", "ctotalt", "TOTAL", "total", "COMPLETIONS") ?? "0";
        const count = toInt(cRaw) ?? 0;

        if (!acc.has(unitid)) {
          acc.set(unitid, { completions: {}, total: 0 });
        }
        const rec = acc.get(unitid);
        rec.completions[String(year)] = (rec.completions[String(year)] ?? 0) + count;
        rec.total += count;
      });
    }

    // Join to institutions
    const results = [];
    for (const [unitid, rec] of acc.entries()) {
      const inst = instByUnitid.get(unitid);
      // Keep row even if inst missing (rare), but label as unknown
      results.push({
        unitid,
        instnm: inst?.instnm || "(unknown)",
        stabbr: inst?.stabbr || "",
        control: inst?.control ?? null,
        webaddr: inst?.webaddr || "",
        completions: rec.completions,
        total: rec.total,
      });
    }

    // Sort: highest total first
    results.sort((a, b) => (b.total || 0) - (a.total || 0));

    const payload = {
      cip,
      awlevel,
      years: YEARS,
      results,
    };

    // cache with small cap
    cache.set(cacheKey, payload);
    if (cache.size > CACHE_MAX) {
      const firstKey = cache.keys().next().value;
      cache.delete(firstKey);
    }

    return res.json(payload);
  } catch (err) {
    console.error("Comps error:", err);
    return res.status(500).json({ error: "Server error", detail: String(err.message || err) });
  }
});

/* -----------------------------
   Boot
------------------------------*/

try {
  loadInstitutions();
} catch (e) {
  console.error("❌ Failed to load institutions:", e);
  // Let process crash so Railway shows obvious failure instead of half-running
  process.exit(1);
}

const server = app.listen(PORT, () => {
  const actualPort = server.address().port;
  console.log(`🚀 Phase 3 IPEDS API running on port ${actualPort}`);
});
