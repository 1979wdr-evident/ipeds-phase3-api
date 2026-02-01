/* server.cjs
   IPEDS Phase 3 Comps API
   Railway-safe, lazy-load completions, Carnegie included
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
const DATA_DIR = path.join(__dirname, "data", "ipeds");

const HD_FILE = path.join(DATA_DIR, "HD_2023.csv");

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

const CACHE_MAX = 50;
const cache = new Map();

/* -----------------------------
   Helpers
------------------------------*/

function normalizeCip(input) {
  if (!input) return "";
  let s = String(input).trim().replace(/[^\d.]/g, "");
  if (!s.includes(".") && s.length >= 4) {
    s = s.slice(0, 2) + "." + s.slice(2);
  }
  const m = s.match(/^(\d{2})\.(\d{1,4})/);
  if (!m) return s;
  return `${m[1]}.${m[2].padEnd(4, "0").slice(0, 4)}`;
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

function streamCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    existsOrThrow(filePath);

    const readStream = fs.createReadStream(filePath);

    Papa.parse(readStream, {
      header: true,
      skipEmptyLines: true,
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
   Load Institutions (HD)
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
    const unitid = String(safeGet(row, "UNITID") ?? "").trim();
    if (!unitid) continue;

    map.set(unitid, {
      unitid,
      instnm: String(safeGet(row, "INSTNM") ?? "").trim(),
      stabbr: String(safeGet(row, "STABBR") ?? "").trim(),
      city: String(safeGet(row, "CITY") ?? "").trim(),
      zip: String(safeGet(row, "ZIP") ?? "").trim(),
      webaddr: String(safeGet(row, "WEBADDR") ?? "").trim(),
      control: toInt(safeGet(row, "CONTROL")),
      carnegie: toInt(safeGet(row, "CARNEGIE")), // ✅ retained + exposed
    });
  }

  instByUnitid = map;
  console.log(`✅ Institutions loaded: ${instByUnitid.size}`);
}

/* -----------------------------
   Routes
------------------------------*/

app.get("/", (req, res) => {
  res.send(
    "IPEDS Phase 3 Comps API is running. Try /health or /api/comps?cip=51.2001&awlevel=7"
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    institutionsLoaded: instByUnitid.size,
    years: YEARS,
  });
});

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

    const acc = new Map();

    for (const year of YEARS) {
      const filePath = COMPLETIONS_BY_YEAR[year];
      console.log(`📦 Streaming ${year} for CIP ${cip}, AWLEVEL ${awlevel}`);

      await streamCsv(filePath, (row) => {
        const unitid = String(safeGet(row, "UNITID") ?? "").trim();
        if (!unitid) return;

        const rowCip = normalizeCip(safeGet(row, "CIPCODE"));
        if (rowCip !== cip) return;

        const rowAw = toInt(safeGet(row, "AWLEVEL"));
        if (rowAw !== awlevel) return;

        const count = toInt(safeGet(row, "CTOTALT")) ?? 0;

        if (!acc.has(unitid)) {
          acc.set(unitid, { completions: {}, total: 0 });
        }

        acc.get(unitid).completions[year] =
          (acc.get(unitid).completions[year] ?? 0) + count;
        acc.get(unitid).total += count;
      });
    }

    const results = [];

    for (const [unitid, rec] of acc.entries()) {
      const inst = instByUnitid.get(unitid);

      results.push({
        unitid,
        instnm: inst?.instnm || "(unknown)",
        stabbr: inst?.stabbr || "",
        control: inst?.control ?? null,
        carnegie: inst?.carnegie ?? null, // ✅ FIXED
        webaddr: inst?.webaddr || "",
        completions: rec.completions,
        total: rec.total,
      });
    }

    results.sort((a, b) => (b.total || 0) - (a.total || 0));

    const payload = {
      cip,
      awlevel,
      years: YEARS,
      results,
    };

    cache.set(cacheKey, payload);
    if (cache.size > CACHE_MAX) {
      cache.delete(cache.keys().next().value);
    }

    res.json(payload);
  } catch (err) {
    console.error("Comps error:", err);
    res.status(500).json({ error: "Server error", detail: err.message });
  }
});

/* -----------------------------
   Boot
------------------------------*/

try {
  loadInstitutions();
} catch (err) {
  console.error("❌ Failed to load institutions:", err);
  process.exit(1);
}

const server = app.listen(PORT, () => {
  console.log(`🚀 Phase 3 IPEDS API running on port ${server.address().port}`);
});
