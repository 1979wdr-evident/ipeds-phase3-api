/* server.cjs
   IPEDS Phase 3 Comps API
   - CIP required for /api/comps
   - AWLEVEL optional
   - Returns completions grouped by award level

   Startup builds an in-memory CIP index so /api/comps is O(matches),
   not a full re-scan of ~370MB CSV on every request.

   Portfolio Scan uses POST /api/institution/completions (same index, unitid-keyed).
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

const HD_FILE = fs.existsSync(path.join(DATA_DIR, "HD_2025.csv"))
  ? path.join(DATA_DIR, "HD_2025.csv")
  : path.join(DATA_DIR, "HD_2024.csv");

const COMPLETIONS_BY_YEAR = {
  2019: path.join(DATA_DIR, "C_2019.csv"),
  2020: path.join(DATA_DIR, "C_2020.csv"),
  2021: path.join(DATA_DIR, "C_2021.csv"),
  2022: path.join(DATA_DIR, "C_2022.csv"),
  2023: path.join(DATA_DIR, "C_2023.csv"),
  2024: path.join(DATA_DIR, "C_2024.csv"),
  2025: path.join(DATA_DIR, "C_2025.csv"),
};

const YEARS = Object.keys(COMPLETIONS_BY_YEAR)
  .filter((y) => fs.existsSync(COMPLETIONS_BY_YEAR[y]))
  .map(Number)
  .sort((a, b) => a - b);

/* -----------------------------
   Helpers
------------------------------*/

function normalizeCip(input) {
  if (!input) return "";
  let s = String(input).replace(/[^\d.]/g, "");
  if (!s.includes(".") && s.length >= 4) {
    s = s.slice(0, 2) + "." + s.slice(2);
  }
  const m = s.match(/^(\d{2})\.(\d{1,4})/);
  if (!m) return s;
  return `${m[1]}.${m[2].padEnd(4, "0").slice(0, 4)}`;
}

function toInt(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

/** Strip UTF-8 BOM from header keys (common when NCES CSVs are saved from Excel/Windows). */
function normalizeRowKeys(row) {
  if (!row || typeof row !== "object") return {};
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    out[String(k).replace(/^\uFEFF/, "")] = v;
  }
  return out;
}

function streamCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    Papa.parse(stream, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      step: (r) => onRow(normalizeRowKeys(r.data)),
      complete: resolve,
      error: reject,
    });
  });
}

function normalizeUnitid(input) {
  return String(input || "").trim();
}

function normalizeCipList(raw, { preserveOrder = false } = {}) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  const seen = preserveOrder ? null : new Set();
  for (const item of raw) {
    const rawCip = String(item || "").trim();
    if (!rawCip) {
      if (preserveOrder) out.push({ rawCip: "", norm: "" });
      continue;
    }
    const norm = normalizeCip(rawCip);
    if (!norm) {
      if (preserveOrder) out.push({ rawCip, norm: "" });
      continue;
    }
    if (seen && seen.has(norm)) continue;
    if (seen) seen.add(norm);
    out.push({ rawCip, norm });
  }
  return out;
}

function resolveYearsFilter(rawYears) {
  if (!Array.isArray(rawYears) || rawYears.length === 0) return YEARS;
  const allowed = new Set(YEARS.map(String));
  return rawYears
    .map((y) => parseInt(String(y), 10))
    .filter((n) => Number.isFinite(n) && allowed.has(String(n)))
    .sort((a, b) => a - b);
}

/* -----------------------------
   Load Institutions (HD)
------------------------------*/

let institutions = new Map();

(function loadInstitutions() {
  console.log(`Loading ${path.basename(HD_FILE)}.`);
  const csv = fs.readFileSync(HD_FILE, "utf8").replace(/^\uFEFF/, "");
  const parsed = Papa.parse(csv, { header: true, skipEmptyLines: true });

  for (const raw of parsed.data) {
    const r = normalizeRowKeys(raw);
    const unitid = String(r.UNITID || "").trim();
    if (!unitid) continue;

    institutions.set(unitid, {
      unitid,
      instnm: r.INSTNM || "",
      stabbr: r.STABBR || "",
      city: r.CITY || "",
      webaddr: r.WEBADDR || "",
      control: toInt(r.CONTROL),
      carnegie: r.CARNEGIE || null,
      c21basic: toInt(r.C21BASIC),
    });
  }

  console.log(`Institutions loaded: ${institutions.size}`);
})();

/* -----------------------------
   CIP index (built once at startup)
   cip -> Map(unitid -> awards)
------------------------------*/

/** @type {Map<string, Map<string, object>>} */
let cipIndex = new Map();
let indexReady = false;
let indexError = null;
let indexBuiltAt = null;
let indexCipCount = 0;
let indexRowCount = 0;

function ensureAwardBucket(instRec, rowAw) {
  if (!instRec[rowAw]) {
    instRec[rowAw] = { completions: {}, total: 0 };
  }
  return instRec[rowAw];
}

function ingestCompletionRow(row, year) {
  const cip = normalizeCip(row.CIPCODE || row.CIPCODE6);
  if (!cip) return;

  const unitid = String(row.UNITID || "").trim();
  if (!unitid) return;

  const rowAw = toInt(row.AWLEVEL);
  if (rowAw == null) return;

  const count = toInt(row.CTOTALT) || 0;
  indexRowCount += 1;

  let byUnit = cipIndex.get(cip);
  if (!byUnit) {
    byUnit = new Map();
    cipIndex.set(cip, byUnit);
  }

  let instRec = byUnit.get(unitid);
  if (!instRec) {
    instRec = {};
    byUnit.set(unitid, instRec);
  }

  const bucket = ensureAwardBucket(instRec, rowAw);
  bucket.completions[year] = (bucket.completions[year] || 0) + count;
  bucket.total += count;
}

async function buildCipIndex() {
  const t0 = Date.now();
  console.log("Building CIP completions index (one-time scan).");
  cipIndex = new Map();
  indexRowCount = 0;

  for (const year of YEARS) {
    const filePath = COMPLETIONS_BY_YEAR[year];
    if (!fs.existsSync(filePath)) {
      console.warn(`Missing completions file for ${year}: ${filePath}`);
      continue;
    }
    console.log(`Indexing ${year}.`);
    await streamCsv(filePath, (row) => ingestCompletionRow(row, year));
  }

  indexCipCount = cipIndex.size;
  indexBuiltAt = new Date().toISOString();
  indexReady = true;
  console.log(
    `CIP index ready: ${indexCipCount} CIPs, ${indexRowCount} rows, ${Date.now() - t0}ms`
  );
}

/**
 * Deep-clone awards so callers cannot mutate the shared index.
 * @param {object} awards
 */
function cloneAwards(awards) {
  const out = {};
  for (const [aw, block] of Object.entries(awards || {})) {
    out[aw] = {
      total: block.total || 0,
      completions: { ...(block.completions || {}) },
    };
  }
  return out;
}

function filterAwardsYears(awards, yearsFilter) {
  const allowed = new Set(yearsFilter.map(String));
  const out = {};
  for (const [aw, block] of Object.entries(awards || {})) {
    const completions = {};
    let total = 0;
    for (const [yr, val] of Object.entries(block.completions || {})) {
      if (!allowed.has(String(yr))) continue;
      completions[yr] = val;
      total += val;
    }
    if (Object.keys(completions).length) {
      out[aw] = { completions, total };
    }
  }
  return out;
}

function institutionPayload(unitid, awards, yearsFilter) {
  const inst = institutions.get(unitid) || {};
  const filtered = filterAwardsYears(awards, yearsFilter);
  if (!Object.keys(filtered).length) return null;
  return {
    unitid,
    instnm: inst.instnm || "(unknown)",
    stabbr: inst.stabbr || "",
    control: inst.control ?? null,
    carnegie: inst.carnegie ?? null,
    c21basic: inst.c21basic ?? null,
    webaddr: inst.webaddr || "",
    awards: filtered,
  };
}

function buildResultsForCip(cip, awlevelFilter) {
  const byUnit = cipIndex.get(cip);
  if (!byUnit || byUnit.size === 0) return [];

  const filterByAw = Number.isFinite(awlevelFilter);
  const results = [];

  for (const [unitid, awards] of byUnit.entries()) {
    let awardsOut = awards;
    if (filterByAw) {
      const key = String(awlevelFilter);
      const block = awards[awlevelFilter] || awards[key];
      if (!block) continue;
      awardsOut = { [awlevelFilter]: block };
    }

    const inst = institutions.get(unitid) || {};
    results.push({
      unitid,
      instnm: inst.instnm || "(unknown)",
      stabbr: inst.stabbr || "",
      control: inst.control ?? null,
      carnegie: inst.carnegie ?? null,
      c21basic: inst.c21basic ?? null,
      webaddr: inst.webaddr || "",
      awards: cloneAwards(awardsOut),
    });
  }

  return results;
}

/**
 * Look up one institution's completions for a list of CIPs (Portfolio Scan matrix).
 * @param {string} unitid
 * @param {Array<{ rawCip: string, norm: string }>} cipEntries
 * @param {number[]} yearsFilter
 */
function buildInstitutionCompletionsItems(unitid, cipEntries, yearsFilter) {
  const items = [];
  for (const { rawCip, norm } of cipEntries) {
    if (!norm) {
      items.push({
        cip: rawCip,
        found: false,
        years: yearsFilter,
        institution: null,
      });
      continue;
    }
    const byUnit = cipIndex.get(norm);
    const awards = byUnit?.get(unitid);
    const institution = awards ? institutionPayload(unitid, awards, yearsFilter) : null;
    items.push({
      cip: rawCip,
      found: !!institution,
      years: yearsFilter,
      institution,
    });
  }
  return items;
}

function requireIndexReady(res) {
  if (indexReady) return true;
  res.status(503).json({
    error: "Index still building",
    detail: "Retry in a few seconds",
    indexError,
  });
  return false;
}

/* -----------------------------
   Routes
------------------------------*/

app.get("/", (_, res) => {
  res.send(
    "IPEDS Phase 3 Comps API running. Try /health, /api/comps?cip=51.2001, or POST /api/institution/completions"
  );
});

app.get("/health", (_, res) => {
  res.json({
    ok: true,
    ready: indexReady,
    years: YEARS,
    institutionsLoaded: institutions.size,
    cipCount: indexCipCount,
    indexedRows: indexRowCount,
    indexBuiltAt,
    indexError,
    endpoints: ["/api/comps", "/api/institution/completions", "/api/comps/batch"],
  });
});

/**
 * GET /api/comps?cip=51.2001
 * GET /api/comps?cip=51.2001&awlevel=7   (optional filter)
 */
app.get("/api/comps", (req, res) => {
  try {
    if (!requireIndexReady(res)) return;

    const cip = normalizeCip(req.query.cip);
    const awlevel = toInt(req.query.awlevel);

    if (!cip) {
      return res.status(400).json({ error: "Missing required query param: cip" });
    }

    const results = buildResultsForCip(cip, awlevel);

    res.json({
      cip,
      years: YEARS,
      results,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error", detail: err.message });
  }
});

/**
 * POST /api/institution/completions
 * Body: { unitid: "198950", cips: ["51.2001", "52.0201"], years?: [2021,2022,2023,2024,2025] }
 *
 * Returns one item per requested CIP with that institution's award-level completions.
 * Does not change /api/comps — safe for live Lab comps tabs.
 */
app.post("/api/institution/completions", (req, res) => {
  try {
    if (!requireIndexReady(res)) return;

    const unitid = normalizeUnitid(req.body?.unitid);
    const cipEntries = normalizeCipList(req.body?.cips);
    const yearsFilter = resolveYearsFilter(req.body?.years);

    if (!unitid) {
      return res.status(400).json({ error: "Missing required body field: unitid" });
    }
    if (!cipEntries.length) {
      return res.status(400).json({ error: "Missing required body field: cips (non-empty array)" });
    }

    const items = buildInstitutionCompletionsItems(unitid, cipEntries, yearsFilter);
    const inst = institutions.get(unitid) || null;

    res.json({
      unitid,
      years: yearsFilter,
      institution: inst
        ? {
            unitid,
            instnm: inst.instnm || "",
            stabbr: inst.stabbr || "",
            control: inst.control ?? null,
            carnegie: inst.carnegie ?? null,
            c21basic: inst.c21basic ?? null,
            webaddr: inst.webaddr || "",
          }
        : null,
      items,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error", detail: err.message });
  }
});

/**
 * POST /api/comps/batch
 * Legacy alias used by Portfolio Scan shared loader.
 * Body: { unitid, cips, years? }
 * Response: { items: [{ years, institution }, ...] } aligned to input cips order.
 */
app.post("/api/comps/batch", (req, res) => {
  try {
    if (!requireIndexReady(res)) return;

    const unitid = normalizeUnitid(req.body?.unitid);
    const rawCips = Array.isArray(req.body?.cips) ? req.body.cips : [];
    const cipEntries = normalizeCipList(rawCips, { preserveOrder: true });
    const yearsFilter = resolveYearsFilter(req.body?.years);

    if (!unitid) {
      return res.status(400).json({ error: "Missing required body field: unitid" });
    }
    if (!cipEntries.length) {
      return res.status(400).json({ error: "Missing required body field: cips (non-empty array)" });
    }

    const built = buildInstitutionCompletionsItems(unitid, cipEntries, yearsFilter);
    const items = built.map((entry) => ({
      years: yearsFilter,
      institution: entry.institution,
    }));

    res.json({ unitid, years: yearsFilter, items });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error", detail: err.message });
  }
});

/* -----------------------------
   Start Server
------------------------------*/

const server = app.listen(PORT, () => {
  console.log(`Phase 3 IPEDS API listening on port ${server.address().port}`);
});

buildCipIndex().catch((err) => {
  indexError = err?.message || String(err);
  console.error("Failed to build CIP index:", err);
});
