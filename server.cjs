/* server.cjs
   IPEDS Phase 3 Comps API
   - CIP required
   - AWLEVEL optional
   - Returns completions grouped by award level
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

function streamCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    Papa.parse(stream, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      step: (r) => onRow(r.data),
      complete: resolve,
      error: reject,
    });
  });
}

/* -----------------------------
   Load Institutions (HD)
------------------------------*/

let institutions = new Map();

(function loadInstitutions() {
  console.log("📦 Loading HD_2023.csv…");
  const csv = fs.readFileSync(HD_FILE, "utf8");
  const parsed = Papa.parse(csv, { header: true, skipEmptyLines: true });

  for (const r of parsed.data) {
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
    });
  }

  console.log(`✅ Institutions loaded: ${institutions.size}`);
})();

/* -----------------------------
   Routes
------------------------------*/

app.get("/", (_, res) => {
  res.send("IPEDS Phase 3 Comps API running. Try /health or /api/comps?cip=51.2001");
});

app.get("/health", (_, res) => {
  res.json({
    ok: true,
    years: YEARS,
    institutionsLoaded: institutions.size,
  });
});

/**
 * GET /api/comps?cip=51.2001
 * GET /api/comps?cip=51.2001&awlevel=7   (optional filter)
 */
app.get("/api/comps", async (req, res) => {
  try {
    const cip = normalizeCip(req.query.cip);
    const awlevel = toInt(req.query.awlevel);
    const filterByAw = Number.isFinite(awlevel);

    if (!cip) {
      return res.status(400).json({ error: "Missing required query param: cip" });
    }

    const acc = new Map();

    for (const year of YEARS) {
      const filePath = COMPLETIONS_BY_YEAR[year];
      console.log(`📦 Streaming ${year} (cip=${cip})`);

      await streamCsv(filePath, (row) => {
        const rowCip = normalizeCip(row.CIPCODE || row.CIPCODE6);
        if (rowCip !== cip) return;

        const rowAw = toInt(row.AWLEVEL);
        if (filterByAw && rowAw !== awlevel) return;

        const unitid = String(row.UNITID || "").trim();
        if (!unitid) return;

        const count = toInt(row.CTOTALT) || 0;

        if (!acc.has(unitid)) acc.set(unitid, {});
        const instRec = acc.get(unitid);

        if (!instRec[rowAw]) {
          instRec[rowAw] = { completions: {}, total: 0 };
        }

        instRec[rowAw].completions[year] =
          (instRec[rowAw].completions[year] || 0) + count;
        instRec[rowAw].total += count;
      });
    }

    const results = [];

    for (const [unitid, awards] of acc.entries()) {
      const inst = institutions.get(unitid) || {};
      results.push({
        unitid,
        instnm: inst.instnm || "(unknown)",
        stabbr: inst.stabbr || "",
        control: inst.control ?? null,
        carnegie: inst.carnegie ?? null,
        webaddr: inst.webaddr || "",
        awards, // keyed by AWLEVEL
      });
    }

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

/* -----------------------------
   Start Server
------------------------------*/

const server = app.listen(PORT, () => {
  console.log(`🚀 Phase 3 IPEDS API running on port ${server.address().port}`);
});
