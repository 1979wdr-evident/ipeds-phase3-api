/* =========================================================
   Phase 3 IPEDS Comps API
   server.cjs
   ========================================================= */

const express = require("express");
const fs = require("fs");
const path = require("path");
const Papa = require("papaparse");
const cors = require("cors");

const app = express();
app.use(cors());

/* -----------------------------
   Config
------------------------------ */

const PORT = process.env.PORT || 3000;

const DATA_DIR = path.join(__dirname, "data", "ipeds");

const COMPLETION_FILES = {
  2019: "C_2019.csv",
  2020: "C_2020.csv",
  2021: "C_2021.csv",
  2022: "C_2022.csv",
  2023: "C_2023.csv",
};

const HD_FILE = "HD_2023.csv";

/* -----------------------------
   In-memory stores
------------------------------ */

const institutions = new Map(); // UNITID → metadata
const completionsByYear = {};   // year → array of rows

/* -----------------------------
   Helpers
------------------------------ */

function parseCSV(filePath) {
  const csv = fs.readFileSync(filePath, "utf8");
  const parsed = Papa.parse(csv, {
    header: true,
    skipEmptyLines: true,
  });
  return parsed.data;
}

function sectorLabel(sectorCode) {
  if (!sectorCode) return "Unknown";
  if (sectorCode.startsWith("1")) return "Public";
  if (sectorCode.startsWith("2")) return "Private nonprofit";
  if (sectorCode.startsWith("3")) return "Private for-profit";
  return "Other";
}

/* -----------------------------
   Load Institutions (HD)
------------------------------ */

console.log("📦 Loading HD_2023.csv…");

const hdRows = parseCSV(path.join(DATA_DIR, HD_FILE));

hdRows.forEach((row) => {
  institutions.set(row.UNITID, {
    unitid: row.UNITID,
    institution_name: row.INSTNM,
    state: row.STABBR,
    sector: sectorLabel(row.SECTOR),
    website: row.WEBADDR || null,
    carnegie: row.C21BASIC || null,
  });
});

console.log(`✅ Institutions loaded: ${institutions.size}`);

/* -----------------------------
   Load Completions (by year)
------------------------------ */

Object.entries(COMPLETION_FILES).forEach(([year, filename]) => {
  console.log(`📦 Loading completions ${year}…`);
  completionsByYear[year] = parseCSV(path.join(DATA_DIR, filename));
});

/* -----------------------------
   Routes
------------------------------ */

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    service: "ipeds-phase3-api",
    availableYears: Object.keys(COMPLETION_FILES).map(Number),
    institutionCount: institutions.size,
  });
});

app.get("/comps", (req, res) => {
  const { cip, awlevel } = req.query;

  if (!cip) {
    return res.status(400).json({ error: "Missing required parameter: cip" });
  }

  const results = new Map();

  Object.entries(completionsByYear).forEach(([year, rows]) => {
    rows.forEach((row) => {
      if (row.CIPCODE !== cip) return;
      if (awlevel && row.AWLEVEL !== awlevel) return;

      const unitid = row.UNITID;
      const count = Number(row.CTOTALT || 0);

      if (!results.has(unitid)) {
        const inst = institutions.get(unitid);
        if (!inst) return;

        results.set(unitid, {
          ...inst,
          completions: {},
        });
      }

      if (count > 0) {
        results.get(unitid).completions[year] = count;
      }
    });
  });

  res.json({
    cip,
    awlevel: awlevel || null,
    years: Object.keys(COMPLETION_FILES).map(Number),
    institutionCount: results.size,
    results: Array.from(results.values()),
  });
});

/* -----------------------------
   Start server
------------------------------ */

const server = app.listen(PORT, () => {
  const actualPort = server.address().port;
  console.log(`🚀 Phase 3 IPEDS API running on port ${actualPort}`);
});
