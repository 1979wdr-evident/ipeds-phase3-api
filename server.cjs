const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');

const app = express();
const PORT = process.env.PORT || 0;

app.use(cors());

// Paths
const DATA_DIR = path.join(__dirname, 'data', 'ipeds');

// In-memory (safe to hold)
const institutions = {};
let availableYears = [];

/* -----------------------------
   Utilities
------------------------------*/

function normalizeCip(val) {
  return String(val).trim().replace(/\.?0+$/, '');
}

function detectAvailableYears() {
  return fs
    .readdirSync(DATA_DIR)
    .filter(f => f.startsWith('C_') && f.endsWith('.csv'))
    .map(f => Number(f.replace('C_', '').replace('.csv', '')))
    .sort();
}

function loadInstitutions() {
  const hdPath = path.join(DATA_DIR, 'HD_2023.csv');

  console.log('📦 Loading HD_2023.csv…');

  const csv = fs.readFileSync(hdPath, 'utf8');
  const rows = Papa.parse(csv, {
    header: true,
    skipEmptyLines: true
  }).data;

  rows.forEach(row => {
    institutions[row.UNITID] = {
      unitid: row.UNITID,
      institution_name: row.INSTNM,
      state: row.STABBR,
      sector:
        row.CONTROL === '1' ? 'Public' :
        row.CONTROL === '2' ? 'Private nonprofit' :
        row.CONTROL === '3' ? 'Private for-profit' :
        'Unknown',
      website: row.WEBADDR || null
    };
  });

  console.log(`✅ Institutions loaded: ${Object.keys(institutions).length}`);
}

/* -----------------------------
   Startup
------------------------------*/

availableYears = detectAvailableYears();
loadInstitutions();

/* -----------------------------
   Health
------------------------------*/

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    availableYears,
    institutionCount: Object.keys(institutions).length
  });
});

/* -----------------------------
   /ipeds/by-cip  (ALL YEARS)
------------------------------*/

app.get('/ipeds/by-cip', async (req, res) => {
  const { cip } = req.query;

  if (!cip) {
    return res.status(400).json({ error: 'cip parameter required' });
  }

  const targetCip = normalizeCip(cip);
  console.log(`🔎 by-cip request: ${targetCip}`);

  // { unitid: { year: count } }
  const results = {};

  let pending = availableYears.length;
  let errored = false;

  availableYears.forEach(year => {
    const filePath = path.join(DATA_DIR, `C_${year}.csv`);

    if (!fs.existsSync(filePath)) {
      pending--;
      return;
    }

    const stream = fs.createReadStream(filePath);

    Papa.parse(stream, {
      header: true,
      skipEmptyLines: true,
      step: (row) => {
        const data = row.data;

        if (!data.UNITID || !data.CIPCODE) return;
        if (normalizeCip(data.CIPCODE) !== targetCip) return;

        const count = Number(data.CTOTALT);
        if (!count || count <= 0) return;

        if (!results[data.UNITID]) {
          results[data.UNITID] = {};
        }

        results[data.UNITID][year] =
          (results[data.UNITID][year] || 0) + count;
      },
      complete: () => {
        pending--;

        if (pending === 0 && !errored) {
          const output = Object.keys(results)
            .map(unitid => {
              const inst = institutions[unitid];
              if (!inst) return null;

              return {
                unitid,
                institution_name: inst.institution_name,
                state: inst.state,
                sector: inst.sector,
                website: inst.website,
                completions: results[unitid]
              };
            })
            .filter(Boolean);

          res.json({
            cip: targetCip,
            years: availableYears,
            institutionCount: output.length,
            results: output
          });
        }
      },
      error: (err) => {
        if (!errored) {
          errored = true;
          console.error('❌ CSV parse error', err);
          res.status(500).json({ error: 'CSV processing error' });
        }
      }
    });
  });
});

/* -----------------------------
   Start server
------------------------------*/

const server = app.listen(PORT, () => {
  const actualPort = server.address().port;
  console.log(`🚀 Phase 3 IPEDS API running on port ${actualPort}`);
});
