import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

report_path = ROOT / "enforcer_report" / "report_data.json"
violated_path = ROOT / "validation_reports" / "week3_violated.json"
output_path = HERE / "dashboard.html"

report_data = json.loads(report_path.read_text(encoding="utf-8"))
violated_data = json.loads(violated_path.read_text(encoding="utf-8"))

html = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Data Contract Enforcer Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0d1321;
      --panel: rgba(9, 18, 34, 0.96);
      --panel-strong: rgba(18, 36, 67, 0.98);
      --text: #f7f8fc;
      --muted: #8fa3c3;
      --accent: #62d2ff;
      --success: #47d18f;
      --warn: #f7b955;
      --danger: #ff6f75;
      --border: rgba(255,255,255,0.07);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: 'Inter', sans-serif;
      background: radial-gradient(circle at top left, rgba(98, 210, 255, 0.18), transparent 25%),
                  radial-gradient(circle at bottom right, rgba(144, 95, 255, 0.18), transparent 20%),
                  linear-gradient(180deg, #09101c 0%, #0c1421 100%);
      color: var(--text);
    }
    .page {
      width: min(1400px, calc(100% - 32px));
      margin: 0 auto;
      padding: 32px 0 64px;
    }
    header {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
      margin-bottom: 32px;
    }
    .hero {
      max-width: 720px;
    }
    h1 {
      margin: 0 0 16px;
      font-size: clamp(2.4rem, 4vw, 4rem);
      line-height: 1.02;
    }
    p.lead {
      margin: 0;
      color: var(--muted);
      font-size: 1.05rem;
      line-height: 1.75;
    }
    .badge {
      display: inline-flex;
      padding: 0.7rem 1rem;
      border-radius: 999px;
      font-weight: 700;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      font-size: 0.78rem;
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.08);
    }
    .badge.green { color: #8ef2c8; border-color: rgba(71, 209, 143,0.3); }
    .badge.red { color: #ffb7bd; border-color: rgba(255,111,117,0.3); }
    .grid {
      display: grid;
      gap: 24px;
    }
    .grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 28px;
      padding: 26px;
      box-shadow: 0 24px 80px rgba(0,0,0,0.22);
      backdrop-filter: blur(18px);
    }
    .card strong { color: var(--text); }
    .metric {
      display: flex;
      align-items: baseline;
      gap: 16px;
    }
    .metric .value {
      font-size: clamp(2rem, 3vw, 3.4rem);
      line-height: 1;
      font-weight: 800;
    }
    .metric .label {
      color: var(--muted);
      font-size: 0.95rem;
    }
    .small-card {
      display: grid;
      gap: 12px;
    }
    .small-card .label { color: var(--muted); text-transform: uppercase; letter-spacing: 0.14em; font-size: 0.75rem; }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.08);
      font-weight: 700;
      font-size: 0.88rem;
    }
    .status-pill span { width: 10px; height: 10px; border-radius: 999px; display: inline-block; }
    .status-pill.pass span { background: var(--success); }
    .status-pill.fail span { background: var(--danger); }
    .status-pill.warn span { background: var(--warn); }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
      color: var(--text);
    }
    thead tr { border-bottom: 1px solid rgba(255,255,255,0.08); }
    th, td {
      padding: 14px 12px;
      text-align: left;
    }
    th { color: var(--muted); font-weight: 600; }
    tbody tr:hover { background: rgba(255,255,255,0.04); }
    tbody tr:nth-child(odd) { background: rgba(255,255,255,0.02); }
    .pill { display: inline-flex; padding: 0.45rem 0.8rem; border-radius: 999px; font-size: 0.82rem; font-weight: 700; letter-spacing: 0.02em; }
    .pill.PASS { background: rgba(71, 209, 143, 0.14); color: #9df8cc; }
    .pill.FAIL { background: rgba(255,111,117, 0.14); color: #ffb7bd; }
    .pill.INFO { background: rgba(98,210,255,0.14); color: #8ce5ff; }
    .pill.CRITICAL { background: rgba(255,111,117,0.14); color: #ffb7bd; }
    .pill.HIGH { background: rgba(255,111,117,0.14); color: #ffb7bd; }
    .footer-notes {
      margin-top: 40px;
      color: var(--muted);
      font-size: 0.95rem;
      display: grid;
      gap: 12px;
    }
    @media (max-width: 980px) {
      .grid-3 { grid-template-columns: 1fr; }
      header { flex-direction: column; align-items: flex-start; }
    }
  </style>
</head>
<body>
  <div class="page">
    <header>
      <div class="hero">
        <span class="badge green">Data Contract Enforcer</span>
        <h1>Validation dashboard for Week 7 extractions</h1>
        <p class="lead">An interactive summary that highlights contract health, failed checks, and AI monitoring metrics for your data pipeline.</p>
      </div>
      <div style="min-width:280px;">
        <div class="card" style="padding:24px; text-align:center;">
          <div class="metric">
            <div class="value" id="health-score">0</div>
            <div class="label">Data health score</div>
          </div>
          <div class="status-pill" id="health-badge"><span></span><span>Loading...</span></div>
        </div>
      </div>
    </header>

    <div class="grid grid-3">
      <div class="card">
        <div class="small-card">
          <span class="label">Contract run</span>
          <strong id="contract-id"></strong>
          <span id="run-timestamp" style="color: var(--muted);"></span>
        </div>
      </div>
      <div class="card">
        <div class="small-card">
          <span class="label">Overall results</span>
          <div class="metric"><span class="value" id="total-checks">0</span></div>
          <div style="display:grid; gap:8px; margin-top:12px;">
            <span>Passed: <strong id="passed-count"></strong></span>
            <span>Failed: <strong id="failed-count"></strong></span>
            <span>Violations: <strong id="violations-count"></strong></span>
          </div>
        </div>
      </div>
      <div class="card">
        <canvas id="summary-chart" height="220"></canvas>
      </div>
    </div>

    <div class="grid" style="grid-template-columns: 2fr 1fr; margin-top:24px; gap:24px;">
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;">
          <div>
            <span class="label">Failed checks overview</span>
            <h2 style="margin:8px 0 0; font-size:1.4rem;">Critical issues and drift warnings</h2>
          </div>
          <span class="pill" style="background: rgba(255,111,117,0.16); color: #ffb7bd;">{violated_data['failed']} failed</span>
        </div>
        <div style="overflow-x:auto;">
          <table>
            <thead>
              <tr>
                <th>Check</th>
                <th>Status</th>
                <th>Column</th>
                <th>Details</th>
                <th>Severity</th>
              </tr>
            </thead>
            <tbody id="failed-rows"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="small-card">
          <span class="label">AI & drift status</span>
          <div style="display:grid; gap:16px; margin-top:16px;">
            <div>
              <strong>Embedding drift</strong>
              <p style="margin:8px 0 0; color: var(--muted);" id="embed-drift"></p>
            </div>
            <div>
              <strong>LLM violation rate</strong>
              <p style="margin:8px 0 0; color: var(--muted);" id="llm-rate"></p>
            </div>
            <div>
              <strong>AI status</strong>
              <p style="margin:8px 0 0; color: var(--muted);" id="ai-status"></p>
            </div>
          </div>
        </div>
        <div class="small-card" style="margin-top:24px;">
          <span class="label">Recommended actions</span>
          <ul id="recommendations" style="margin:12px 0 0; padding-left:20px; color: var(--text);"></ul>
        </div>
      </div>
    </div>

    <div class="footer-notes">
      <div>Open this file in a modern browser, or serve it with a lightweight static server for full interactivity.</div>
      <div>Use <code>python3 -m http.server 8000</code> from the repo root and open <code>http://localhost:8000/ui/dashboard.html</code>.</div>
    </div>
  </div>

  <script>
    const enforcerData = __ENFORCER_DATA__;
    const validationData = __VALIDATION_DATA__;

    const healthScore = Math.round(enforcerData.data_health_score);
    document.getElementById('health-score').textContent = healthScore;
    const healthBadge = document.getElementById('health-badge');
    const healthText = healthScore >= 90 ? 'Excellent' : healthScore >= 70 ? 'Healthy' : 'Attention';
    healthBadge.classList.add(healthScore >= 90 ? 'green' : healthScore >= 70 ? 'warn' : 'red');
    healthBadge.querySelector('span:last-child').textContent = healthText;
    document.getElementById('contract-id').textContent = validationData.contract_id;
    document.getElementById('run-timestamp').textContent = new Date(validationData.run_timestamp).toLocaleString();
    document.getElementById('total-checks').textContent = validationData.total_checks;
    document.getElementById('passed-count').textContent = validationData.passed;
    document.getElementById('failed-count').textContent = validationData.failed;
    document.getElementById('violations-count').textContent = enforcerData.summary.violations;
    document.getElementById('embed-drift').textContent = enforcerData.ai_system_status.embedding_drift;
    document.getElementById('llm-rate').textContent = enforcerData.ai_system_status.llm_violation_rate;
    document.getElementById('ai-status').textContent = enforcerData.ai_system_status.overall_ai_status;

    const recList = document.getElementById('recommendations');
    enforcerData.recommended_actions.forEach(action => {
      const li = document.createElement('li');
      li.textContent = action;
      recList.appendChild(li);
    });

    const failures = validationData.results.filter(r => r.status !== 'PASS').slice(0, 8);
    const failedRows = document.getElementById('failed-rows');
    if (failures.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="5" style="padding:20px 12px; color: var(--muted); text-align:center;">No failed checks found — all contracts are passing.</td>';
      failedRows.appendChild(tr);
    } else {
      failures.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${item.check_id}</td>
          <td><span class="pill ${item.status}">${item.status}</span></td>
          <td>${item.column_name}</td>
          <td>${item.message || item.actual_value}</td>
          <td>${item.severity}</td>
        `;
        failedRows.appendChild(tr);
      });
    }

    const ctx = document.getElementById('summary-chart');
    new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Passed', 'Failed'],
        datasets: [{
          data: [validationData.passed, validationData.failed],
          backgroundColor: ['#47d18f', '#ff6f75'],
          borderWidth: 0,
          hoverOffset: 10,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom', labels: { color: '#c1d4ff' } },
          tooltip: { bodyColor: '#fff', backgroundColor: 'rgba(10, 18, 36, 0.96)' }
        },
        cutout: '72%'
      }
    });
  </script>
</body>
</html>
"""

html = html.replace("__ENFORCER_DATA__", json.dumps(report_data)).replace("__VALIDATION_DATA__", json.dumps(violated_data))
output_path.write_text(html, encoding="utf-8")
print(f"Dashboard written to: {output_path}")
