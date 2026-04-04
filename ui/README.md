# UI Dashboard

This folder contains a lightweight HTML dashboard for the Data Contract Enforcer project.

## Generate the dashboard

```bash
python3 ui/build_dashboard.py
```

## View the dashboard

Serve the repo root with a static server and open the file in your browser:

```bash
cd /home/bethel/Documents/10academy/data-contract-enforcer
python3 -m http.server 8000
```

Then open:

```text
http://localhost:8000/ui/dashboard.html
```

## Notes

- The dashboard loads data from `enforcer_report/report_data.json` and `validation_reports/week3_violated.json`.
- It uses Chart.js from a CDN for the validation summary chart.
