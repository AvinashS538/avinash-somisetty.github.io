# BESS Report Automation

Automated one-pager PDF/HTML report generator for Telco Battery Energy Storage System (BESS) sites. Processes smart meter CSV data and generates daily operational reports covering energy consumption, charging/discharging sessions, and S.H.O.W. analytics KPIs.

## What It Does

- **Section 1** — Energy Consumption Summary (manual Zabbix input: DC Load, EB Energy, Battery DC Discharge, Solar DC MPPT)
- **Section 2** — Operational Overview per site (availability, grid dependency, solar contribution)
- **Section 3** — S.H.O.W. Analytics KPIs (Solar Hours, Operating Hours, Working Efficiency)
- **Section 4** — Charging & Discharging Session Analysis with detailed breakdowns

## Architecture

```
Meter CSVs → bess_report_engine.py (calculations) → bess_report_pdf.py (HTML + WeasyPrint PDF)
                                                   → bess_report_generator.py (standalone HTML)
```

## Quick Start

```bash
# Install dependencies
pip install pandas numpy weasyprint

# 1. Place meter CSV files in a folder
# 2. Edit configuration in bess_report_engine.py:
#    - CSV_FOLDER_PATH = "/path/to/csvs"
#    - ENERGY_CONSUMPTION_MANUAL (Zabbix data)
#    - REPORT_DATE

# Generate PDF report
python bess_report_pdf.py

# OR generate standalone HTML report
python bess_report_generator.py
```

## Configuration

Before each run, update these in `bess_report_engine.py`:

| Parameter | Description |
|-----------|-------------|
| `CSV_FOLDER_PATH` | Directory containing meter CSV files |
| `REPORT_DATE` | Report date (`"YYYY-MM-DD"` or `None` for auto-detect) |
| `ENERGY_CONSUMPTION_MANUAL` | Zabbix data: DC Load, EB Energy, Battery DC Discharge, Solar DC MPPT per site |

### Meter ID Mappings

| Meter IDs | SAP Site ID |
|-----------|-------------|
| `PHzeJNYRFj8XUdw_1`, `PMhVP33cBpKta4w_2` | KOLR-9059 |
| `PPyPTGDaNionVkQ_1`, `PPyPTGDaNionVkQ_2` | BGLR-3469 |
| `PZ0P20T0OQiSjSg_1`, `PBzyNe6CGRMO2EA_2` | GOND-9145 |

## Output

- `bess_report_YYYY-MM-DD.pdf` — Print-ready one-pager (210mm × 400mm)
- `bess_report_YYYY-MM-DD.html` — Browser-viewable report

## Tech Stack

- Python 3.9+
- pandas, numpy — data processing
- WeasyPrint — HTML-to-PDF conversion
- Jinja2-style HTML templating

## Sample Output

See `sample_output.html` for an example report.
