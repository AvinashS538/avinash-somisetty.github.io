"""
BESS Report — Complete Report Generator
=========================================
Single file: Computes all sections from CSV data + generates HTML report.

Usage:
  1. Update CONFIGURATION section below (CSV path, Zabbix data, report date)
  2. Run:  python bess_report_generator.py
  3. Output: bess_report_YYYY-MM-DD.html (open in browser, print/save as PDF)
"""

import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                    CONFIGURATION — EDIT BEFORE EACH RUN                ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# Path to folder containing meter CSV files
CSV_FOLDER_PATH = r"./csv_data"  # Place meter CSV files in this folder

# Report date — set to specific date string or None for auto-detect from CSV
REPORT_DATE = None  # e.g. "2026-02-28" or None

# Output folder for the HTML file
OUTPUT_FOLDER = r"/mnt/user-data/outputs"

# Meter to SAP ID Mapping
METER_TO_SAP_MAPPING = {
    'PHzeJNYRFj8XUdw_1': 'I-KA-KOLR-ENB-9059',
    'PMhVP33cBpKta4w_2': 'I-KA-KOLR-ENB-9059',
    'PPyPTGDaNionVkQ_1': 'I-KA-BGLR-ENB-3469',
    'PPyPTGDaNionVkQ_2': 'I-KA-BGLR-ENB-3469',
    'PZ0P20T0OQiSjSg_1': 'I-UP-GOND-ENB-9145',
    'PBzyNe6CGRMO2EA_2': 'I-UP-GOND-ENB-9145',
}

# Fixed display order across all sections
SAP_ID_ORDER = [
    'I-KA-KOLR-ENB-9059',
    'I-KA-BGLR-ENB-3469',
    'I-UP-GOND-ENB-9145',
]

# Battery rated capacity (Ah) — used for C-Rate
RATED_CAPACITY_AH = 314

# Site configuration
SAP_TO_SITE_CONFIG = {
    'I-KA-KOLR-ENB-9059': {
        'site_name': 'Site 1',
        'eb': 'EB (15 KVA) & Solar (7.5 KW)',
        'bess_desc': '314Ah based BESS for Backup at DC Bus (30 kWh)',
        'dg': 'DG (Make:M&M, 10KVA) as a last-resort power source',
        'deploy': '2 New 314Ah 48V BESS (in Parallel)',
        'commissioned': 'Jan 9, 2026',
    },
    'I-KA-BGLR-ENB-3469': {
        'site_name': 'Site 2',
        'eb': 'EB (15 KVA)',
        'bess_desc': '314Ah based BESS for Backup at DC Bus (30 kWh)',
        'dg': None,
        'deploy': '2 New 314Ah 48V BESS (in Parallel)',
        'commissioned': 'Feb 05, 2026',
    },
    'I-UP-GOND-ENB-9145': {
        'site_name': 'Site 3',
        'eb': 'EB (15 KVA)',
        'bess_desc': '314Ah based BESS for Backup at DC Bus (30 kWh)',
        'dg': None,
        'deploy': '2 New 314Ah 48V BESS (in Parallel)',
        'commissioned': 'Feb 19, 2026',
    },
}

# ──────────────────────────────────────────────────────────────
# SECTION 1: ENERGY CONSUMPTION (MANUAL INPUT — Zabbix Data)
# Update these values before each report generation.
# Set solar_dc_mppt_kwh to None if not applicable.
# ──────────────────────────────────────────────────────────────
ENERGY_CONSUMPTION_MANUAL = {
    'report_date': '2026-02-26',
    'sites': [
        {
            'sap_id': 'I-KA-KOLR-ENB-9059',
            'dc_load_kwh': 106.42,
            'eb_energy_kwh': 58.21,
            'battery_dc_discharge_kwh': 1.81,
            'solar_dc_mppt_kwh': 16.88,
            'unaccounted_energy_kwh': 29.52,
        },
        {
            'sap_id': 'I-KA-BGLR-ENB-3469',
            'dc_load_kwh': 104.79,
            'eb_energy_kwh': 113.4,
            'battery_dc_discharge_kwh': 0.36,
            'solar_dc_mppt_kwh': None,
            'unaccounted_energy_kwh': 8.98,
        },
        {
            'sap_id': 'I-UP-GOND-ENB-9145',
            'dc_load_kwh': 53.62,
            'eb_energy_kwh': 55.08,
            'battery_dc_discharge_kwh': 12.92,
            'solar_dc_mppt_kwh': None,
            'unaccounted_energy_kwh': 14.38,
        },
    ]
}


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                         DATA LOADING                                   ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def load_and_preprocess(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {folder_path}")
    print(f"  Found {len(csv_files)} CSV files")
    df_list = [pd.read_csv(f, low_memory=False) for f in csv_files]
    final_df = pd.concat(df_list, ignore_index=True)
    final_df['time'] = pd.to_datetime(final_df['time'], format="%Y-%m-%d %I:%M:%S %p")
    final_df = final_df.sort_values(by=['meterId', 'time']).reset_index(drop=True)
    final_df['date'] = final_df['time'].dt.date
    final_df['Time_Diff'] = final_df.groupby(['meterId', 'date'])['time'].diff().dt.total_seconds().fillna(0)
    next_time_diff = final_df['Time_Diff'].shift(-1).fillna(0)
    final_df['energy'] = (final_df['voltage'] * final_df['current'] * next_time_diff * 0.001) / 3600
    final_df['sap_id'] = final_df['meterId'].map(METER_TO_SAP_MAPPING)
    print(f"  Loaded {len(final_df)} rows | {final_df['time'].min()} to {final_df['time'].max()}")
    return final_df


def build_sap_to_meters():
    m = {}
    for meter_id, sap_id in METER_TO_SAP_MAPPING.items():
        m.setdefault(sap_id, []).append(meter_id)
    return m


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                 UNIFIED SESSION DETECTION                              ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def get_unified_sessions(raw_df, meter_ids, gap_threshold_min=5, min_duration_min=2):
    site_df = raw_df[raw_df['meterId'].isin(meter_ids)].copy().sort_values('time').reset_index(drop=True)
    if site_df.empty:
        return []
    site_df['time_rounded'] = site_df['time'].dt.round('15s')
    time_agg = site_df.groupby('time_rounded').agg({
        'battery_status': lambda x: list(x.dropna().unique()), 'energy': 'sum',
    }).reset_index().sort_values('time_rounded').reset_index(drop=True)

    def unified_status(statuses):
        flat = []
        for s in statuses:
            flat.extend(s) if isinstance(s, list) else flat.append(s)
        if any(s == 'CHARGING' for s in flat): return 'CHARGING'
        elif any(s == 'DISCHARGING' for s in flat): return 'DISCHARGING'
        return 'IDLE'

    time_agg['status'] = time_agg['battery_status'].apply(unified_status)
    active = time_agg[time_agg['status'].isin(['CHARGING', 'DISCHARGING'])].copy()
    if active.empty:
        return []
    sessions = []
    cur = {'status': active.iloc[0]['status'], 'start': active.iloc[0]['time_rounded'],
           'end': active.iloc[0]['time_rounded'], 'energy': active.iloc[0]['energy']}
    for i in range(1, len(active)):
        row = active.iloc[i]
        gap_min = (row['time_rounded'] - cur['end']).total_seconds() / 60
        if row['status'] == cur['status'] and gap_min <= gap_threshold_min:
            cur['end'] = row['time_rounded']; cur['energy'] += row['energy']
        else:
            sessions.append(cur)
            cur = {'status': row['status'], 'start': row['time_rounded'],
                   'end': row['time_rounded'], 'energy': row['energy']}
    sessions.append(cur)
    for s in sessions:
        s['duration_min'] = (s['end'] - s['start']).total_seconds() / 60
    return [s for s in sessions if s['duration_min'] >= min_duration_min]


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                  SESSION ENRICHMENT                                    ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def enrich_session(session, raw_df, meter_ids):
    site_df = raw_df[
        (raw_df['meterId'].isin(meter_ids)) &
        (raw_df['time'] >= session['start'] - pd.Timedelta(seconds=30)) &
        (raw_df['time'] <= session['end'] + pd.Timedelta(seconds=30)) &
        (raw_df['battery_status'] == session['status'])
    ]
    if site_df.empty:
        site_df = raw_df[
            (raw_df['meterId'].isin(meter_ids)) &
            (raw_df['time'] >= session['start'] - pd.Timedelta(seconds=30)) &
            (raw_df['time'] <= session['end'] + pd.Timedelta(seconds=30))
        ]
    if site_df.empty:
        return session

    sw = site_df[site_df['time'] <= session['start'] + pd.Timedelta(seconds=30)]
    ew = site_df[site_df['time'] >= session['end'] - pd.Timedelta(seconds=30)]
    session['start_soc'] = round(sw['soc'].mean(), 2) if not sw.empty else None
    session['end_soc'] = round(ew['soc'].mean(), 2) if not ew.empty else None
    if session['start_soc'] is not None and session['end_soc'] is not None:
        session['soc_change'] = abs(session['start_soc'] - session['end_soc'])
        session['equiv_cycle'] = round(session['soc_change'] / 200, 4)
    else:
        session['soc_change'] = 0; session['equiv_cycle'] = 0
    session['total_energy'] = round(abs(site_df['energy'].sum()), 3)
    session['crate_min'] = round(site_df['current'].abs().min() / RATED_CAPACITY_AH, 3)
    session['crate_max'] = round(site_df['current'].abs().max() / RATED_CAPACITY_AH, 3)
    session['volt_min'] = round(site_df['voltage'].min(), 2)
    session['volt_max'] = round(site_df['voltage'].max(), 2)
    sc = site_df.copy(); sc['time_rounded'] = sc['time'].dt.round('15s')
    cs = sc.groupby('time_rounded')['current'].apply(lambda x: x.abs().sum())
    session['curr_min'] = round(cs.min(), 2); session['curr_max'] = round(cs.max(), 2)
    session['temp_min'] = round(site_df['tempMax'].min(), 2)
    session['temp_max'] = round(site_df['tempMax'].max(), 2)
    session['cell_min'] = round(site_df['minVoltage'].min() / 1000, 2)
    session['cell_max'] = round(site_df['maxVoltage'].max() / 1000, 2)
    return session


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                  SECTION COMPUTATIONS                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def compute_all(final_df, sap_to_meters):
    results = {}
    for sap_id in SAP_ID_ORDER:
        meter_ids = sap_to_meters[sap_id]
        site_df = final_df[final_df['meterId'].isin(meter_ids)]
        sessions = get_unified_sessions(final_df, meter_ids)
        for s in sessions:
            s['sap_id'] = sap_id
            enrich_session(s, final_df, meter_ids)

        cha_s = [s for s in sessions if s['status'] == 'CHARGING']
        dis_s = [s for s in sessions if s['status'] == 'DISCHARGING']

        cha_mask = (final_df['meterId'].isin(meter_ids)) & (final_df['battery_status'] == 'CHARGING')
        dis_mask = (final_df['meterId'].isin(meter_ids)) & (final_df['battery_status'] == 'DISCHARGING')
        cha_energy = round(abs(final_df.loc[cha_mask, 'energy'].sum()), 2)
        dis_energy = round(abs(final_df.loc[dis_mask, 'energy'].sum()), 2)
        longest_dis = max([s['duration_min'] for s in dis_s], default=0)

        # S.H.O.W.
        max_vdiff = site_df['voltageDiff'].max()
        volt_imbal = f"{round(max_vdiff/1000,2)} V" if max_vdiff > 5000 else "< 5 V"
        max_temp = round(site_df['tempMax'].max(), 1)
        soh = round(site_df.sort_values('time').iloc[-1]['soh'], 1)

        low_soc = site_df[site_df['soc'] < 20].sort_values('time')
        if low_soc.empty:
            bess_below_20 = "0 mins"
        else:
            ranges, cs, ce = [], low_soc.iloc[0]['time'], low_soc.iloc[0]['time']
            for _, row in low_soc.iterrows():
                if (row['time'] - ce).total_seconds() / 60 > 5:
                    ranges.append((cs, ce)); cs = row['time']
                ce = row['time']
            ranges.append((cs, ce))
            tm = sum([(e - s).total_seconds() / 60 for s, e in ranges])
            h, m = int(tm // 60), int(tm % 60)
            bess_below_20 = f"{h}hr {m}mins" if h > 0 else f"{m} mins"

        total_soc_change = sum(s.get('soc_change', 0) for s in dis_s)
        num_meters = len(meter_ids)
        energy_per_bess = round(dis_energy / num_meters, 3) if num_meters > 0 else 0
        avg_dod = round(total_soc_change, 2)
        throughput = round(cha_energy + dis_energy, 2)

        results[sap_id] = {
            'cha_energy': cha_energy, 'dis_energy': dis_energy,
            'cha_count': len(cha_s), 'dis_count': len(dis_s),
            'longest_dis_min': longest_dis, 'sessions': sessions,
            'volt_imbal': volt_imbal, 'max_temp': max_temp,
            'soh': soh, 'bess_below_20': bess_below_20,
            'avg_dod': avg_dod, 'energy_per_bess': energy_per_bess,
            'throughput': throughput,
        }
    return results


def get_summary_row(sessions, status):
    f = [s for s in sessions if s['status'] == status and 'volt_min' in s]
    if not f: return None
    if status == 'DISCHARGING':
        soc = f"{max(s.get('start_soc',0) for s in f):.2f}% to {min(s.get('end_soc',0) for s in f):.2f}%"
    else:
        soc = f"{min(s.get('start_soc',0) for s in f):.2f}% to {max(s.get('end_soc',0) for s in f):.2f}%"
    return {
        'soc': soc,
        'crate': f"{min(s.get('crate_min',0) for s in f):.3f} to {max(s.get('crate_max',0) for s in f):.3f} C",
        'volt': f"{min(s.get('volt_min',0) for s in f):.2f} to {max(s.get('volt_max',0) for s in f):.2f} V",
        'curr': f"{min(s.get('curr_min',0) for s in f):.2f} – {max(s.get('curr_max',0) for s in f):.2f} A",
        'temp': f"{min(s.get('temp_min',0) for s in f):.0f} - {max(s.get('temp_max',0) for s in f):.0f} °C",
        'cell': f"{min(s.get('cell_min',0) for s in f):.2f} to {max(s.get('cell_max',0) for s in f):.2f} V",
    }


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                   HTML GENERATION                                      ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def fmt_dur(mins):
    h, m = int(mins // 60), int(mins % 60)
    return f"{h} hr {m} mins" if h > 0 else f"{m} mins"

def generate_html(report_date, data, ec):
    # ── Site Overview ──
    site_left = ""
    site_right = ""
    for idx, sap_id in enumerate(SAP_ID_ORDER, 1):
        c = SAP_TO_SITE_CONFIG.get(sap_id, {})
        dg_line = f"<br>{c['dg']}" if c.get('dg') else ""
        site_left += f"""<div class="site-block"><strong>Site {idx}: ( {sap_id} )</strong>
            <p>{c.get('eb','')}<br>{c.get('bess_desc','')}{dg_line}</p></div>"""
        site_right += f"""<div class="site-block"><strong>Site {idx}: ( {sap_id} )</strong>
            <p>{c.get('deploy','')}<br>Commissioned successfully on <strong>{c.get('commissioned','')}</strong></p></div>"""

    # ── Section 1: Zabbix ──
    ec_rows = ""
    for i, s in enumerate(ec['sites'], 1):
        solar = f"{s['solar_dc_mppt_kwh']} kWh" if s['solar_dc_mppt_kwh'] is not None else "--"
        ec_rows += f"<tr><td>{i}</td><td>{s['sap_id']}</td><td>{s['dc_load_kwh']} kWh</td><td>{s['eb_energy_kwh']} kWh</td><td>{s['battery_dc_discharge_kwh']} kWh</td><td>{solar}</td><td>{s['unaccounted_energy_kwh']} kWh</td></tr>"

    # ── Section 2: Op Overview cards ──
    op_html = ""
    for sap_id in SAP_ID_ORDER:
        r = data[sap_id]
        longest = fmt_dur(r['longest_dis_min'])
        op_html += f"""
        <div class="section-title gold">Operational Overview for Telecom Site {sap_id}</div>
        <div class="op-grid">
          <div class="op-card"><div class="op-icon green">⚡</div>
            <div class="op-val">{r['cha_energy']} <span class="op-unit">kWh</span></div>
            <div class="op-lbl">Total Charged Energy</div></div>
          <div class="op-card"><div class="op-icon pink">🔋</div>
            <div class="op-val">{r['dis_energy']} <span class="op-unit">kWh</span></div>
            <div class="op-lbl">Total Discharged Energy</div></div>
          <div class="op-card"><div class="op-icon blue">🔄</div>
            <div class="op-val">{r['cha_count']}</div>
            <div class="op-lbl"># of Charging Sessions</div></div>
          <div class="op-card"><div class="op-icon orange">📊</div>
            <div class="op-val">{r['dis_count']}</div>
            <div class="op-lbl"># of Discharging Sessions</div></div>
          <div class="op-card"><div class="op-icon purple">🕐</div>
            <div class="op-val">{longest}</div>
            <div class="op-lbl">Longest Discharging Session</div></div>
        </div>"""

    # ── Section 3: SHOW tables ──
    sh_rows = ""
    for sap_id in SAP_ID_ORDER:
        r = data[sap_id]
        fdc = f"{r['avg_dod']}% DOD – {r['energy_per_bess']} kWh/ BESS"
        sh_rows += f"<tr><td>{sap_id}</td><td>{r['volt_imbal']}</td><td>{r['max_temp']} °C</td><td>NIL</td><td>{r['soh']}%</td><td>{r['bess_below_20']}</td><td>Zero</td><td>{fdc}</td></tr>"

    ow_rows = ""
    for sap_id in SAP_ID_ORDER:
        r = data[sap_id]
        ow_rows += f"<tr><td>{sap_id}</td><td>{r['avg_dod']}%</td><td>{r['throughput']} kWh</td><td>{r['cha_count']}</td><td>{r['dis_count']}</td><td>NIL</td></tr>"

    # ── Section 4: Summary tables ──
    def summary_table(status):
        rows = ""
        for sap_id in SAP_ID_ORDER:
            row = get_summary_row(data[sap_id]['sessions'], status)
            if row:
                rows += f"<tr><td>{sap_id}</td><td>{row['soc']}</td><td>{row['crate']}</td><td>{row['volt']}</td><td>{row['curr']}</td><td>{row['temp']}</td><td>{row['cell']}</td></tr>"
            else:
                rows += f"<tr><td>{sap_id}</td>" + "<td>No Session</td>" * 6 + "</tr>"
        return rows

    # ── Detailed sessions (>= 10 min) ──
    def detail_rows(status, energy_label):
        all_s = []
        for sap_id in SAP_ID_ORDER:
            for s in data[sap_id]['sessions']:
                if s['status'] == status and s['duration_min'] >= 10:
                    all_s.append(s)
        all_s.sort(key=lambda x: x['start'])
        if not all_s:
            return f"<tr><td colspan='14' style='text-align:center;color:#999;'>No sessions exceeding 10 minutes</td></tr>"
        rows = ""
        for i, s in enumerate(all_s, 1):
            rows += f"""<tr><td>{i}</td><td>{s.get('sap_id','')}</td>
                <td>{s['start'].strftime('%d %b, %I:%M %p')}</td>
                <td>{s['end'].strftime('%d %b, %I:%M %p')}</td>
                <td>{fmt_dur(s['duration_min'])}</td>
                <td>{s.get('start_soc',0):.2f}%</td><td>{s.get('end_soc',0):.2f}%</td>
                <td>{s.get('equiv_cycle',0):.4f}</td><td>{s.get('total_energy',0):.3f}</td>
                <td>{s.get('crate_min',0):.3f} to {s.get('crate_max',0):.3f} C</td>
                <td>{s.get('volt_min',0):.1f} to {s.get('volt_max',0):.1f} V</td>
                <td>{s.get('curr_min',0):.1f} – {s.get('curr_max',0):.1f} A</td>
                <td>{s.get('temp_min',0):.0f} - {s.get('temp_max',0):.0f}°C</td>
                <td>{s.get('cell_min',0):.2f} to {s.get('cell_max',0):.2f} V</td></tr>"""
        return rows

    # ══════════════ FULL HTML ══════════════
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BESS Report – {report_date}</title>
<style>
  @page {{ size: A3 portrait; margin: 6mm; }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: Segoe UI, Tahoma, Geneva, Verdana, sans-serif; font-size: 11px; color:#333; background:#f4f4f4; line-height:1.35; }}
  .page {{ max-width:880px; margin:12px auto; background:#fff; border:1px solid #ccc; border-radius:6px; padding:18px 22px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); }}

  /* ── Header ── */
  .hdr-row {{ display:flex; gap:14px; margin-bottom:16px; }}
  .hdr-box {{ flex:1; border:1px solid #ddd; border-radius:6px; padding:12px 14px; background:#fafafa; }}
  .hdr-box h3 {{ font-size:12.5px; font-weight:700; margin-bottom:8px; color:#222; }}
  .site-block {{ margin-bottom:5px; }}
  .site-block strong {{ font-size:11px; color:#111; }}
  .site-block p {{ font-size:10px; color:#555; margin:1px 0; line-height:1.3; }}

  /* ── Section Titles ── */
  .section-title {{ font-size:13px; font-weight:700; margin:14px 0 5px 0; padding-bottom:2px; }}
  .section-title.red {{ color:#C62828; }}
  .section-title.gold {{ color:#B8860B; }}
  .section-title.blue {{ color:#1565C0; }}
  .section-title.green {{ color:#2E7D32; text-decoration:underline; }}
  .section-title.purple {{ color:#6A1B9A; text-decoration:underline; }}
  .sub-title {{ font-size:11.5px; font-weight:600; margin:2px 0 5px 0; color:#444; }}

  /* ── Tables ── */
  table {{ width:100%; border-collapse:collapse; margin-bottom:10px; font-size:10.5px; }}
  th {{ background:#f0f0f0; font-weight:600; padding:5px 6px; border:1px solid #ccc; text-align:center; font-size:10px; }}
  td {{ padding:4px 6px; border:1px solid #ddd; text-align:center; font-size:10.5px; }}
  tbody tr:nth-child(even) {{ background:#fafafa; }}

  .sh-safety {{ background:#FFCDD2 !important; color:#B71C1C; font-weight:700; }}
  .sh-health {{ background:#C8E6C9 !important; color:#1B5E20; font-weight:700; }}
  .sh-ops    {{ background:#FFE0B2 !important; color:#E65100; font-weight:700; }}
  .sh-warr   {{ background:#D1C4E9 !important; color:#4A148C; font-weight:700; }}

  .dtl th {{ font-size:9px; padding:3px 3px; }}
  .dtl td {{ font-size:9px; padding:2px 3px; }}

  /* ── Op Cards ── */
  .op-grid {{ display:flex; gap:8px; margin-bottom:10px; }}
  .op-card {{ flex:1; border:1px solid #e0e0e0; border-radius:7px; padding:8px 6px; text-align:center; background:#fff; }}
  .op-icon {{ width:28px; height:28px; border-radius:5px; display:inline-flex; align-items:center; justify-content:center; font-size:14px; margin-bottom:3px; }}
  .op-icon.green  {{ background:#e8f5e9; }}
  .op-icon.pink   {{ background:#fce4ec; }}
  .op-icon.blue   {{ background:#e3f2fd; }}
  .op-icon.orange {{ background:#fff3e0; }}
  .op-icon.purple {{ background:#f3e5f5; }}
  .op-val {{ font-size:18px; font-weight:700; color:#222; line-height:1.15; }}
  .op-unit {{ font-size:10px; font-weight:400; color:#888; }}
  .op-lbl {{ font-size:8.5px; color:#999; margin-top:1px; }}

  .note {{ font-size:9.5px; color:#888; margin-top:4px; font-style:italic; }}

  @media print {{
    body {{ background:#fff; }}
    .page {{ box-shadow:none; border:none; margin:0; padding:10px 14px; max-width:100%; }}
  }}
</style>
</head>
<body>
<div class="page">

  <!-- HEADER -->
  <div class="hdr-row">
    <div class="hdr-box">
      <h3>Tower Site Overview</h3>
      {site_left}
    </div>
    <div class="hdr-box">
      <h3>BESS Deployment Details</h3>
      {site_right}
    </div>
  </div>

  <!-- SECTION 1 -->
  <div class="section-title red">Energy Trend observed via Zabbix Data</div>
  <div class="sub-title">Energy Consumption Summary for {ec['report_date']}</div>
  <table>
    <thead><tr><th>Sno</th><th>SAP ID</th><th>DC Load Energy</th><th>EB Energy</th><th>Battery DC Discharge</th><th>Solar DC MPPT</th><th>Unaccounted Energy</th></tr></thead>
    <tbody>{ec_rows}</tbody>
  </table>

  <!-- SECTION 2 -->
  {op_html}

  <!-- SECTION 3 -->
  <div class="section-title blue"><u>S</u>.H.<u>O</u>.W. Analytics based KPIs for Telco BESS</div>
  <table>
    <thead>
      <tr><th rowspan="2" style="vertical-align:middle;">SAP ID</th><th class="sh-safety" colspan="4">Safety KPIs</th><th class="sh-health" colspan="3">Health KPIs</th></tr>
      <tr><th>Voltage Imbalance</th><th>Max Cell Temperatures</th><th>Number of Critical Faults</th><th>State of Health</th><th>BESS Stayed &lt;20% SoC</th><th>Cell Replacement Count</th><th>Full Discharge Capacity Delivered</th></tr>
    </thead>
    <tbody>{sh_rows}</tbody>
  </table>
  <table>
    <thead>
      <tr><th rowspan="2" style="vertical-align:middle;">SAP ID</th><th class="sh-ops" colspan="4">Operational KPIs</th><th class="sh-warr">Warranty KPIs</th></tr>
      <tr><th>Avg Daily Depth of Discharge</th><th>Energy Throughput Logged</th><th>Number of Charging Sessions</th><th>Number of Discharging Sessions</th><th>Warranty Breached Events Detected</th></tr>
    </thead>
    <tbody>{ow_rows}</tbody>
  </table>

  <!-- SECTION 4 -->
  <div class="section-title green">Charging &amp; Discharging Summary</div>
  <div class="sub-title">Discharging Summary for Telco BESS</div>
  <table>
    <thead><tr><th>SAP ID</th><th>SoC Range</th><th>C-Rate Range</th><th>Voltage Range</th><th>Current Range</th><th>Temperature Range</th><th>Cell Voltages</th></tr></thead>
    <tbody>{summary_table('DISCHARGING')}</tbody>
  </table>
  <div class="sub-title">Charging Summary for Telco BESS</div>
  <table>
    <thead><tr><th>SAP ID</th><th>SoC Range</th><th>C-Rate Range</th><th>Voltage Range</th><th>Current Range</th><th>Temperature Range</th><th>Cell Voltages</th></tr></thead>
    <tbody>{summary_table('CHARGING')}</tbody>
  </table>

  <!-- DETAILED SESSIONS -->
  <div class="section-title purple">Detailed Session Analysis</div>
  <div class="sub-title">Analysis of Discharging Sessions</div>
  <table class="dtl">
    <thead><tr><th>Sno</th><th>SAP ID</th><th>Session Start Time</th><th>Session End Time</th><th>Session Duration</th><th>Start SoC</th><th>End SoC</th><th>Equivalent Cycle</th><th>Energy Discharged</th><th>C-Rate Change</th><th>Voltage Range</th><th>Current</th><th>Temperature</th><th>Cell Voltages</th></tr></thead>
    <tbody>{detail_rows('DISCHARGING', 'Discharged')}</tbody>
  </table>
  <div class="sub-title">Analysis of Charging Sessions</div>
  <table class="dtl">
    <thead><tr><th>Sno</th><th>SAP ID</th><th>Session Start Time</th><th>Session End Time</th><th>Session Duration</th><th>Start SoC</th><th>End SoC</th><th>Equivalent Cycle</th><th>Energy Charged</th><th>C-Rate Change</th><th>Voltage Range</th><th>Current</th><th>Temperature</th><th>Cell Voltages</th></tr></thead>
    <tbody>{detail_rows('CHARGING', 'Charged')}</tbody>
  </table>

  <p class="note">Note: For Detailed Session Analysis, only charging and discharging sessions with a duration exceeding 10 minutes are considered.</p>

</div>
</body>
</html>"""
    return html


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                              MAIN                                      ║
# ╚══════════════════════════════════════════════════════════════════════════╝

def main():
    print("=" * 60)
    print("  BESS REPORT GENERATOR")
    print("=" * 60)

    print(f"\n  Loading CSVs from: {CSV_FOLDER_PATH}")
    final_df = load_and_preprocess(CSV_FOLDER_PATH)
    sap_to_meters = build_sap_to_meters()

    report_date = REPORT_DATE if REPORT_DATE else str(final_df['date'].min())
    print(f"  Report Date: {report_date}")

    print("  Computing all sections...")
    data = compute_all(final_df, sap_to_meters)

    print("  Generating HTML...")
    html = generate_html(report_date, data, ENERGY_CONSUMPTION_MANUAL)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    out_path = os.path.join(OUTPUT_FOLDER, f"bess_report_{report_date}.html")
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n  ✓ Report saved: {out_path}")
    print(f"  ✓ Open in browser → Print → Save as PDF (one page)")
    print("=" * 60)
    return out_path


if __name__ == '__main__':
    main()
