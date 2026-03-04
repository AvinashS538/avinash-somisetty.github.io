"""
BESS Report - Data Processing & Calculation Engine
====================================================
This script processes meter CSV files and computes all report sections:
  Section 1: Energy Consumption Summary (Manual Zabbix Data)
  Section 2: Operational Overview per Site
  Section 3: S.H.O.W. Analytics KPIs
  Section 4: Charging & Discharging Summary + Detailed Session Analysis

Usage:
  1. Set CSV_FOLDER_PATH to your CSV files directory
  2. Update ENERGY_CONSUMPTION_MANUAL with Zabbix data
  3. Run: python bess_report_engine.py
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import os
import glob
import json
from datetime import datetime

# ╔══════════════════════════════════════════════════════════════════╗
# ║  CONFIGURATION — EDIT THESE BEFORE EACH RUN                    ║
# ╚══════════════════════════════════════════════════════════════════╝

# Path to folder containing meter CSV files
CSV_FOLDER_PATH = r"./csv_data"  # Place meter CSV files in this folder

# Report date (set manually or auto-detect from data)
REPORT_DATE = None  # Set to "2026-02-28" or None for auto-detect

# Meter to SAP ID Mapping
METER_TO_SAP_MAPPING = {
    'PHzeJNYRFj8XUdw_1': 'I-KA-KOLR-ENB-9059',
    'PMhVP33cBpKta4w_2': 'I-KA-KOLR-ENB-9059',
    'PPyPTGDaNionVkQ_1': 'I-KA-BGLR-ENB-3469',
    'PPyPTGDaNionVkQ_2': 'I-KA-BGLR-ENB-3469',
    'PZ0P20T0OQiSjSg_1': 'I-UP-GOND-ENB-9145',
    'PBzyNe6CGRMO2EA_2': 'I-UP-GOND-ENB-9145',
}

# SAP ID to Site Configuration
SAP_TO_SITE_CONFIG = {
    'I-KA-KOLR-ENB-9059': {
        'site_name': 'Site 1',
        'eb_capacity_kva': 15,
        'solar_capacity_kw': 7.5,
        'bess_capacity_ah': 314,
        'bess_voltage_v': 48,
        'bess_count': 2,
        'bess_config': 'Parallel',
        'bess_backup_kwh': 30,
        'dg_make': 'M&M',
        'dg_model': '10KVA',
        'commissioned_date': '2026-01-09',
    },
    'I-KA-BGLR-ENB-3469': {
        'site_name': 'Site 2',
        'eb_capacity_kva': 15,
        'solar_capacity_kw': None,
        'bess_capacity_ah': 314,
        'bess_voltage_v': 48,
        'bess_count': 2,
        'bess_config': 'Parallel',
        'bess_backup_kwh': 30,
        'dg_make': None,
        'dg_model': None,
        'commissioned_date': '2026-02-05',
    },
    'I-UP-GOND-ENB-9145': {
        'site_name': 'Site 3',
        'eb_capacity_kva': 15,
        'solar_capacity_kw': None,
        'bess_capacity_ah': 314,
        'bess_voltage_v': 48,
        'bess_count': 2,
        'bess_config': 'Parallel',
        'bess_backup_kwh': 30,
        'dg_make': None,
        'dg_model': None,
        'commissioned_date': '2026-02-19',
    },
}

# Battery rated capacity (Ah) — used for C-Rate calculation
RATED_CAPACITY_AH = 314

# Fixed display order of SAP IDs across all sections
SAP_ID_ORDER = [
    'I-KA-KOLR-ENB-9059',
    'I-KA-BGLR-ENB-3469',
    'I-UP-GOND-ENB-9145',
]

# ────────────────────────────────────────────────────────────────────
# SECTION 1: ENERGY CONSUMPTION SUMMARY (MANUAL INPUT — ZABBIX DATA)
# ────────────────────────────────────────────────────────────────────
# Update these values before each report generation.
# All values are in kWh. Set solar_dc_mppt_kwh to None if not applicable.
# All 6 fields per site are manual entry — no computation needed.

ENERGY_CONSUMPTION_MANUAL = {
    'report_date': '2026-02-26',  # Update this each time
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

# ╔══════════════════════════════════════════════════════════════════╗
# ║  DATA LOADING & PREPROCESSING                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

def load_and_preprocess(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {folder_path}")

    print(f"Found {len(csv_files)} CSV files in {folder_path}")

    df_list = []
    for file in csv_files:
        df = pd.read_csv(file, low_memory=False)
        df_list.append(df)

    final_df = pd.concat(df_list, ignore_index=True)

    # Parse timestamps
    final_df['time'] = pd.to_datetime(final_df['time'], format="%Y-%m-%d %I:%M:%S %p")
    final_df = final_df.sort_values(by=['meterId', 'time']).reset_index(drop=True)
    final_df['date'] = final_df['time'].dt.date

    # Calculate energy: voltage * current * time_diff (in kWh)
    final_df['Time_Diff'] = final_df.groupby(['meterId', 'date'])['time'].diff().dt.total_seconds().fillna(0)
    next_time_diff = final_df['Time_Diff'].shift(-1).fillna(0)
    final_df['energy'] = (final_df['voltage'] * final_df['current'] * next_time_diff * 0.001) / 3600
    final_df['capacity'] = (final_df['current'] * next_time_diff) / 3600

    # Map to SAP IDs
    final_df['sap_id'] = final_df['meterId'].map(METER_TO_SAP_MAPPING)

    unmapped = final_df[final_df['sap_id'].isna()]['meterId'].unique()
    if len(unmapped) > 0:
        print(f"  WARNING: Unmapped meter IDs found: {list(unmapped)}")

    print(f"  Loaded {len(final_df)} rows | Time: {final_df['time'].min()} to {final_df['time'].max()}")
    print(f"  Meters: {list(final_df['meterId'].unique())}")

    return final_df


def build_sap_to_meters():
    sap_to_meters = {}
    for meter_id, sap_id in METER_TO_SAP_MAPPING.items():
        sap_to_meters.setdefault(sap_id, []).append(meter_id)
    return sap_to_meters


# ╔══════════════════════════════════════════════════════════════════╗
# ║  UNIFIED SESSION DETECTION (Across Parallel Meters)            ║
# ╚══════════════════════════════════════════════════════════════════╝
# Rules:
# - At any moment, if ANY meter in the SAP ID is CHARGING -> site CHARGING
# - At any moment, if ANY meter is DISCHARGING -> site DISCHARGING
# - Only CHARGING/DISCHARGING statuses matter; others are ignored
# - Merge same-status sessions with gap <= gap_threshold_min
# - After merging, filter out sessions with duration < min_duration_min

def get_unified_sessions(raw_df, meter_ids, gap_threshold_min=5, min_duration_min=2):
    site_df = raw_df[raw_df['meterId'].isin(meter_ids)].copy()
    site_df = site_df.sort_values('time').reset_index(drop=True)
    if site_df.empty:
        return []

    # Round to 15s to align readings from parallel meters
    site_df['time_rounded'] = site_df['time'].dt.round('15s')

    # Aggregate across meters at each time point
    time_agg = site_df.groupby('time_rounded').agg({
        'battery_status': lambda x: list(x.dropna().unique()),
        'energy': 'sum',
    }).reset_index().sort_values('time_rounded').reset_index(drop=True)

    def unified_status(statuses):
        flat = []
        for s in statuses:
            flat.extend(s) if isinstance(s, list) else flat.append(s)
        if any(s == 'CHARGING' for s in flat):
            return 'CHARGING'
        elif any(s == 'DISCHARGING' for s in flat):
            return 'DISCHARGING'
        return 'IDLE'

    time_agg['status'] = time_agg['battery_status'].apply(unified_status)

    # Only active statuses
    active = time_agg[time_agg['status'].isin(['CHARGING', 'DISCHARGING'])].copy()
    if active.empty:
        return []

    # Build sessions with gap merging
    sessions = []
    cur = {
        'status': active.iloc[0]['status'],
        'start': active.iloc[0]['time_rounded'],
        'end': active.iloc[0]['time_rounded'],
        'energy': active.iloc[0]['energy'],
    }

    for i in range(1, len(active)):
        row = active.iloc[i]
        gap_min = (row['time_rounded'] - cur['end']).total_seconds() / 60

        if row['status'] == cur['status'] and gap_min <= gap_threshold_min:
            cur['end'] = row['time_rounded']
            cur['energy'] += row['energy']
        else:
            sessions.append(cur)
            cur = {
                'status': row['status'],
                'start': row['time_rounded'],
                'end': row['time_rounded'],
                'energy': row['energy'],
            }
    sessions.append(cur)

    # Add duration and filter
    for s in sessions:
        s['duration_min'] = (s['end'] - s['start']).total_seconds() / 60

    return [s for s in sessions if s['duration_min'] >= min_duration_min]


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SESSION ENRICHMENT — Detailed Metrics per Session             ║
# ╚══════════════════════════════════════════════════════════════════╝
# - Current: SUM |current| across meters at each time point, then min/max
# - C-Rate:  Per individual meter reading |current| / RATED_CAPACITY_AH
# - SoC:     Average across meters at session boundaries
# - Eq Cycle: |SoC change| / 200  (100→0 = 0.5 cycle)
# - Energy:  Sum across all meters
# - Voltage: min/max of individual readings
# - Temp:    min/max of tempMax
# - Cell V:  min(minVoltage)/max(maxVoltage) in mV → V

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

    # SoC at boundaries (avg across meters)
    start_window = site_df[site_df['time'] <= session['start'] + pd.Timedelta(seconds=30)]
    end_window = site_df[site_df['time'] >= session['end'] - pd.Timedelta(seconds=30)]
    session['start_soc'] = round(start_window['soc'].mean(), 2) if not start_window.empty else None
    session['end_soc'] = round(end_window['soc'].mean(), 2) if not end_window.empty else None

    # Equivalent Cycle: |SoC change| / 200  (100→0 = 0.5 cycle)
    if session['start_soc'] is not None and session['end_soc'] is not None:
        session['soc_change'] = abs(session['start_soc'] - session['end_soc'])
        session['equiv_cycle'] = round(session['soc_change'] / 200, 4)
    else:
        session['soc_change'] = 0
        session['equiv_cycle'] = 0

    # Energy (sum across all meters)
    session['total_energy'] = round(abs(site_df['energy'].sum()), 3)

    # C-Rate: per INDIVIDUAL meter reading |current| / rated capacity
    session['crate_min'] = round(site_df['current'].abs().min() / RATED_CAPACITY_AH, 3)
    session['crate_max'] = round(site_df['current'].abs().max() / RATED_CAPACITY_AH, 3)

    # Voltage range (individual readings)
    session['volt_min'] = round(site_df['voltage'].min(), 2)
    session['volt_max'] = round(site_df['voltage'].max(), 2)

    # Current: SUM absolute values across meters at each time point
    site_df_copy = site_df.copy()
    site_df_copy['time_rounded'] = site_df_copy['time'].dt.round('15s')
    current_summed = site_df_copy.groupby('time_rounded')['current'].apply(lambda x: x.abs().sum())
    session['curr_min'] = round(current_summed.min(), 2)
    session['curr_max'] = round(current_summed.max(), 2)

    # Temperature range
    session['temp_min'] = round(site_df['tempMax'].min(), 2)
    session['temp_max'] = round(site_df['tempMax'].max(), 2)

    # Cell voltages (mV → V)
    session['cell_min'] = round(site_df['minVoltage'].min() / 1000, 2)
    session['cell_max'] = round(site_df['maxVoltage'].max() / 1000, 2)

    return session


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 1: Energy Consumption Summary (Manual Zabbix Data)    ║
# ╚══════════════════════════════════════════════════════════════════╝

def print_section1():
    ec = ENERGY_CONSUMPTION_MANUAL
    print(f"\n{'='*90}")
    print(f"  SECTION 1: Energy Trend observed via Zabbix Data")
    print(f"  Energy Consumption Summary for {ec['report_date']}")
    print(f"{'='*90}")

    header = f"  {'Sno':>3} | {'SAP ID':<22} | {'DC Load':>10} | {'EB Energy':>10} | {'Batt DC Dis':>11} | {'Solar MPPT':>10} | {'Unaccounted':>11}"
    print(header)
    print(f"  {'-'*3}-+-{'-'*22}-+-{'-'*10}-+-{'-'*10}-+-{'-'*11}-+-{'-'*10}-+-{'-'*11}")

    for i, site in enumerate(ec['sites'], 1):
        solar = f"{site['solar_dc_mppt_kwh']:.2f} kWh" if site['solar_dc_mppt_kwh'] is not None else "--"
        print(f"  {i:3d} | {site['sap_id']:<22} | {site['dc_load_kwh']:>7.2f} kWh | {site['eb_energy_kwh']:>7.2f} kWh | {site['battery_dc_discharge_kwh']:>8.2f} kWh | {solar:>10} | {site['unaccounted_energy_kwh']:>8.2f} kWh")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 2: Operational Overview per Site                       ║
# ╚══════════════════════════════════════════════════════════════════╝
# - Energy: voltage * current * time_diff, summed across ALL meters
# - Sessions: Unified timeline, merged, filtered >= 2 min
# - Longest: Max duration from merged discharging sessions

def compute_section2(final_df, sap_to_meters):
    results = {}

    for sap_id in SAP_ID_ORDER:
        meter_ids = sap_to_meters[sap_id]
        site_df = final_df[final_df['meterId'].isin(meter_ids)]
        sessions = get_unified_sessions(final_df, meter_ids)

        cha_sessions = [s for s in sessions if s['status'] == 'CHARGING']
        dis_sessions = [s for s in sessions if s['status'] == 'DISCHARGING']

        # Energy from raw data (sum across all meters)
        cha_mask = (final_df['meterId'].isin(meter_ids)) & (final_df['battery_status'] == 'CHARGING')
        dis_mask = (final_df['meterId'].isin(meter_ids)) & (final_df['battery_status'] == 'DISCHARGING')
        charged_energy = round(abs(final_df.loc[cha_mask, 'energy'].sum()), 2)
        discharged_energy = round(abs(final_df.loc[dis_mask, 'energy'].sum()), 2)

        # Longest discharging from merged sessions
        longest_dis = max([s['duration_min'] for s in dis_sessions], default=0)

        results[sap_id] = {
            'charged_energy': charged_energy,
            'discharged_energy': discharged_energy,
            'charging_sessions': len(cha_sessions),
            'discharging_sessions': len(dis_sessions),
            'longest_discharging_min': longest_dis,
            'sessions': sessions,
        }

    return results


def print_section2(section2_results):
    print(f"\n{'='*90}")
    print(f"  SECTION 2: Operational Overview for Telecom Sites")
    print(f"{'='*90}")

    for sap_id in SAP_ID_ORDER:
        r = section2_results[sap_id]
        longest = r['longest_discharging_min']
        hrs, mins = int(longest // 60), int(longest % 60)
        longest_str = f"{hrs} hr {mins} mins" if hrs > 0 else f"{mins} mins"

        print(f"\n  Site: {sap_id}")
        print(f"    Total Charged Energy:    {r['charged_energy']} kWh")
        print(f"    Total Discharged Energy: {r['discharged_energy']} kWh")
        print(f"    # of Charging Sessions:  {r['charging_sessions']}")
        print(f"    # of Discharging Sessions: {r['discharging_sessions']}")
        print(f"    Longest Discharging:     {longest_str}")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 3: S.H.O.W. Analytics KPIs                           ║
# ╚══════════════════════════════════════════════════════════════════╝
# Safety:  Voltage Imbalance, Max Temp, Critical Faults, SoH
# Health:  BESS <20% SoC, Cell Replacement, Full Discharge Capacity
# Ops:     Avg Daily DoD, Energy Throughput, Sessions
# Warranty: Breached Events

def compute_section3(final_df, sap_to_meters, section2_results):
    results = {}

    for sap_id in SAP_ID_ORDER:
        meter_ids = sap_to_meters[sap_id]
        site_df = final_df[final_df['meterId'].isin(meter_ids)]
        sessions = section2_results[sap_id]['sessions']
        dis_sessions = [s for s in sessions if s['status'] == 'DISCHARGING']
        cha_sessions = [s for s in sessions if s['status'] == 'CHARGING']

        # ── SAFETY KPIs ──
        max_vdiff = site_df['voltageDiff'].max()
        voltage_imbalance = f"{round(max_vdiff / 1000, 2)} V" if max_vdiff > 5000 else "< 5 V"
        max_temp = round(site_df['tempMax'].max(), 1)
        soh = round(site_df.sort_values('time').iloc[-1]['soh'], 1)

        # ── HEALTH KPIs ──
        # BESS <20% SoC duration
        low_soc = site_df[site_df['soc'] < 20].sort_values('time')
        if low_soc.empty:
            bess_below_20 = "0 mins"
        else:
            time_ranges = []
            cs, ce = low_soc.iloc[0]['time'], low_soc.iloc[0]['time']
            for _, row in low_soc.iterrows():
                if (row['time'] - ce).total_seconds() / 60 > 5:
                    time_ranges.append((cs, ce))
                    cs = row['time']
                ce = row['time']
            time_ranges.append((cs, ce))
            total_mins = sum([(e - s).total_seconds() / 60 for s, e in time_ranges])
            h, m = int(total_mins // 60), int(total_mins % 60)
            bess_below_20 = f"{h}hr {m}mins" if h > 0 else f"{m} mins"

        # Full Discharge Capacity & Avg Daily DoD
        # DOD = total SoC change (SoC is already avg across meters, no division)
        # Energy per BESS = total discharge energy / num_meters
        total_soc_change = 0
        for s in dis_sessions:
            enrich_session(s, final_df, meter_ids)
            total_soc_change += s.get('soc_change', 0)

        total_dis_energy = abs(site_df[site_df['battery_status'] == 'DISCHARGING']['energy'].sum())
        num_meters = len(meter_ids)
        energy_per_bess = round(total_dis_energy / num_meters, 3) if num_meters > 0 else 0
        avg_dod = round(total_soc_change, 2)
        full_discharge_cap = f"{avg_dod}% DOD - {energy_per_bess} kWh/ BESS"

        # ── OPERATIONAL KPIs ──
        cha_energy = abs(site_df[site_df['battery_status'] == 'CHARGING']['energy'].sum())
        throughput = round(cha_energy + total_dis_energy, 2)

        results[sap_id] = {
            'safety': {
                'voltage_imbalance': voltage_imbalance,
                'max_cell_temp': f"{max_temp} °C",
                'critical_faults': 'NIL',
                'state_of_health': f"{soh}%",
            },
            'health': {
                'bess_below_20_soc': bess_below_20,
                'cell_replacement': 'Zero',
                'full_discharge_capacity': full_discharge_cap,
            },
            'operational': {
                'avg_daily_dod': f"{avg_dod}%",
                'energy_throughput': f"{throughput} kWh",
                'charging_sessions': section2_results[sap_id]['charging_sessions'],
                'discharging_sessions': section2_results[sap_id]['discharging_sessions'],
            },
            'warranty': {
                'breached_events': 'NIL',
            },
        }

    return results


def print_section3(section3_results):
    print(f"\n{'='*90}")
    print(f"  SECTION 3: S.H.O.W. Analytics based KPIs for Telco BESS")
    print(f"{'='*90}")

    # Safety & Health table
    print(f"\n  {'SAP ID':<22} | {'Volt Imbal':>10} | {'Max Temp':>8} | {'Faults':>7} | {'SoH':>5} | {'<20% SoC':>10} | {'Cell Rep':>8} | {'Full Discharge Capacity':<30}")
    print(f"  {'-'*22}-+-{'-'*10}-+-{'-'*8}-+-{'-'*7}-+-{'-'*5}-+-{'-'*10}-+-{'-'*8}-+-{'-'*30}")
    for sap_id in SAP_ID_ORDER:
        r = section3_results[sap_id]
        s = r['safety']
        h = r['health']
        print(f"  {sap_id:<22} | {s['voltage_imbalance']:>10} | {s['max_cell_temp']:>8} | {s['critical_faults']:>7} | {s['state_of_health']:>5} | {h['bess_below_20_soc']:>10} | {h['cell_replacement']:>8} | {h['full_discharge_capacity']:<30}")

    # Operational & Warranty table
    print(f"\n  {'SAP ID':<22} | {'Avg DoD':>8} | {'Throughput':>12} | {'Cha Sess':>8} | {'Dis Sess':>8} | {'Warranty Breached':>18}")
    print(f"  {'-'*22}-+-{'-'*8}-+-{'-'*12}-+-{'-'*8}-+-{'-'*8}-+-{'-'*18}")
    for sap_id in SAP_ID_ORDER:
        r = section3_results[sap_id]
        o = r['operational']
        w = r['warranty']
        print(f"  {sap_id:<22} | {o['avg_daily_dod']:>8} | {o['energy_throughput']:>12} | {o['charging_sessions']:>8} | {o['discharging_sessions']:>8} | {w['breached_events']:>18}")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 4: Charging & Discharging Summary + Detailed Sessions ║
# ╚══════════════════════════════════════════════════════════════════╝
# Summary: Per SAP ID, min/max across all sessions (>= 2 min)
#   - SoC: Discharging max→min, Charging min→max
#   - C-Rate: Per individual meter |current|/314
#   - Current: Sum |current| across meters at each time point
#   - Cell V: min(minVoltage)/max(maxVoltage) in mV→V
# Detailed: Combined table all SAP IDs, sessions >= 10 min only

def compute_section4(final_df, sap_to_meters, section2_results):
    all_enriched = {}

    for sap_id in SAP_ID_ORDER:
        meter_ids = sap_to_meters[sap_id]
        sessions = section2_results[sap_id]['sessions']

        for s in sessions:
            s['sap_id'] = sap_id
            enrich_session(s, final_df, meter_ids)

        all_enriched[sap_id] = sessions

    return all_enriched


def get_summary_row(sessions, status):
    filtered = [s for s in sessions if s['status'] == status and 'volt_min' in s]
    if not filtered:
        return None

    if status == 'DISCHARGING':
        soc_range = f"{max(s.get('start_soc', 0) for s in filtered):.2f}% to {min(s.get('end_soc', 0) for s in filtered):.2f}%"
    else:
        soc_range = f"{min(s.get('start_soc', 0) for s in filtered):.2f}% to {max(s.get('end_soc', 0) for s in filtered):.2f}%"

    return {
        'soc_range': soc_range,
        'crate_range': f"{min(s.get('crate_min', 0) for s in filtered):.3f} to {max(s.get('crate_max', 0) for s in filtered):.3f} C",
        'volt_range': f"{min(s.get('volt_min', 0) for s in filtered):.2f} to {max(s.get('volt_max', 0) for s in filtered):.2f} V",
        'curr_range': f"{min(s.get('curr_min', 0) for s in filtered):.2f} to {max(s.get('curr_max', 0) for s in filtered):.2f} A",
        'temp_range': f"{min(s.get('temp_min', 0) for s in filtered):.0f} - {max(s.get('temp_max', 0) for s in filtered):.0f} °C",
        'cell_range': f"{min(s.get('cell_min', 0) for s in filtered):.2f} to {max(s.get('cell_max', 0) for s in filtered):.2f} V",
    }


def print_section4(section4_results):
    print(f"\n{'='*90}")
    print(f"  SECTION 4: Charging & Discharging Summary")
    print(f"{'='*90}")

    # Discharging Summary
    print(f"\n  Discharging Summary for Telco BESS:")
    print(f"  {'SAP ID':<22} | {'SoC Range':>20} | {'C-Rate':>16} | {'Voltage':>18} | {'Current':>18} | {'Temperature':>12} | {'Cell V':>14}")
    print(f"  {'-'*22}-+-{'-'*20}-+-{'-'*16}-+-{'-'*18}-+-{'-'*18}-+-{'-'*12}-+-{'-'*14}")

    for sap_id in SAP_ID_ORDER:
        sessions = section4_results[sap_id]
        row = get_summary_row(sessions, 'DISCHARGING')
        if row:
            print(f"  {sap_id:<22} | {row['soc_range']:>20} | {row['crate_range']:>16} | {row['volt_range']:>18} | {row['curr_range']:>18} | {row['temp_range']:>12} | {row['cell_range']:>14}")
        else:
            print(f"  {sap_id:<22} | {'No Session':>20} | {'No Session':>16} | {'No Session':>18} | {'No Session':>18} | {'No Session':>12} | {'No Session':>14}")

    # Charging Summary
    print(f"\n  Charging Summary for Telco BESS:")
    print(f"  {'SAP ID':<22} | {'SoC Range':>20} | {'C-Rate':>16} | {'Voltage':>18} | {'Current':>18} | {'Temperature':>12} | {'Cell V':>14}")
    print(f"  {'-'*22}-+-{'-'*20}-+-{'-'*16}-+-{'-'*18}-+-{'-'*18}-+-{'-'*12}-+-{'-'*14}")

    for sap_id in SAP_ID_ORDER:
        sessions = section4_results[sap_id]
        row = get_summary_row(sessions, 'CHARGING')
        if row:
            print(f"  {sap_id:<22} | {row['soc_range']:>20} | {row['crate_range']:>16} | {row['volt_range']:>18} | {row['curr_range']:>18} | {row['temp_range']:>12} | {row['cell_range']:>14}")
        else:
            print(f"  {sap_id:<22} | {'No Session':>20} | {'No Session':>16} | {'No Session':>18} | {'No Session':>18} | {'No Session':>12} | {'No Session':>14}")

    # Detailed Session Analysis (>= 10 min, combined across all SAP IDs)
    all_dis = []
    all_cha = []
    for sap_id in SAP_ID_ORDER:
        sessions = section4_results[sap_id]
        for s in sessions:
            if s['duration_min'] >= 10:
                if s['status'] == 'DISCHARGING':
                    all_dis.append(s)
                elif s['status'] == 'CHARGING':
                    all_cha.append(s)

    all_dis.sort(key=lambda x: x['start'])
    all_cha.sort(key=lambda x: x['start'])

    def print_session_table(title, session_list):
        print(f"\n  {title} ({len(session_list)} sessions):")
        if not session_list:
            print(f"  No sessions >= 10 minutes")
            return

        print(f"  {'Sno':>3} | {'SAP ID':<22} | {'Start Time':<20} | {'End Time':<20} | {'Duration':<11} | {'Start SoC':>9} | {'End SoC':>7} | {'Eq Cyc':>6} | {'Energy':>7} | {'C-Rate':>14} | {'Voltage':>16} | {'Current':>16} | {'Temp':>9} | {'Cell V':>14}")
        print(f"  {'-'*3}-+-{'-'*22}-+-{'-'*20}-+-{'-'*20}-+-{'-'*11}-+-{'-'*9}-+-{'-'*7}-+-{'-'*6}-+-{'-'*7}-+-{'-'*14}-+-{'-'*16}-+-{'-'*16}-+-{'-'*9}-+-{'-'*14}")

        for i, s in enumerate(session_list, 1):
            h, m = int(s['duration_min'] // 60), int(s['duration_min'] % 60)
            dur = f"{h}hr {m}min" if h > 0 else f"{m} min"
            print(f"  {i:3d} | {s.get('sap_id',''):22} | {s['start'].strftime('%d %b, %I:%M %p'):<20} | {s['end'].strftime('%d %b, %I:%M %p'):<20} | {dur:<11} | {s.get('start_soc',0):8.2f}% | {s.get('end_soc',0):6.2f}% | {s.get('equiv_cycle',0):.4f} | {s.get('total_energy',0):6.3f} | {s.get('crate_min',0):.3f} to {s.get('crate_max',0):.3f} | {s.get('volt_min',0):.1f} to {s.get('volt_max',0):.1f} | {s.get('curr_min',0):.1f} to {s.get('curr_max',0):.1f} | {s.get('temp_min',0):.0f}-{s.get('temp_max',0):.0f}°C | {s.get('cell_min',0):.2f} to {s.get('cell_max',0):.2f}")

    print(f"\n{'='*90}")
    print(f"  Detailed Session Analysis")
    print(f"{'='*90}")
    print_session_table("Analysis of Discharging Sessions", all_dis)
    print_session_table("Analysis of Charging Sessions", all_cha)
    print(f"\n  Note: Only charging and discharging sessions with duration exceeding 10 minutes are considered.")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  MAIN — Run All Sections                                       ║
# ╚══════════════════════════════════════════════════════════════════╝

def main():
    print("=" * 90)
    print("  BESS REPORT — DATA PROCESSING ENGINE")
    print("=" * 90)

    # Load data
    print(f"\n  Loading CSV files from: {CSV_FOLDER_PATH}")
    final_df = load_and_preprocess(CSV_FOLDER_PATH)
    sap_to_meters = build_sap_to_meters()

    # Detect report date
    if REPORT_DATE:
        report_date = REPORT_DATE
    else:
        report_date = str(final_df['date'].min())
    print(f"\n  Report Date: {report_date}")
    print(f"  SAP IDs: {list(sap_to_meters.keys())}")
    for sap_id, meters in sap_to_meters.items():
        config = SAP_TO_SITE_CONFIG.get(sap_id, {})
        print(f"    {sap_id} ({config.get('site_name', '?')}): {meters}")

    # Section 1
    print_section1()

    # Section 2
    section2_results = compute_section2(final_df, sap_to_meters)
    print_section2(section2_results)

    # Section 3
    section3_results = compute_section3(final_df, sap_to_meters, section2_results)
    print_section3(section3_results)

    # Section 4
    section4_results = compute_section4(final_df, sap_to_meters, section2_results)
    print_section4(section4_results)

    print(f"\n{'='*90}")
    print(f"  ALL SECTIONS COMPLETE")
    print(f"{'='*90}")

    return {
        'report_date': report_date,
        'energy_consumption': ENERGY_CONSUMPTION_MANUAL,
        'section2': section2_results,
        'section3': section3_results,
        'section4': section4_results,
        'sap_to_meters': sap_to_meters,
        'site_config': SAP_TO_SITE_CONFIG,
    }


if __name__ == '__main__':
    report_data = main()
