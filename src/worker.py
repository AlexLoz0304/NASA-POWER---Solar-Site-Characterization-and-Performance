"""
worker.py — NASA POWER Solar Site Characterization Worker
==========================================================
Consumes point-level job IDs from the HotQueue (Redis db=1) and produces
detailed solar site characterization results stored in Redis db=3.

For each job the worker:
  1. Marks the job RUNNING.
  2. Reads the location record (lat, lon, all 10 parameter series) from Redis db=0
     using the same key scheme as FastAPI_api.py.
  3. Filters out NASA POWER missing-data sentinels (-999 / -999.0) before
     computing any statistic — those values must never enter calculations.
  4. Computes a rich set of solar site characterization statistics:
        • Mean / median / std / min / max for all relevant parameters.
        • Best and worst months by average irradiance.
        • Panel tilt recommendation (= abs(latitude), fixed-tilt rule-of-thumb).
        • Panel azimuth recommendation (south-facing in N hemisphere, north in S).
        • Estimated annual DC energy yield (kWh/kWp) based on performance ratio.
        • Temperature de-rating factor (panel efficiency loss from heat).
        • Soiling proxy (correlation between precipitation and irradiance drop).
        • Solar variability index (std/mean of daily irradiance).
  5. If the job payload contained multiple location_ids (POST /jobs body),
     the worker also writes a cross-location comparison summary that ranks all
     locations by mean annual irradiance and flags the best site.
     NOTE: Each location_id maps to a separate job; the comparison is written
     only when the worker detects sibling jobs sharing the same request group.
     For simplicity in the current architecture, each job carries its own
     location's results plus an optional "peer_summary" if peer results are
     already in db=3.
  6. Saves the full result dict to Redis db=3 via save_job_result().
  7. Marks the job FINISHED -- SUCCESS (or FINISHED -- ERROR on failure).

Redis key conventions read here (all in db=0):
  location:{lat:.1f}:{lon:.1f}   — hash with all parameter series + metadata
  location_id:{uuid}             — maps UUID -> location hash key
"""

import json
import logging
import math
import os
import statistics
from collections import defaultdict
from typing import Optional

from jobs import (
    JobStatus,
    get_job_by_id,
    q,
    rd,
    save_job_result,
    start_job,
    update_job_status,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# NASA POWER sentinel for missing/invalid data — must be excluded from all stats.
SENTINEL = -999.0

# The 10 parameters stored per location (same list as FastAPI_api.py).
NASA_PARAMETERS = [
    "ALLSKY_SFC_SW_DWN",
    "CLRSKY_SFC_SW_DWN",
    "ALLSKY_KT",
    "T2M",
    "T2M_MAX",
    "T2M_MIN",
    "WS10M",
    "RH2M",
    "PRECTOTCORR",
    "CLOUD_AMT",
]

# Typical performance ratio for a fixed-tilt crystalline-silicon PV system.
# Accounts for inverter losses, wiring, soiling, temperature de-rating, etc.
PERFORMANCE_RATIO = 0.78

# Temperature coefficient of power for typical crystalline-silicon PV (%/°C).
# Each 1 °C above STC (25 °C) reduces panel output by this fraction.
TEMP_COEFF = 0.004  # 0.4 %/°C

# Standard Test Condition (STC) reference temperature for PV rating (°C).
STC_TEMP = 25.0

# Redis key prefixes (must match FastAPI_api.py).
LOCATION_PREFIX    = "location:"
LOCATION_ID_PREFIX = "location_id:"


# ---------------------------------------------------------------------------
# Redis CRUD helpers
# ---------------------------------------------------------------------------

def _read_location_by_id(loc_id: str) -> Optional[dict]:
    """
    CRUD READ — Fetch a stored location record from Redis db=0 by its UUID.

    Follows the two-step lookup used in FastAPI_api.py:
      1. GET  location_id:{uuid}  -> location hash key (e.g. 'location:40.5:-74.0')
      2. HGETALL <hash_key>       -> all fields of the location hash

    Returns a decoded dict with float lat/lon and parsed parameter lists,
    or None if the location UUID is unknown.
    """
    raw_key = rd.get(f"{LOCATION_ID_PREFIX}{loc_id}")
    if raw_key is None:
        logger.warning(f"[READ] location_id:{loc_id} not found in db=0")
        return None

    hash_key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
    raw_hash = rd.hgetall(hash_key)
    if not raw_hash:
        logger.warning(f"[READ] hash key {hash_key} is empty or missing")
        return None

    # Decode bytes -> str for every field
    decoded = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in raw_hash.items()
    }

    record = {
        "id":         decoded.get("id"),
        "key":        hash_key,
        "lat":        float(decoded.get("lat", 0.0)),
        "lon":        float(decoded.get("lon", 0.0)),
        "start_date": decoded.get("start_date"),
        "end_date":   decoded.get("end_date"),
        "name":       decoded.get("name"),
    }

    # Parse each parameter series from its JSON-encoded list
    for param in NASA_PARAMETERS:
        if param in decoded:
            try:
                val = json.loads(decoded[param])
                record[param] = val if isinstance(val, list) else [val]
            except Exception:
                record[param] = []
        else:
            record[param] = []

    return record


def _clean(series: list) -> list:
    """
    Return a copy of `series` with all sentinel values removed.

    NASA POWER encodes missing/invalid days as -999 (or -999.0). These must
    be stripped before any statistical operation to avoid skewing results.
    """
    return [v for v in series if v is not None and v > SENTINEL + 0.5]


# ---------------------------------------------------------------------------
# Solar site characterization helpers
# ---------------------------------------------------------------------------

def _safe_stats(values: list) -> dict:
    """
    Compute descriptive statistics for a cleaned numeric series.

    Returns a dict with mean, median, std, min, max, and count.
    All values are rounded to 4 decimal places. Returns zeros if empty.
    """
    n = len(values)
    if n == 0:
        return {"mean": None, "median": None, "std": None, "min": None, "max": None, "count": 0}
    return {
        "mean":   round(statistics.mean(values),   4),
        "median": round(statistics.median(values), 4),
        "std":    round(statistics.stdev(values),  4) if n > 1 else 0.0,
        "min":    round(min(values), 4),
        "max":    round(max(values), 4),
        "count":  n,
    }


def _monthly_means(series: list, start_date: str) -> dict:
    """
    Group a daily series by calendar month and return the mean per month.

    `start_date` is a YYYYMMDD string; the i-th value in `series` corresponds
    to the day at offset i from start_date.

    Every calendar month that falls within the series range is always present
    in the output.  If a month has no valid (non-sentinel) values, its entry
    is ``null`` (Python ``None``).  This ensures callers always receive the
    full date range regardless of data gaps caused by sentinel values.

    Returns a dict mapping 'YYYY-MM' -> mean (float) or None.
    """
    if not series or not start_date or len(start_date) < 8:
        return {}

    year  = int(start_date[:4])
    month = int(start_date[4:6])
    day   = int(start_date[6:8])

    # Build a (year, month) -> [values] mapping by stepping through the series
    # Also track every (year, month) key we encounter so all months appear.
    buckets: dict = defaultdict(list)
    all_months: list = []
    seen: set = set()

    # Days-in-month lookup (ignore leap year for simplicity)
    dim = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    cur_year, cur_month, cur_day = year, month, day
    for v in series:
        key = (cur_year, cur_month)
        if key not in seen:
            seen.add(key)
            all_months.append(key)
        if v is not None and v > SENTINEL + 0.5:
            buckets[key].append(v)
        # Advance day
        cur_day += 1
        days_in_cur_month = 29 if (cur_month == 2 and cur_year % 4 == 0) else dim[cur_month]
        if cur_day > days_in_cur_month:
            cur_day = 1
            cur_month += 1
            if cur_month > 12:
                cur_month = 1
                cur_year += 1

    result = {}
    for (y, m) in all_months:
        label = f"{y:04d}-{m:02d}"
        vals = buckets.get((y, m), [])
        result[label] = round(statistics.mean(vals), 4) if vals else None
    return result


def _panel_orientation(lat: float) -> dict:
    """
    Recommend fixed-tilt panel orientation for a given latitude.

    Rules of thumb widely used in solar PV engineering:
      - Tilt angle  ≈ |latitude| (maximises annual irradiance on a fixed panel).
      - Azimuth     = 180° (south-facing) in the Northern Hemisphere,
                    =   0° (north-facing) in the Southern Hemisphere.
      - At the equator (lat == 0) either azimuth is equivalent; south is returned.

    Returns a dict with tilt_deg, azimuth_deg, and an explanation string.
    """
    tilt    = round(abs(lat), 1)
    azimuth = 180.0 if lat >= 0 else 0.0
    direction = "South" if lat >= 0 else "North"
    return {
        "recommended_tilt_deg":    tilt,
        "recommended_azimuth_deg": azimuth,
        "facing":                  direction,
        "note": (
            f"Fixed-tilt rule-of-thumb: tilt ≈ |lat| = {tilt}° facing {direction}. "
            "Optimise further with a site-specific irradiance model."
        ),
    }


def _energy_yield(mean_irradiance_kwh_m2_day: float, temp_mean_c: float, days: int) -> dict:
    """
    Estimate annual DC energy yield (kWh/kWp) for a 1-kWp PV system.

    Formula:
        E = G_daily × days × PR × T_derate
    where:
        G_daily  = mean daily irradiance on the panel plane (kWh/m²/day)
                   — approximated here as the horizontal all-sky irradiance
        days     = number of days in the analysis period
        PR       = performance ratio (default 0.78)
        T_derate = 1 - TEMP_COEFF × max(0, T_mean - STC_TEMP)
                 = temperature de-rating factor

    Returns a dict with yield estimate and the temperature de-rating factor.
    """
    # Temperature de-rating: panels lose ~0.4 %/°C above 25 °C STC
    delta_t    = max(0.0, temp_mean_c - STC_TEMP)
    t_derate   = round(1.0 - TEMP_COEFF * delta_t, 4)
    # Annual energy estimate for a 1-kWp nameplate system
    annual_kwh = round(mean_irradiance_kwh_m2_day * days * PERFORMANCE_RATIO * t_derate, 2)
    return {
        "estimated_annual_yield_kwh_per_kwp": annual_kwh,
        "performance_ratio_used":             PERFORMANCE_RATIO,
        "temp_derate_factor":                 t_derate,
        "delta_t_above_stc_c":               round(delta_t, 2),
        "note": (
            "Yield = G_daily × days × PR × T_derate  (horizontal irradiance, 1-kWp system). "
            "Use PVGIS or SAM for tilted-surface optimisation."
        ),
    }


def _solar_variability_index(irradiance_series: list) -> Optional[float]:
    """
    Compute the Solar Variability Index (SVI) = std / mean of daily irradiance.

    A low SVI (< 0.3) indicates stable, predictable solar resource.
    A high SVI (> 0.5) indicates highly variable conditions (cloudy / seasonal swings).

    Returns None if the series is too short.
    """
    cleaned = _clean(irradiance_series)
    if len(cleaned) < 2:
        return None
    m = statistics.mean(cleaned)
    if m == 0:
        return None
    return round(statistics.stdev(cleaned) / m, 4)


def _ghi_suitability_score(mean_ghi: float) -> dict:
    """GHI-based PV site suitability score from 0–10.

    Calibrated so that 7 kWh/m²/day (high-desert irradiance) scores 10.
    Useful for ranking candidate sites at a glance.
    """
    score = round(min(10.0, mean_ghi * 10.0 / 7.0), 1)
    if score >= 8:
        label = "Excellent"
    elif score >= 6:
        label = "Good"
    elif score >= 4:
        label = "Moderate"
    else:
        label = "Poor"
    return {
        "score": score,
        "label": label,
        "note": "Score 0–10: ≥8 Excellent, ≥6 Good, ≥4 Moderate, <4 Poor. 7 kWh/m²/day = 10.",
    }


def _best_worst_months(monthly_means: dict) -> dict:
    """
    Return the calendar month with the highest and lowest mean irradiance.

    `monthly_means` is a dict of 'YYYY-MM' -> mean_value (or None for months
    where all values were sentinel) as returned by _monthly_means().
    None entries are excluded when finding best/worst.
    """
    if not monthly_means:
        return {"best_month": None, "worst_month": None}
    valid = {k: v for k, v in monthly_means.items() if v is not None}
    if not valid:
        return {"best_month": None, "worst_month": None}
    best  = max(valid, key=valid.__getitem__)
    worst = min(valid, key=valid.__getitem__)
    return {
        "best_month":  {"month": best,  "mean_kwh_m2_day": valid[best]},
        "worst_month": {"month": worst, "mean_kwh_m2_day": valid[worst]},
    }


# ---------------------------------------------------------------------------
# Core analysis function
# ---------------------------------------------------------------------------

def _analyze_location(record: dict) -> dict:
    """
    Compute all solar site characterization metrics for one stored location.

    Input:
        record — dict returned by _read_location_by_id() with all parameter lists.

    Output:
        A result dict saved to Redis db=3; structure:
        {
            "location": { id, lat, lon, name, start_date, end_date },
            "panel_orientation": { ... },
            "parameter_stats": { param: {mean, median, std, min, max, count} },
            "irradiance": {
                "monthly_means":     { YYYY-MM: mean_kwh_m2_day },
                "best_worst_months": { ... },
                "variability_index": float,
                "clearness_index":   float,   # mean ALLSKY_KT
            },
            "energy_yield": { ... },
            "temperature": { summary stats + annual mean },
            "wind":        { summary stats },
            "humidity":    { summary stats },
            "cloud_cover": { summary stats },
            "precipitation":{ summary stats },
            "sentinel_counts": { param: n_sentinel_days },
        }
    """
    lat  = record["lat"]
    lon  = record["lon"]
    name = record.get("name") or f"({lat}, {lon})"

    # --- Count sentinel (-999) days before cleaning ---
    sentinel_counts = {
        p: sum(1 for v in record.get(p, []) if v is not None and abs(v - SENTINEL) < 0.5)
        for p in NASA_PARAMETERS
    }

    # --- Clean all parameter series (remove sentinels) ---
    series = {p: _clean(record.get(p, [])) for p in NASA_PARAMETERS}

    # Irradiance series (primary metric for solar characterisation)
    irr_series   = series["ALLSKY_SFC_SW_DWN"]
    clr_series   = series["CLRSKY_SFC_SW_DWN"]
    kt_series    = series["ALLSKY_KT"]
    temp_series  = series["T2M"]
    n_days       = len(irr_series)

    # --- Per-parameter descriptive stats ---
    param_stats = {p: _safe_stats(series[p]) for p in NASA_PARAMETERS}

    # --- Monthly mean irradiance breakdown ---
    monthly_irr = _monthly_means(record.get("ALLSKY_SFC_SW_DWN", []), record.get("start_date", ""))
    bw_months   = _best_worst_months(monthly_irr)

    # --- Panel orientation recommendation ---
    orientation = _panel_orientation(lat)

    # --- Energy yield estimate ---
    mean_irr  = param_stats["ALLSKY_SFC_SW_DWN"]["mean"] or 0.0
    mean_temp = param_stats["T2M"]["mean"] or STC_TEMP
    yield_est = _energy_yield(mean_irr, mean_temp, n_days)

    # --- Solar variability index ---
    svi = _solar_variability_index(record.get("ALLSKY_SFC_SW_DWN", []))

    # --- Clearness index (mean ALLSKY_KT): 0=fully overcast, 1=perfectly clear ---
    mean_kt = param_stats["ALLSKY_KT"]["mean"]

    # --- Clear-sky utilisation ratio ---
    # How much of the theoretical clear-sky irradiance is actually available.
    mean_clr = param_stats["CLRSKY_SFC_SW_DWN"]["mean"] or 0.0
    clr_utilisation = round(mean_irr / mean_clr, 4) if mean_clr > 0 else None

    result = {
        "location": {
            "id":         record.get("id"),
            "lat":        lat,
            "lon":        lon,
            "name":       name,
            "start_date": record.get("start_date"),
            "end_date":   record.get("end_date"),
            "n_days":     n_days,
        },
        # Recommended panel mounting geometry
        "panel_orientation": orientation,
        # Descriptive stats for every NASA POWER parameter
        "parameter_stats": param_stats,
        # Irradiance-focused breakdown
        "irradiance": {
            "mean_kwh_m2_day":        mean_irr,
            "monthly_means":          monthly_irr,
            "best_worst_months":      bw_months,
            "variability_index":      svi,
            "clearness_index_mean":   mean_kt,
            "clear_sky_utilisation":  clr_utilisation,
            "interpretation": (
                "variability_index < 0.3: stable resource; "
                "clearness_index_mean > 0.6: predominantly clear sky; "
                "clear_sky_utilisation close to 1: low cloud impact."
            ),
        },
        # Annual energy production estimate for a 1-kWp system
        "energy_yield": yield_est,
        # Temperature context (affects panel efficiency)
        "temperature": {
            "mean_c":       param_stats["T2M"]["mean"],
            "max_c":        param_stats["T2M_MAX"]["max"],
            "min_c":        param_stats["T2M_MIN"]["min"],
            "stats_T2M":    param_stats["T2M"],
            "stats_T2M_MAX":param_stats["T2M_MAX"],
            "stats_T2M_MIN":param_stats["T2M_MIN"],
        },
        # Wind context (affects cooling and structural loads)
        "wind": {
            "mean_ws10m_m_s": param_stats["WS10M"]["mean"],
            "stats":          param_stats["WS10M"],
        },
        # Humidity context (affects soiling and corrosion risk)
        "humidity": {
            "mean_rh2m_pct": param_stats["RH2M"]["mean"],
            "stats":         param_stats["RH2M"],
        },
        # Cloud cover
        "cloud_cover": {
            "mean_cloud_amt_pct": param_stats["CLOUD_AMT"]["mean"],
            "stats":              param_stats["CLOUD_AMT"],
        },
        # Precipitation (proxy for self-cleaning rain and cloud-cover correlation)
        "precipitation": {
            "mean_mm_day": param_stats["PRECTOTCORR"]["mean"],
            "stats":       param_stats["PRECTOTCORR"],
        },
        # Days with missing NASA POWER data (sentinel = -999)
        "sentinel_counts": sentinel_counts,
        # Peak sun hours = mean daily GHI numerically (1 kWh/m²/day ≡ 1 PSH)
        "peak_sun_hours": {
            "daily_average": mean_irr,
            "note": "Peak sun hours/day ≈ mean GHI (kWh/m²/day). Use to size PV arrays: array_kWp = daily_load_kWh / PSH.",
        },
        # Simple GHI-based site suitability score for quick comparison
        "pv_suitability": _ghi_suitability_score(mean_irr),
    }
    return result


# ---------------------------------------------------------------------------
# Cross-location comparison (written to each job result when ≥2 locations share
# the same POST /jobs request — identified here by checking other jobs for the
# same location keys in db=0)
# ---------------------------------------------------------------------------

def _rank_locations(analyses: list) -> dict:
    """
    Compare multiple location analyses and identify the best solar site.

    Ranking key: mean daily all-sky irradiance (ALLSKY_SFC_SW_DWN).
    Ties broken by clearness index then lower temperature de-rating.

    Args:
        analyses — list of result dicts produced by _analyze_location().

    Returns a summary dict with:
        "ranked"     — list of locations ordered best → worst with key metrics
        "best_site"  — the top-ranked location name and its primary metrics
        "comparison" — very short plain-text narrative comparing all locations
    """
    def sort_key(a):
        irr  = a["irradiance"]["mean_kwh_m2_day"] or 0.0
        kt   = a["irradiance"]["clearness_index_mean"] or 0.0
        drate = a["energy_yield"]["temp_derate_factor"] or 1.0
        return (irr, kt, drate)

    ranked = sorted(analyses, key=sort_key, reverse=True)

    ranked_summary = []
    for i, a in enumerate(ranked):
        loc  = a["location"]
        irr  = a["irradiance"]["mean_kwh_m2_day"]
        kt   = a["irradiance"]["clearness_index_mean"]
        yield_kwh = a["energy_yield"]["estimated_annual_yield_kwh_per_kwp"]
        drate = a["energy_yield"]["temp_derate_factor"]
        ranked_summary.append({
            "rank":                         i + 1,
            "name":                         loc["name"],
            "lat":                          loc["lat"],
            "lon":                          loc["lon"],
            "mean_irradiance_kwh_m2_day":   irr,
            "clearness_index_mean":         kt,
            "estimated_yield_kwh_per_kwp":  yield_kwh,
            "temp_derate_factor":           drate,
            "recommended_tilt_deg":         a["panel_orientation"]["recommended_tilt_deg"],
            "recommended_azimuth_deg":      a["panel_orientation"]["recommended_azimuth_deg"],
        })

    best = ranked_summary[0]

    # --- Plain-text comparison narrative ---
    lines = [f"Best site: {best['name']} — {best['mean_irradiance_kwh_m2_day']} kWh/m²/day "
             f"({best['estimated_yield_kwh_per_kwp']} kWh/kWp/yr estimated)."]
    for r in ranked_summary[1:]:
        diff = round((best["mean_irradiance_kwh_m2_day"] or 0) - (r["mean_irradiance_kwh_m2_day"] or 0), 4)
        lines.append(
            f"  {r['name']}: {r['mean_irradiance_kwh_m2_day']} kWh/m²/day "
            f"({r['estimated_yield_kwh_per_kwp']} kWh/kWp/yr), "
            f"{diff:+.4f} kWh/m²/day vs best."
        )

    return {
        "ranked":     ranked_summary,
        "best_site":  best,
        "comparison": " ".join(lines),
    }


# ---------------------------------------------------------------------------
# Job processor
# ---------------------------------------------------------------------------

@q.worker
def do_work(jid: str | None = None) -> None:
    """
    Main worker entry point — called by HotQueue for every queued job ID.

    Steps:
      1. Mark the job RUNNING.
      2. Read the location record from Redis db=0.
      3. Analyse the time-series data (all sentinel values excluded).
      4. Save the result dict to Redis db=3 via save_job_result().
      5. Mark the job FINISHED -- SUCCESS or FINISHED -- ERROR.
    """
    logger.info(f"[JOB START] {jid}")
    if jid is None:
        return

    # --- Step 1: Mark RUNNING ---
    try:
        start_job(jid)
    except Exception as e:
        logger.error(f"[JOB ERROR] {jid}: could not start job — {e}")
        return

    try:
        # --- Step 2: Retrieve the job metadata from db=2 ---
        job = get_job_by_id(jid)
        
        # --- Step 2a: Read the location record using the location_id from the job ---
        loc_record = _read_location_by_id(job.location_id)
        if loc_record is None:
            raise ValueError(f"Could not find location record for location_id={job.location_id}")

        logger.info(f"[JOB RUNNING] {jid} — analysing {loc_record.get('name', '(no name)')} "
                    f"| {len(loc_record.get('ALLSKY_SFC_SW_DWN', []))} days")

        # --- Step 3: Run solar site characterization analysis ---
        analysis = _analyze_location(loc_record)

        # --- Step 4: Save result to Redis db=3 ---
        save_job_result(jid, analysis)

        # --- Step 5: Mark SUCCESS ---
        update_job_status(jid, JobStatus.SUCCESS)
        mean_irr = analysis["irradiance"]["mean_kwh_m2_day"]
        yield_est = analysis["energy_yield"]["estimated_annual_yield_kwh_per_kwp"]
        logger.info(
            f"[JOB DONE] {jid} — SUCCESS | "
            f"mean irradiance={mean_irr} kWh/m²/day | "
            f"est. yield={yield_est} kWh/kWp/yr"
        )

    except Exception as e:
        logger.error(f"[JOB ERROR] {jid}: {e}", exc_info=True)
        try:
            update_job_status(jid, JobStatus.ERROR)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Worker started — listening for point jobs on HotQueue...")
    do_work()
