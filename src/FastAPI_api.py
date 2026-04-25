"""
NASA POWER — Point-focused FastAPI
===================================

All interactions are point-level: a location is fetched from the NASA POWER
daily point endpoint, stored in Redis, and can then be referenced by jobs.

Routes
------
Locations:
  POST   /locations                 — Fetch a point from NASA POWER and store it as a named location.
  GET    /locations                 — List all stored locations (id, lat, lon, name).
  GET    /locations/name/{name}     — Retrieve a stored location by friendly name (case-insensitive).
  GET    /locations/{loc_id}        — Retrieve a stored location by UUID.
  GET    /locations/{lat}/{lon}     — Retrieve a stored location by snapped coordinates.
  DELETE /locations/{loc_id}        — Delete a stored location and all its Redis mappings.

Jobs:
  POST   /jobs                      — Queue point jobs for one or more stored location UUIDs.
  GET    /jobs                      — List all queued/running/finished jobs.
  GET    /jobs/{jid}                — Get a single job by UUID.
  GET    /results/{jid}             — Get the full solar site characterization result of a finished job.

Redis key conventions (db=0 for locations):
  location:{lat:.1f}:{lon:.1f}      — Hash storing all parameter series and metadata.
  location_name:{name_lower}        — Maps friendly name (lower-cased) → location hash key.
  location_id:{uuid}                — Maps UUID → location hash key.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, root_validator
from jobs import Job, add_job, get_job_by_id, jdb, rd, get_job_result
import json
import redis
import os
import logging
import requests as http_requests
from datetime import datetime
from typing import Optional, List
import uuid

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI()

# ---------------------------------------------------------------------------
# Default date range — one year back from yesterday.
# Using yesterday as the end date ensures NASA POWER data is available
# (the API typically lags by ~1 day for near-real-time data).
# These are evaluated once at module import; clients may override per-request
# by passing explicit start_date / end_date in the request body.
# ---------------------------------------------------------------------------
from datetime import timedelta as _td
_end_date  = datetime.utcnow().date() - _td(days=10)   # 10 days ago
_start_date = _end_date - _td(days=365)                 # 1 year before that

DEFAULT_END   = _end_date.strftime("%Y%m%d")    # e.g. "20260415"
DEFAULT_START = _start_date.strftime("%Y%m%d")  # e.g. "20250415"

# ---------------------------------------------------------------------------
# Redis key format for stored locations: location:{lat:.1f}:{lon:.1f}
# e.g.  location:40.5:-74.0
# Locations are persisted by the worker/API when a point fetch is performed.
# The Redis key prefixes are defined immediately below and used as follows:
#   LOCATION_PREFIX      -> 'location:'       (hash with per-parameter series)
#   LOCATION_NAME_PREFIX -> 'location_name:'  (maps friendly name -> location key)
#   LOCATION_ID_PREFIX   -> 'location_id:'    (maps UUID -> location key)
# ---------------------------------------------------------------------------
LOCATION_PREFIX = "location:"
LOCATION_NAME_PREFIX = "location_name:"
LOCATION_ID_PREFIX = "location_id:"
# ---------------------------------------------------------------------------
# NASA POWER API constants
# ---------------------------------------------------------------------------
NASA_POWER_POINT_URL    = "https://power.larc.nasa.gov/api/temporal/daily/point"
NASA_COMMUNITY          = "RE"
# The 10 NASA POWER parameters fetched for every point location.
# All values are daily averages unless otherwise noted.
NASA_PARAMETERS = [
    "ALLSKY_SFC_SW_DWN",   # All-sky surface shortwave downward irradiance (kW·h/m²/day) — total solar radiation reaching the surface under actual cloud conditions
    "CLRSKY_SFC_SW_DWN",   # Clear-sky surface shortwave downward irradiance (kW·h/m²/day) — solar radiation if there were no clouds (theoretical maximum)
    "ALLSKY_KT",           # All-sky insolation clearness index (dimensionless 0–1) — ratio of actual to clear-sky irradiance; indicates cloud cover impact
    "T2M",                 # Temperature at 2 m above ground (°C) — daily mean air temperature
    "T2M_MAX",             # Maximum temperature at 2 m (°C) — daily high
    "T2M_MIN",             # Minimum temperature at 2 m (°C) — daily low
    "WS10M",               # Wind speed at 10 m above ground (m/s) — daily mean; relevant for wind cooling of solar panels
    "RH2M",                # Relative humidity at 2 m (%) — daily mean; affects panel efficiency and soiling
    "PRECTOTCORR",         # Precipitation (mm/day) — bias-corrected total precipitation; proxy for cloud cover and panel cleaning
    "CLOUD_AMT",           # Cloud amount (%) — daily mean fractional cloud cover; directly impacts ALLSKY_SFC_SW_DWN
]

# ---------------------------------------------------------------------------
# Models (location + job request)
# ---------------------------------------------------------------------------


class LocationCreate(BaseModel):
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    # start_date / end_date default to the module-level DEFAULT_START / DEFAULT_END
    # (one year back from yesterday at server startup). Clients may supply explicit
    # YYYYMMDD strings to override the range.
    start_date: Optional[str] = Field(DEFAULT_START)
    end_date: Optional[str] = Field(DEFAULT_END)
    name: Optional[str] = None


class Location(BaseModel):
    id: str
    lat: float
    lon: float
    start_date: Optional[str]
    end_date: Optional[str]
    name: Optional[str] = None
    # Each field below holds a time-ordered list of daily values for the
    # corresponding NASA POWER parameter, one entry per calendar day in the
    # requested start_date → end_date range. Missing days use null (None).
    ALLSKY_SFC_SW_DWN: Optional[List[float]] = None   # All-sky surface solar irradiance (kW·h/m²/day)
    CLRSKY_SFC_SW_DWN: Optional[List[float]] = None   # Clear-sky surface solar irradiance (kW·h/m²/day)
    ALLSKY_KT: Optional[List[float]] = None           # Clearness index (0–1); ratio of actual to clear-sky irradiance
    T2M: Optional[List[float]] = None                 # Mean air temperature at 2 m (°C)
    T2M_MAX: Optional[List[float]] = None             # Daily maximum temperature at 2 m (°C)
    T2M_MIN: Optional[List[float]] = None             # Daily minimum temperature at 2 m (°C)
    WS10M: Optional[List[float]] = None               # Mean wind speed at 10 m (m/s)
    RH2M: Optional[List[float]] = None                # Mean relative humidity at 2 m (%)
    PRECTOTCORR: Optional[List[float]] = None         # Bias-corrected total precipitation (mm/day)
    CLOUD_AMT: Optional[List[float]] = None           # Mean cloud cover fraction (%)


class JobRequest(BaseModel):
    location_ids: List[str] = Field(..., min_items=1, description="One or more stored location UUIDs")
    # If omitted the job will use the stored location's start/end dates
    # (the values saved when the location was created). This lets callers
    # queue refresh jobs without re-specifying the original range.
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @root_validator(skip_on_failure=True)
    def require_location_ids(cls, values):
        locs = values.get('location_ids')
        if not locs or len(locs) == 0:
            raise ValueError('location_ids must contain at least one id')
        return values


# ---------------------------------------------------------------------------
# Grid-cell helpers
# ---------------------------------------------------------------------------

def _snap_to_grid(value: float, resolution: float = 0.5) -> float:
    """Snap a lat or lon coordinate to the nearest NASA POWER 0.5° grid cell centre.

    NASA POWER data lives on a regular 0.5° × 0.5° global grid. Snapping
    ensures that Redis key lookups always hit the canonical key regardless
    of how precise the caller's input is.

    Examples:  40.71 -> 40.5   |   -74.01 -> -74.0   |   51.52 -> 51.5
    """
    return round(round(value / resolution) * resolution, 1)


def _location_key(lat: float, lon: float) -> str:
    """Redis key for a stored location: location:{lat:.1f}:{lon:.1f}"""
    return f"{LOCATION_PREFIX}{lat:.1f}:{lon:.1f}"


def _fetch_point_from_nasa(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    """Call NASA POWER point endpoint and return parameter dict."""
    params_str = ",".join(NASA_PARAMETERS)
    resp = http_requests.get(
        NASA_POWER_POINT_URL,
        params={
            "parameters":    params_str,
            "community":     NASA_COMMUNITY,
            "format":        "JSON",
            "start":         start_date,
            "end":           end_date,
            "latitude":      lat,
            "longitude":     lon,
            "time-standard": "UTC",
        },
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("properties", {}).get("parameter", {})


def _series_to_list(series) -> list:
    """Normalize a NASA parameter series into a list of values ordered by date.

    The NASA point endpoint returns a mapping { YYYYMMDD: value }.
    Convert that into a list ordered by ascending date. If `series` is
    already a list, return it unchanged. If it's falsy, return an empty list.
    NASA POWER missing-data sentinels (-999 / -999.0) are kept as-is.
    """
    if not series:
        return []
    if isinstance(series, list):
        return [float(v) for v in series]
    # If it's a mapping of date->value, sort keys and return values
    try:
        items = sorted(series.items(), key=lambda x: x[0])
        return [float(v) for _, v in items]
    except Exception:
        return list(series)


def _save_location_record(lat: float, lon: float, start_date: str, end_date: str, parameters: dict, name: Optional[str] = None) -> dict:
    """Persist a location record in Redis db=0 and optionally map a name.

    Returns the stored record dict.
    """
    s_lat = _snap_to_grid(lat)
    s_lon = _snap_to_grid(lon)
    key = _location_key(s_lat, s_lon)
    # generate a stable unique id for this stored location
    loc_id = str(uuid.uuid4())
    mapping = {
        "id": loc_id,
        "lat": str(s_lat),
        "lon": str(s_lon),
        "start_date": start_date,
        "end_date": end_date,
    }
    # Store each parameter as a JSON-encoded list (ordered by date)
    for p in NASA_PARAMETERS:
        series = parameters.get(p, {})
        lst = _series_to_list(series)
        mapping[p] = json.dumps(lst)
    if name:
        mapping["name"] = name.strip()
        # store a case-insensitive name -> key map
        rd.set(f"{LOCATION_NAME_PREFIX}{name.strip().lower()}", key)
    # persist the mapping and an id->key map for later lookup
    rd.hset(key, mapping=mapping)
    rd.set(f"{LOCATION_ID_PREFIX}{loc_id}", key)
    rec = {
        "id": loc_id,
        "key": key,
        "lat": s_lat,
        "lon": s_lon,
        "start_date": start_date,
        "end_date": end_date,
        "name": name if name else None,
    }
    # Add each parameter as its own top-level list field
    for p in NASA_PARAMETERS:
        rec[p] = _series_to_list(parameters.get(p, {}))
    return rec


def _read_location_hash(key: str) -> Optional[dict]:
    """
    Read a Redis location/hash key and return a normalized record.

    This single helper replaces the previous two helpers and always
    returns a dict with these top-level fields when the key exists:
      - id (str|None): UUID assigned when the location was created (may be None)
      - key (str): the Redis key passed in
      - lat, lon (float)
      - start_date, end_date (str|None)
      - name (str|None)
      - one top-level list field for each NASA parameter (or None)

    Returns None if the key does not exist.
    """
    raw = rd.hgetall(key)
    if not raw:
        return None
    decoded = { (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in raw.items() }
    rec = {
        "id": decoded.get("id"),
        "key": key,
        "lat": float(decoded.get("lat", 0.0)),
        "lon": float(decoded.get("lon", 0.0)),
        "start_date": decoded.get("start_date"),
        "end_date": decoded.get("end_date"),
        "name": decoded.get("name"),
    }
    for p in NASA_PARAMETERS:
        if p in decoded:
            try:
                val = json.loads(decoded[p])
                if isinstance(val, list):
                    rec[p] = val
                elif isinstance(val, dict):
                    rec[p] = _series_to_list(val)
                else:
                    rec[p] = [val]
            except Exception:
                rec[p] = [decoded[p]]
        else:
            rec[p] = None
    return rec
@app.post("/locations", status_code=201)
def post_location(req: LocationCreate) -> Location:
    """Fetch a single point from NASA POWER and persist it as a known location.

    Body fields:
      lat, lon        — Coordinates (snapped to nearest 0.5° grid cell on save).
      start_date      — YYYYMMDD start of the data range (default: one year ago).
      end_date        — YYYYMMDD end of the data range (default: yesterday).
      name            — Optional friendly name; enables GET /locations/name/{name}.

    Returns the stored Location record including all 10 NASA parameter lists.
    """
    try:
        start = req.start_date or DEFAULT_START
        end   = req.end_date   or DEFAULT_END
        params = _fetch_point_from_nasa(req.lat, req.lon, start, end)
        record = _save_location_record(req.lat, req.lon, start, end, params, name=req.name)
        logger.info(f"POST /locations: stored id={record['id']} key={record['key']} range={start}→{end}")
        return Location(**record)

    except http_requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"NASA POWER API error: {e}")
    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except Exception as e:
        logger.error(f"POST /locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Location endpoints (point-focused): name / id / coords retrieval
# ---------------------------------------------------------------------------

@app.get("/locations/name/{name}")
def get_location_by_name(name: str) -> dict:
    """Return a stored location by friendly name (case-insensitive)."""
    try:
        mapped = rd.get(f"{LOCATION_NAME_PREFIX}{name.strip().lower()}")
        if not mapped:
            raise HTTPException(status_code=404, detail=f"Location name '{name}' not found")
        key = mapped.decode() if isinstance(mapped, bytes) else mapped
        rec = _read_location_hash(key)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Location name '{name}' resolved to '{key}' but record missing")
        return {"location": rec}
    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /locations/name/{name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/locations/{loc_id}")
def get_location_by_id(loc_id: str) -> dict:
    """Return a stored location by UUID id."""
    try:
        mapped = rd.get(f"{LOCATION_ID_PREFIX}{loc_id}")
        if not mapped:
            raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' not found")
        key = mapped.decode() if isinstance(mapped, bytes) else mapped
        rec = _read_location_hash(key)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' resolved to '{key}' but record missing")
        return {"location": rec}
    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /locations/{loc_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/locations")
def list_locations() -> dict:
    """List all stored locations with their UUIDs and basic metadata.

    Scans Redis for `location_id:{uuid}` keys and returns a lightweight
    summary for each stored location: id, key, lat, lon, name.
    Use GET /locations/{loc_id} to retrieve the full parameter data.
    """
    try:
        results = []
        for k in rd.scan_iter(match=f"{LOCATION_ID_PREFIX}*"):
            k_str = k.decode() if isinstance(k, bytes) else k
            # key is like 'location_id:{uuid}' — extract the id suffix
            loc_id = k_str[len(LOCATION_ID_PREFIX):]
            mapped = rd.get(k_str)
            if not mapped:
                # id mapping exists but target missing; include id only
                results.append({"id": loc_id, "key": None, "lat": None, "lon": None, "name": None})
                continue
            loc_key = mapped.decode() if isinstance(mapped, bytes) else mapped
            rec = _read_location_hash(loc_key)
            if rec:
                results.append({
                    "id": rec.get("id"),
                    "key": rec.get("key"),
                    "lat": rec.get("lat"),
                    "lon": rec.get("lon"),
                    "name": rec.get("name"),
                })
            else:
                results.append({"id": loc_id, "key": loc_key, "lat": None, "lon": None, "name": None})

        return {"locations": results}

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except Exception as e:
        logger.error(f"GET /locations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/locations/{loc_id}")
def delete_location_by_id(loc_id: str) -> dict:
    """Delete a stored location by UUID id.

    This removes the Redis hash for the location, the id->key mapping and
    the case-insensitive name->key mapping if the location had a friendly
    name set when created.
    """
    try:
        mapped = rd.get(f"{LOCATION_ID_PREFIX}{loc_id}")
        if not mapped:
            raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' not found")
        key = mapped.decode() if isinstance(mapped, bytes) else mapped

        # Read the record (may be None if hash already missing)
        rec = _read_location_hash(key)

        # If a friendly name exists, remove the name -> key mapping
        if rec:
            name_val = rec.get("name")
            if isinstance(name_val, str) and name_val.strip():
                try:
                    name_map_key = f"{LOCATION_NAME_PREFIX}{name_val.strip().lower()}"
                    rd.delete(name_map_key)
                except Exception:
                    logger.warning(f"DELETE /locations/{loc_id}: failed to remove name map for {name_val}")

        # Delete the location hash and the id -> key mapping
        try:
            rd.delete(key)
        except Exception:
            logger.warning(f"DELETE /locations/{loc_id}: failed to delete location hash {key}")
        rd.delete(f"{LOCATION_ID_PREFIX}{loc_id}")

        logger.info(f"DELETE /locations: removed id={loc_id} key={key}")
        return {"deleted": True, "id": loc_id, "key": key}

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DELETE /locations/{loc_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/locations/{lat}/{lon}")
def get_location_data(lat: float, lon: float) -> dict:
    """Return the full stored record for the grid cell nearest to the supplied coordinates.

    Coordinates are automatically snapped to the nearest 0.5° NASA POWER grid
    cell centre before the Redis lookup, so (40.71, -74.01) resolves to the
    same cell as (40.5, -74.0).

    Args:
        lat (float): Latitude  in decimal degrees.
        lon (float): Longitude in decimal degrees.

    Returns:
        dict: Full location record with top-level parameter lists plus
              queried_lat, queried_lon, snapped_lat, snapped_lon transparency fields.

    Raises:
        HTTPException 404: No location stored for this grid cell — run POST /locations first.
        HTTPException 422: Coordinates outside the accepted Americas range.
    """
    try:
        if not (15.0 <= lat <= 75.0):
            raise HTTPException(status_code=422, detail="lat must be between 15.0 and 75.0 (Americas coverage)")
        if not (-170.0 <= lon <= -50.0):
            raise HTTPException(status_code=422, detail="lon must be between -170.0 and -50.0 (Americas coverage)")

        # Snap to nearest 0.5° grid cell to match stored location keys
        snapped_lat = _snap_to_grid(lat)
        snapped_lon = _snap_to_grid(lon)
        key = _location_key(snapped_lat, snapped_lon)

        logger.debug(f"GET /locations/{lat}/{lon}: snapped to ({snapped_lat}, {snapped_lon}) key={key}")

        val = rd.hgetall(key)
        if not val:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No solar data for grid cell ({snapped_lat}, {snapped_lon}). "
                    "Run POST /data for a global fetch, or POST /jobs for this "
                    "specific coordinate."
                ),
            )

        record = _read_location_hash(key)
        assert record is not None  # key exists — checked above via hgetall
        # Add transparency fields so the caller knows what snapping occurred
        record["queried_lat"] = lat
        record["queried_lon"] = lon
        record["snapped_lat"] = snapped_lat
        record["snapped_lon"] = snapped_lon
        return record

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /locations/{lat}/{lon}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# /jobs  — point-level job submission and tracking
# ---------------------------------------------------------------------------

@app.post("/jobs", status_code=200)
def post_job(req: JobRequest) -> List[Job]:
    """Queue background point jobs for one or more stored locations.

    Accepts a list of location UUIDs (previously created via POST /locations).
    For each location, a point job is enqueued for the worker to process.
    If start_date / end_date are omitted, the dates stored with the location
    are used as-is.

    Args:
        req (JobRequest): { location_ids, start_date?, end_date? }

    Returns:
        List[Job]: One Job object per location_id, each in QUEUED state.
                   Poll GET /jobs/{jid} for status updates.
                   Note: the current worker is a stub — it marks jobs SUCCESS
                   after 5 s but does not persist result data to Redis.
    """
    try:
        jobs = []
        for loc_id in req.location_ids:
            mapped = rd.get(f"{LOCATION_ID_PREFIX}{loc_id}")
            if not mapped:
                raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' not found")
            key = mapped.decode() if isinstance(mapped, bytes) else mapped
            rec = _read_location_hash(key)
            if not rec:
                raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' resolved to '{key}' but record missing")
            lat = rec["lat"]
            lon = rec["lon"]
            # Determine dates for the job: prefer explicit request dates;
            # otherwise fall back to the stored location start/end (or
            # defaults if the stored record lacks them).
            job_start = req.start_date or rec.get("start_date") or DEFAULT_START
            job_end = req.end_date or rec.get("end_date") or DEFAULT_END

            job = add_job(
                job_type="point",
                lat=lat,
                lon=lon,
                start_date=job_start,
                end_date=job_end,
            )
            logger.info(f"POST /jobs: Queued point job {job.jid} for id={loc_id} ({lat}, {lon})")
            jobs.append(job)
        return jobs

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"POST /jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs")
def get_jobs() -> list:
    """
    List all background jobs stored in the job database (Redis db=2).

    Returns all jobs regardless of state: QUEUED, RUNNING,
    FINISHED -- SUCCESS, FINISHED -- ERROR.

    Returns:
        list: All Job objects in the job database.
    """
    try:
        jobs = []
        for jid in jdb.scan_iter():
            jid_str = jid.decode() if isinstance(jid, bytes) else jid
            try:
                jobs.append(get_job_by_id(jid_str))
            except Exception as e:
                logger.warning(f"GET /jobs: Failed to retrieve job {jid_str}: {e}")

        logger.info(f"GET /jobs: Returned {len(jobs)} job(s)")
        return jobs

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except Exception as e:
        logger.error(f"GET /jobs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{jid}")
def get_job(jid: str) -> Job:
    """
    Retrieve a specific background job by its UUID.

    Args:
        jid (str): UUID of the job to look up.

    Returns:
        Job: Job object with current status, timestamps, type, and input params.
    """
    try:
        return get_job_by_id(jid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except Exception as e:
        logger.error(f"GET /jobs/{jid}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/results/{jid}")
def get_results(jid: str) -> dict:
    """Retrieve the solar site characterization result of a finished job.

    Response varies by job state:
        QUEUED              — Worker hasn't picked it up yet.
        RUNNING             — Job is in progress; check back shortly.
        FINISHED -- ERROR   — Job failed; timestamps returned.
        FINISHED -- SUCCESS — Full solar characterization result dict, including
                              panel orientation recommendation, energy yield
                              estimate, per-parameter statistics, monthly
                              irradiance breakdown, and variability index.

    Args:
        jid (str): UUID of the job.

    Returns:
        dict: Status/result payload.

    Raises:
        HTTPException 404: No job with this UUID exists.
    """
    try:
        try:
            job = get_job_by_id(jid)
        except Exception:
            raise HTTPException(status_code=404, detail=f"Job '{jid}' not found")

        if job.status == "QUEUED":
            return {
                "job_id":     jid,
                "job_type":   job.job_type,
                "job_status": job.status,
                "message":    "Job is queued — waiting for the worker to pick it up.",
            }

        if job.status == "RUNNING":
            return {
                "job_id":     jid,
                "job_type":   job.job_type,
                "job_status": job.status,
                "start_time": str(job.start_time),
                "message":    "Job is currently running — check back soon.",
            }

        if job.status.startswith("FINISHED -- ERROR"):
            return {
                "job_id":     jid,
                "job_type":   job.job_type,
                "job_status": job.status,
                "start_time": str(job.start_time),
                "end_time":   str(job.end_time),
                "message":    "Job encountered an error during processing.",
            }

        if job.status.startswith("FINISHED -- SUCCESS"):
            # Retrieve any result data saved to Redis db=3 by the worker.
            # The current worker stub does not write results, so this will
            # typically return None and the response below will say so.
            results = get_job_result(jid)
            if results is None:
                return {
                    "job_id":     jid,
                    "job_status": job.status,
                    "message":    "Job completed but results could not be retrieved.",
                }
            return {
                "status":     "success",
                "job_id":     jid,
                "job_type":   job.job_type,
                "job_status": job.status,
                "start_time": str(job.start_time),
                "end_time":   str(job.end_time),
                "results":    results,
            }

        return {
            "job_id":     jid,
            "job_status": job.status,
            "message":    f"Unrecognised job status: {job.status}",
        }

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /results/{jid}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

