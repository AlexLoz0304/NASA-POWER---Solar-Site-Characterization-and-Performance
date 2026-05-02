"""
NASA POWER — Point-Redis key conventions (db=0 for locations):
  location:{lat:.1f}:{lon:.1f}                   — Redis set: stores all location keys at this grid cell (enables multiple locations per cell).
  location:{lat:.1f}:{lon:.1f}:{loc_id}          — Hash storing all parameter series and metadata for a specific location.
  location_name:{name_lower}                    — Redis set: maps friendly name (lower-cased) → location keys (supports multiple locations per name).
  location_id:{uuid}                             — Maps UUID → full location key with loc_id.

FastAPI — Solar Site Characterization API
=========================================

All interactions are point-level: a location is fetched from the NASA POWER
daily point endpoint, stored in Redis, and can then be referenced by jobs.

Routes
------
Locations:
  POST   /locations                 — Fetch a point from NASA POWER and store it as a named location.
  GET    /locations                 — List all stored locations (id, lat, lon, name).
  GET    /locations/name/{name}     — Retrieve all stored locations by friendly name (case-insensitive, supports multiple).
  GET    /locations/{loc_id}        — Retrieve a stored location by UUID.
  GET    /locations/{lat}/{lon}     — Retrieve a stored location by snapped coordinates.
  DELETE /locations/{loc_id}        — Delete a stored location and all its Redis mappings.

Jobs:
  POST   /jobs                      — Queue one job covering all supplied location UUIDs. Returns a single Job.
  GET    /jobs                      — List all queued/running/finished jobs.
  GET    /jobs/{jid}                — Get a single job by UUID.
  GET    /results/{jid}             — Get the combined solar site characterization result for a finished job.
  GET    /results/{jid}/plot        — Return the daily irradiance overlay plot (PNG) for a finished job.

Meta:
  GET    /help                      — Return a structured JSON reference of every endpoint.
  GET    /health                    — Return application health status and Redis connectivity.

"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, field_validator, model_validator
from jobs import Job, add_job, get_job_by_id, jdb, rd, get_job_result
import json
import redis
import os
import logging
import requests as http_requests
from datetime import datetime, timezone
from typing import Optional, List
import uuid
import base64
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
from datetime import timedelta as _td, timezone as _tz
_end_date  = datetime.now(_tz.utc).date() - _td(days=10)   # 10 days ago
_start_date = _end_date - _td(days=365)                     # 1 year before that

DEFAULT_END   = _end_date.strftime("%Y%m%d")    # e.g. "20260415"
DEFAULT_START = _start_date.strftime("%Y%m%d")  # e.g. "20250415"

# ---------------------------------------------------------------------------
# Redis key format for stored locations
# Locations are persisted by the worker/API when a point fetch is performed.
# The Redis key prefixes are defined immediately below and used as follows:
#   LOCATION_PREFIX      -> 'location:'       (grid cells store Redis sets of location keys; individual locations are hashes)
#   LOCATION_NAME_PREFIX -> 'location_name:'  (Redis set: maps friendly name -> multiple location keys, supports duplicate names)
#   LOCATION_ID_PREFIX   -> 'location_id:'    (maps UUID -> full location key with loc_id)
# 
# Examples:
#   location:40.5:-74.0                 — Redis set containing all location keys at grid cell (40.5, -74.0)
#   location:40.5:-74.0:a1b2c3d4        — Redis hash storing parameters and metadata for location a1b2c3d4
#   location_name:paris                 — Redis set containing keys of all locations named "Paris"
#   location_id:a1b2c3d4-e5f6-...       — Maps location UUID -> location:40.5:-74.0:a1b2c3d4
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

    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def validate_date_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                parsed = datetime.strptime(v, '%Y%m%d').date()
            except ValueError:
                raise ValueError('date must be in YYYYMMDD format (e.g. 20250101)')
            if parsed > datetime.now(timezone.utc).date():
                raise ValueError('date cannot be in the future')
        return v

    @model_validator(mode='after')
    def validate_date_range(self) -> 'LocationCreate':
        start, end = self.start_date, self.end_date
        if start and end:
            s = datetime.strptime(start, '%Y%m%d').date()
            e = datetime.strptime(end, '%Y%m%d').date()
            if s >= e:
                raise ValueError('start_date must be strictly before end_date')
        return self


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
    location_ids: List[str] = Field(..., min_length=1, description="One or more stored location UUIDs")
    # If omitted the job will use the stored location's start/end dates
    # (the values saved when the location was created). This lets callers
    # queue refresh jobs without re-specifying the original range.
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @model_validator(mode='after')
    def require_location_ids(self) -> 'JobRequest':
        if not self.location_ids:
            raise ValueError('location_ids must contain at least one id')
        return self


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


def _location_key(lat: float, lon: float, loc_id: Optional[str] = None) -> str:
    """Redis key for a stored location: location:{lat:.1f}:{lon:.1f} or location:{lat:.1f}:{lon:.1f}:{loc_id}
    
    If loc_id is provided, creates a unique key for that specific location (enables multiple locations per grid cell).
    If loc_id is omitted, returns the canonical key for that grid cell.
    """
    base = f"{LOCATION_PREFIX}{lat:.1f}:{lon:.1f}"
    return f"{base}:{loc_id}" if loc_id else base


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=16),
    retry=retry_if_exception_type((http_requests.Timeout, http_requests.ConnectionError)),
    reraise=True,
)
def _fetch_point_from_nasa(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    """Call NASA POWER point endpoint and return parameter dict.

    Retries up to 3 times with exponential backoff on transient network errors
    (Timeout, ConnectionError). HTTP 4xx/5xx errors are not retried.
    """
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
    # generate a stable unique id for this stored location
    loc_id = str(uuid.uuid4())
    key = _location_key(s_lat, s_lon, loc_id)  # Include loc_id in key to support multiple locations per grid cell
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
        # store a case-insensitive name -> location keys mapping (supports multiple locations with same name)
        rd.sadd(f"{LOCATION_NAME_PREFIX}{name.strip().lower()}", key)
    # Also store in a Redis set for this grid cell so GET /locations/{lat}/{lon} can find all locations at this cell
    grid_cell_key = _location_key(s_lat, s_lon)  # Base key without loc_id
    rd.sadd(grid_cell_key, key)
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
    """Read a Redis location hash and return a normalized record, or None if missing."""
    raw = rd.hgetall(key)
    if not raw:
        return None

    decode = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
         for k, v in raw.items()}

    rec = {
        "id":         decode.get("id"),
        "key":        key,
        "lat":        float(decode.get("lat", 0.0)),
        "lon":        float(decode.get("lon", 0.0)),
        "start_date": decode.get("start_date"),
        "end_date":   decode.get("end_date"),
        "name":       decode.get("name"),
    }

    for p in NASA_PARAMETERS:
        try:
            rec[p] = _series_to_list(json.loads(decode[p])) if p in decode else None
        except Exception:
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
                        Multiple locations can share the same name (different coords/timeframes).

    Returns the stored Location record including all 10 NASA parameter lists.
    """
    try:
        start = req.start_date or DEFAULT_START
        end   = req.end_date   or DEFAULT_END
        params = _fetch_point_from_nasa(req.lat, req.lon, start, end)
        record = _save_location_record(req.lat, req.lon, start, end, params, name=req.name)
        logger.info(f"POST /locations: stored id={record['id']} key={record['key']} range={start}→{end}")
        return Location(**record)

    except http_requests.RequestException as e:
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
    """Return all stored locations by friendly name (case-insensitive).
    
    Supports multiple locations with the same name. Each location record
    includes its id, coordinates, timeframe (start_date/end_date), and 
    all parameter data.
    """
    try:
        name_key = f"{LOCATION_NAME_PREFIX}{name.strip().lower()}"
        location_keys = rd.smembers(name_key)
        
        if not location_keys:
            raise HTTPException(status_code=404, detail=f"Location name '{name}' not found")
        
        locations = []
        for key in location_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            rec = _read_location_hash(key_str)
            if rec:
                locations.append(rec)
        
        if not locations:
            raise HTTPException(status_code=404, detail=f"Location name '{name}' resolved but no records found")
        
        # Return single location if only one, otherwise return list
        if len(locations) == 1:
            return {"location": locations[0]}
        else:
            return {"locations": locations}
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
                    "id":         rec.get("id"),
                    "key":        rec.get("key"),
                    "lat":        rec.get("lat"),
                    "lon":        rec.get("lon"),
                    "name":       rec.get("name"),
                    "start_date": rec.get("start_date"),
                    "end_date":   rec.get("end_date"),
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

    This removes the Redis hash for the location, the id->key mapping, and
    removes this location from the name->keys set if it had a friendly name.
    Other locations with the same name remain accessible.
    """
    try:
        mapped = rd.get(f"{LOCATION_ID_PREFIX}{loc_id}")
        if not mapped:
            raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' not found")
        key = mapped.decode() if isinstance(mapped, bytes) else mapped

        # Read the record (may be None if hash already missing)
        rec = _read_location_hash(key)

        # If a friendly name exists, remove this location from the name -> keys set
        if rec:
            name_val = rec.get("name")
            if isinstance(name_val, str) and name_val.strip():
                try:
                    name_map_key = f"{LOCATION_NAME_PREFIX}{name_val.strip().lower()}"
                    rd.srem(name_map_key, key)
                except Exception:
                    logger.warning(f"DELETE /locations/{loc_id}: failed to remove name map for {name_val}")
            
            # Also remove from the grid cell set
            lat = float(rec.get("lat", 0.0))
            lon = float(rec.get("lon", 0.0))
            grid_cell_key = _location_key(lat, lon)
            try:
                rd.srem(grid_cell_key, key)
            except Exception:
                logger.warning(f"DELETE /locations/{loc_id}: failed to remove from grid cell set {grid_cell_key}")

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
    """Return all stored locations for the grid cell nearest to the supplied coordinates.

    Coordinates are automatically snapped to the nearest 0.5° NASA POWER grid
    cell centre before the Redis lookup, so (40.71, -74.01) resolves to the
    same cell as (40.5, -74.0). Multiple locations may exist at the same grid cell
    with different timeframes or names.

    Args:
        lat (float): Latitude  in decimal degrees.
        lon (float): Longitude in decimal degrees.

    Returns:
        dict: 
            - If single location: wrapped in "location" key with transparency fields
            - If multiple locations: wrapped in "locations" key (list) with transparency fields
            Each location includes top-level parameter lists plus start_date/end_date/name.

    Raises:
        HTTPException 404: No location stored for this grid cell — run POST /locations first.
        HTTPException 422: Coordinates outside the accepted Americas range.
    """
    try:
        if not (-90.0 <= lat <= 90.0):
            raise HTTPException(status_code=422, detail="lat must be between -90 and 90")
        if not (-180.0 <= lon <= 180.0):
            raise HTTPException(status_code=422, detail="lon must be between -180 and 180")

        # Snap to nearest 0.5° grid cell to match stored location keys
        snapped_lat = _snap_to_grid(lat)
        snapped_lon = _snap_to_grid(lon)
        grid_cell_key = _location_key(snapped_lat, snapped_lon)

        logger.debug(f"GET /locations/{lat}/{lon}: snapped to ({snapped_lat}, {snapped_lon}) key={grid_cell_key}")

        # Get all location keys for this grid cell
        location_keys = rd.smembers(grid_cell_key)
        if not location_keys:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No solar data for grid cell ({snapped_lat}, {snapped_lon}). "
                    "Run POST /locations for this coordinate."
                ),
            )

        locations = []
        for key in location_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            record = _read_location_hash(key_str)
            if record:
                locations.append(record)
        
        if not locations:
            raise HTTPException(
                status_code=404,
                detail=f"Grid cell ({snapped_lat}, {snapped_lon}) resolved but no readable locations found"
            )
        
        # Return single location if only one, otherwise return list
        # Always include transparency fields at response level
        response_data: dict = {
            "queried_lat": lat,
            "queried_lon": lon,
            "snapped_lat": snapped_lat,
            "snapped_lon": snapped_lon,
        }
        
        if len(locations) == 1:
            response_data["id"]       = locations[0].get("id")
            response_data["name"]     = locations[0].get("name")
            response_data["location"] = locations[0]
        else:
            response_data["locations"] = locations
        
        return response_data

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
def post_job(req: JobRequest) -> Job:
    """Queue a single background job for one or more stored locations.

    Accepts a list of location UUIDs (previously created via POST /locations).
    A single job is created that will analyse all supplied locations and return
    a combined result.  If start_date / end_date are omitted, each location's
    stored dates are used.

    Args:
        req (JobRequest): { location_ids, start_date?, end_date? }

    Returns:
        Job: A single Job object in QUEUED state covering all location_ids.
             Poll GET /jobs/{jid} for status; GET /results/{jid} for results.
    """
    try:
        # Validate every location_id exists before queuing anything,
        # and collect start/end dates to derive the overall date range.
        loc_starts = []
        loc_ends   = []
        for loc_id in req.location_ids:
            mapped = rd.get(f"{LOCATION_ID_PREFIX}{loc_id}")
            if not mapped:
                raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' not found")
            key = mapped.decode() if isinstance(mapped, bytes) else mapped
            rec = _read_location_hash(key)
            if not rec:
                raise HTTPException(status_code=404, detail=f"Location id '{loc_id}' resolved to '{key}' but record missing")
            if rec.get("start_date"):
                loc_starts.append(rec["start_date"])
            if rec.get("end_date"):
                loc_ends.append(rec["end_date"])

        # Use the caller's explicit overrides if provided; otherwise derive from locations.
        resolved_start = req.start_date or (min(loc_starts) if loc_starts else None)
        resolved_end   = req.end_date   or (max(loc_ends)   if loc_ends   else None)

        job = add_job(
            location_ids=req.location_ids,
            start_date=resolved_start,
            end_date=resolved_end,
        )
        logger.info(f"POST /jobs: Queued job {job.jid} for {len(req.location_ids)} location(s): {req.location_ids}")
        return job

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


@app.get("/health")
def health_check() -> dict:
    """Return application health status and Redis connectivity.

    Always returns HTTP 200. Check the 'status' field: 'ok' or 'degraded'.
    """
    try:
        rd.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "error"
    return {
        "status": "ok" if redis_status == "ok" else "degraded",
        "redis": redis_status,
        "nasa_power_url": NASA_POWER_POINT_URL,
    }


@app.get("/help")
def get_help() -> dict:
    """Return a structured reference of every available API endpoint.

    Useful for quick discovery without consulting the README.  The response
    lists every route with its HTTP method, path, a short description, any
    path / body parameters, and an example curl command.

    Returns:
        dict: {
            "api": "NASA POWER Solar Site Characterization",
            "base_url": "http://localhost:5000",
            "endpoints": [ { method, path, description, parameters, example }, ... ]
        }
    """
    return {
        "api": "NASA POWER Solar Site Characterization",
        "base_url": "http://localhost:5000",
        "description": (
            "Fetch point-level solar and climate data from NASA POWER, store named "
            "locations in Redis, and run asynchronous solar site characterization jobs "
            "via a background worker."
        ),
        "endpoints": [
            {
                "method": "GET",
                "path": "/help",
                "description": "Return this endpoint reference.",
                "parameters": [],
                "example": "curl http://localhost:5000/help",
            },
            {
                "method": "POST",
                "path": "/locations",
                "description": (
                    "Fetch one year of daily solar/climate data for a coordinate from "
                    "NASA POWER and store it as a named location. Coordinates are "
                    "snapped to the nearest 0.5° grid cell."
                ),
                "parameters": [
                    {"name": "lat",        "in": "body", "type": "float",  "required": True,  "description": "Latitude  −90 … 90"},
                    {"name": "lon",        "in": "body", "type": "float",  "required": True,  "description": "Longitude −180 … 180"},
                    {"name": "name",       "in": "body", "type": "string", "required": False, "description": "Friendly name for the location (supports duplicates)"},
                    {"name": "start_date", "in": "body", "type": "string", "required": False, "description": "YYYYMMDD start of data window (default: 1 year ago)"},
                    {"name": "end_date",   "in": "body", "type": "string", "required": False, "description": "YYYYMMDD end of data window (default: 10 days ago)"},
                ],
                "example": (
                    'curl -X POST http://localhost:5000/locations '
                    '-H "Content-Type: application/json" '
                    '-d \'{"lat": 34.0, "lon": -118.0, "name": "LosAngeles"}\''
                ),
            },
            {
                "method": "GET",
                "path": "/locations",
                "description": "List all stored locations (id, lat, lon, name). Does not include parameter time-series.",
                "parameters": [],
                "example": "curl http://localhost:5000/locations",
            },
            {
                "method": "GET",
                "path": "/locations/name/{name}",
                "description": (
                    "Retrieve stored location(s) by friendly name (case-insensitive). "
                    "Returns {'location': ...} for a single match or {'locations': [...]} "
                    "when multiple records share the same name."
                ),
                "parameters": [
                    {"name": "name", "in": "path", "type": "string", "required": True, "description": "Friendly name (case-insensitive)"},
                ],
                "example": "curl http://localhost:5000/locations/name/LosAngeles",
            },
            {
                "method": "GET",
                "path": "/locations/{loc_id}",
                "description": "Retrieve the full location record (including all parameter time-series) by UUID.",
                "parameters": [
                    {"name": "loc_id", "in": "path", "type": "string (UUID)", "required": True, "description": "Location UUID returned by POST /locations"},
                ],
                "example": "curl http://localhost:5000/locations/9a8dee41-100e-40ba-ac77-a585a94378c5",
            },
            {
                "method": "GET",
                "path": "/locations/{lat}/{lon}",
                "description": (
                    "Retrieve stored location(s) at the grid cell nearest to the supplied "
                    "coordinates. Always includes queried_lat/lon and snapped_lat/lon "
                    "transparency fields. Returns {'location': ...} or {'locations': [...]}."
                ),
                "parameters": [
                    {"name": "lat", "in": "path", "type": "float", "required": True, "description": "Latitude  in decimal degrees"},
                    {"name": "lon", "in": "path", "type": "float", "required": True, "description": "Longitude in decimal degrees"},
                ],
                "example": "curl http://localhost:5000/locations/34.0/-118.0",
            },
            {
                "method": "DELETE",
                "path": "/locations/{loc_id}",
                "description": "Delete a stored location and all its Redis mappings (hash, name index, id index, grid-cell set entry).",
                "parameters": [
                    {"name": "loc_id", "in": "path", "type": "string (UUID)", "required": True, "description": "Location UUID to delete"},
                ],
                "example": "curl -X DELETE http://localhost:5000/locations/9a8dee41-100e-40ba-ac77-a585a94378c5",
            },
            {
                "method": "POST",
                "path": "/jobs",
                "description": (
                    "Queue solar site characterization jobs for one or more stored location UUIDs. "
                    "One job is created for all location_ids. Poll GET /results/{jid} for the combined result."
                ),
                "parameters": [
                    {"name": "location_ids", "in": "body", "type": "list[string]", "required": True,  "description": "One or more location UUIDs (non-empty)"},
                    {"name": "start_date",   "in": "body", "type": "string",       "required": False, "description": "Override YYYYMMDD start (defaults to stored location date)"},
                    {"name": "end_date",     "in": "body", "type": "string",       "required": False, "description": "Override YYYYMMDD end   (defaults to stored location date)"},
                ],
                "example": (
                    'curl -X POST http://localhost:5000/jobs '
                    '-H "Content-Type: application/json" '
                    '-d \'{"location_ids": ["9a8dee41-100e-40ba-ac77-a585a94378c5"]}\''
                ),
            },
            {
                "method": "GET",
                "path": "/jobs",
                "description": "List all jobs (QUEUED / RUNNING / FINISHED -- SUCCESS / FINISHED -- ERROR).",
                "parameters": [],
                "example": "curl http://localhost:5000/jobs",
            },
            {
                "method": "GET",
                "path": "/jobs/{jid}",
                "description": "Retrieve status and timing details for a specific job by its UUID.",
                "parameters": [
                    {"name": "jid", "in": "path", "type": "string (UUID)", "required": True, "description": "Job UUID returned by POST /jobs"},
                ],
                "example": "curl http://localhost:5000/jobs/df89de06-8729-40af-815e-cd3a74a6cc3e",
            },
            {
                "method": "GET",
                "path": "/results/{jid}",
                "description": (
                    "Retrieve the combined solar characterization result for a finished job. "
                    "Response varies by job state: queued → status message; "
                    "running → status + start_time; "
                    "error → status + timestamps; "
                    "success → combined result with location_count, locations list "
                    "(panel orientation, energy yield, irradiance breakdown, "
                    "sentinel counts, climate sections), and optional "
                    "comparison_summary when ≥2 locations were analysed."
                ),
                "parameters": [
                    {"name": "jid", "in": "path", "type": "string (UUID)", "required": True, "description": "Job UUID"},
                ],
                "example": "curl http://localhost:5000/results/df89de06-8729-40af-815e-cd3a74a6cc3e",
            },
            {
                "method": "GET",
                "path": "/results/{jid}/plot",
                "description": (
                    "Return the daily all-sky irradiance overlay plot as a PNG image for a finished job. "
                    "Single-location jobs produce a single line; multi-location jobs overlay all locations "
                    "on one chart for direct comparison. Returns 404 if the job has no plot stored."
                ),
                "parameters": [
                    {"name": "jid", "in": "path", "type": "string (UUID)", "required": True, "description": "Job UUID"},
                ],
                "example": "curl http://localhost:5000/results/df89de06-8729-40af-815e-cd3a74a6cc3e/plot --output plot.png",
            },
        ],
    }


@app.get("/results/{jid}")
def get_results(jid: str) -> dict:
    """Retrieve the combined solar site characterization result for a finished job.

    Response varies by job state:
        QUEUED              — Worker hasn't picked it up yet.
        RUNNING             — Job is in progress; check back shortly.
        FINISHED -- ERROR   — Job failed; timestamps returned.
        FINISHED -- SUCCESS — Combined result dict with:
                                • location_count  — number of locations analysed.
                                • locations       — list of per-location result dicts, each
                                                    containing panel_orientation, energy_yield,
                                                    irradiance, temperature, wind, humidity,
                                                    cloud_cover, precipitation, sentinel_counts,
                                                    peak_sun_hours, pv_suitability.
                                • comparison_summary — present when ≥2 locations were analysed;
                                                    includes ranked list, best_site, and
                                                    plain-text comparison narrative.
                              The irradiance overlay PNG is available separately via
                              GET /results/{jid}/plot.

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
                "job_status": job.status,
                "message":    "Job is queued — waiting for the worker to pick it up.",
            }

        if job.status == "RUNNING":
            return {
                "job_id":     jid,
                "job_status": job.status,
                "start_time": str(job.start_time),
                "message":    "Job is currently running — check back soon.",
            }

        if job.status.startswith("FINISHED -- ERROR"):
            return {
                "job_id":     jid,
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
            # Strip plot blobs from each location — exposed via GET /results/{jid}/plot?idx=N
            locations_clean = [
                {k: v for k, v in loc.items() if k != "irradiance_plot_base64"}
                for loc in results.get("locations", [])
            ]
            response = {
                "status":         "success",
                "job_id":         jid,
                "job_status":     job.status,
                "start_time":     str(job.start_time),
                "end_time":       str(job.end_time),
                "location_count": results.get("location_count", len(locations_clean)),
                "locations":      locations_clean,
            }
            if "comparison_summary" in results:
                response["comparison_summary"] = results["comparison_summary"]
            return response

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


@app.get("/results/{jid}/plot")
def get_result_plot(jid: str) -> Response:
    """Return the combined irradiance plot for a finished job as a PNG image.

    All locations in the job are drawn on a single chart for easy comparison.
    Open directly in a browser:
        http://<host>/results/<jid>/plot

    Args:
        jid (str): UUID of the job.

    Returns:
        PNG image (Content-Type: image/png).
    """
    try:
        try:
            job = get_job_by_id(jid)
        except Exception:
            raise HTTPException(status_code=404, detail=f"Job '{jid}' not found")

        if not job.status.startswith("FINISHED -- SUCCESS"):
            raise HTTPException(
                status_code=404,
                detail=f"Plot not available — job status is '{job.status}'. Wait for FINISHED -- SUCCESS.",
            )

        results = get_job_result(jid)
        if results is None:
            raise HTTPException(status_code=404, detail="Job result not found in Redis.")

        plot_b64 = results.get("irradiance_plot_base64")
        if not plot_b64:
            raise HTTPException(status_code=404, detail="No plot stored for this job.")

        png_bytes = base64.b64decode(plot_b64)
        return Response(content=png_bytes, media_type="image/png")

    except redis.ConnectionError as e:
        raise HTTPException(status_code=500, detail=f"Redis connection error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /results/{jid}/plot: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

