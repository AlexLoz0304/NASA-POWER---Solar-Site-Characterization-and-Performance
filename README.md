# NASA POWER Solar Site Characterization API

This folder contains a FastAPI application that serves solar resource and climate data from the [NASA POWER](https://power.larc.nasa.gov/) (Prediction of Worldwide Energy Resources) API with a distributed job queue system. The app fetches daily solar and meteorological time-series data for any geographic coordinate on Earth, parses it into Pydantic data models, and exposes the data through a RESTful API backed by Redis. Additionally, it provides a job queue system that allows users to submit solar site characterization analyses for asynchronous processing by a background worker. This application is useful for engineers, researchers, and developers who need programmatic access to solar irradiance data, site suitability metrics, and the ability to compare multiple candidate locations for photovoltaic (PV) system installation.

## What's in this folder

- `src/FastAPI_api.py` — main FastAPI application. Fetches point-level solar and climate data from the NASA POWER daily point endpoint, parses it into `Location` Pydantic models, and exposes HTTP endpoints to create, retrieve, and delete location records in Redis. Also provides endpoints for job queue management and result retrieval.

- `src/worker.py` — background worker process that listens to the Redis job queue, processes solar site characterization jobs asynchronously, computes a full suite of statistics from the stored time-series data (sentinel filtering, monthly irradiance breakdowns, energy yield estimates, panel orientation recommendations, variability index, temperature de-rating), and saves results to Redis db=3 for retrieval via `GET /results/{jid}`.

- `src/jobs.py` — core job queue logic. Defines the `Job` Pydantic model, `JobStatus` enum, and functions for creating, retrieving, and updating jobs in Redis across multiple databases (db=0 locations, db=1 queue, db=2 job metadata, db=3 results).

- `test/test_jobs.py` — unit tests for job queue functions. Uses `unittest.mock` to mock Redis dependencies and test all job operations in isolation.

- `test/test_FastAPI_api.py` — integration tests for API endpoints. Uses the `requests` library to test all HTTP routes against the running FastAPI application.

- `test/test_worker.py` — integration tests for the background worker. Tests job processing through the complete system and verifies the worker correctly computes and stores solar characterization results.

- `docker-compose.yml` — orchestrates three services: the FastAPI application, a background worker, and a Redis database.

- `Dockerfile` — single container image used for both the FastAPI server and the worker process, with all Python dependencies installed.

- `README.md` — this file, containing project documentation and usage instructions.

## Data source

The application fetches solar and meteorological data from the NASA POWER daily point endpoint:

```
https://power.larc.nasa.gov/api/temporal/daily/point
```

Data is fetched at runtime using the `requests` library and is never committed to this repository. Each location record stores 366 daily values for 10 parameters covering a one-year window ending 10 days before the current date. No API key or authentication is required.

*Do not include the data itself in this repository* — the app fetches it live from NASA POWER at request time.

### Parameters fetched per location

| Parameter | Description | Units |
|---|---|---|
| `ALLSKY_SFC_SW_DWN` | All-sky surface shortwave downward irradiance (GHI) | kWh/m²/day |
| `CLRSKY_SFC_SW_DWN` | Clear-sky surface shortwave downward irradiance | kWh/m²/day |
| `ALLSKY_KT` | Clearness index — ratio of actual to clear-sky irradiance | dimensionless (0–1) |
| `T2M` | Mean air temperature at 2 m | °C |
| `T2M_MAX` | Daily maximum temperature at 2 m | °C |
| `T2M_MIN` | Daily minimum temperature at 2 m | °C |
| `WS10M` | Mean wind speed at 10 m | m/s |
| `RH2M` | Mean relative humidity at 2 m | % |
| `PRECTOTCORR` | Bias-corrected total precipitation | mm/day |
| `CLOUD_AMT` | Mean cloud cover fraction | % |

Missing or invalid days in the NASA POWER dataset are encoded as `-999` (sentinel values). The worker filters these out before computing any statistic.

## Instructions to run with Docker Compose

1. **Clone the repository and navigate to the project directory:**
   ```bash
   git clone https://github.com/AlexLoz0304/NASA-POWER---Solar-Site-Characterization-and-Performance.git
   cd NASA-POWER---Solar-Site-Characterization-and-Performance
   ```

2. **Build and start the containers:**
   ```bash
   docker compose up --build
   ```

3. **Verify the containers are running:**
   ```bash
   docker compose ps
   ```

The FastAPI server will start on `http://localhost:5000` and Redis will listen on `localhost:6379`. The background worker will continuously listen to the job queue and process characterization jobs as they are submitted.

To stop the containers:

```bash
docker compose down
```

## Running Tests

The project includes unit and integration tests to verify functionality at multiple levels.

### Run all tests

With Docker containers running, execute all test suites:

```bash
# Unit tests (job queue functions)
uv run python test/test_jobs.py

# API integration tests (HTTP endpoints)
uv run python test/test_FastAPI_api.py

# Worker integration tests (job processing)
uv run python test/test_worker.py
```

Or run all tests together:

```bash
uv run python test/test_jobs.py && uv run python test/test_FastAPI_api.py && uv run python test/test_worker.py
```

### Test summary

- **test_jobs.py** — Unit tests for job queue operations with mocked Redis dependencies.
- **test_FastAPI_api.py** — Integration tests for all 9 API endpoints using HTTP requests.
- **test_worker.py** — Integration tests verifying the worker computes and stores solar characterization results end-to-end.

All tests should pass with the Docker containers running.

> **⚠️ Note:** The API and worker integration tests (`test_FastAPI_api.py`, `test_worker.py`) write real data to the live Redis database. They are designed to run against a **fresh (empty) Redis instance**. If Redis already contains data from a previous test run, stale entries can cause failures (e.g. duplicate location names returning multiple results). To reset Redis before running the tests, run:
> ```bash
> docker compose down -v && docker compose up -d
> ```
> This removes the Redis volume and starts with a clean database.

## HTTP API Routes

The application exposes the following endpoints:

| Route | Method | Description |
|---|---|---|
| `/help` | GET | Return a structured JSON reference of every endpoint |
| `/locations` | POST | Fetch a point from NASA POWER and store it as a named location |
| `/locations` | GET | List all stored locations (id, lat, lon, name) |
| `/locations/name/{name}` | GET | Retrieve a stored location by friendly name (case-insensitive) |
| `/locations/{loc_id}` | GET | Retrieve a stored location by UUID |
| `/locations/{lat}/{lon}` | GET | Retrieve a stored location by snapped coordinates |
| `/locations/{loc_id}` | DELETE | Delete a stored location and all its Redis mappings |
| `/jobs` | POST | Queue solar characterization jobs for one or more location UUIDs |
| `/jobs` | GET | List all queued / running / finished jobs |
| `/jobs/{jid}` | GET | Retrieve details for a specific job by its UUID |
| `/results/{jid}` | GET | Retrieve the full solar characterization result for a completed job |

## Example API Queries and Expected Outputs

### Meta

#### GET /help

Returns a structured JSON reference of every available endpoint — useful for quick discovery without consulting this README.

```bash
curl http://localhost:5000/help
```

Expected output (truncated):

```json
{
  "api": "NASA POWER Solar Site Characterization",
  "base_url": "http://localhost:5000",
  "description": "Fetch point-level solar and climate data from NASA POWER ...",
  "endpoints": [
    {
      "method": "GET",
      "path": "/help",
      "description": "Return this endpoint reference.",
      "parameters": [],
      "example": "curl http://localhost:5000/help"
    },
    {
      "method": "POST",
      "path": "/locations",
      "description": "Fetch one year of daily solar/climate data ...",
      "parameters": [
        {"name": "lat",  "in": "body", "type": "float",  "required": true,  "description": "Latitude −90 … 90"},
        {"name": "lon",  "in": "body", "type": "float",  "required": true,  "description": "Longitude −180 … 180"},
        {"name": "name", "in": "body", "type": "string", "required": false, "description": "Friendly name"}
      ],
      "example": "curl -X POST http://localhost:5000/locations -d '{\"lat\": 34.0, ...}'"
    }
  ]
}
```

### Location Endpoints

#### POST /locations

Fetches one year of daily solar data for the given coordinates from NASA POWER and stores it as a named location. Coordinates are snapped to the nearest 0.5° NASA POWER grid cell.

```bash
curl -X POST http://localhost:5000/locations \
  -H "Content-Type: application/json" \
  -d '{"lat": 34.0, "lon": -118.0, "name": "LosAngeles"}'
```

Expected output (truncated):

```json
{
  "id": "b0e5c1e7-2f30-47b9-8591-07c64b49806e",
  "lat": 34.0,
  "lon": -118.0,
  "name": "LosAngeles",
  "start_date": "20250415",
  "end_date": "20260415",
  "ALLSKY_SFC_SW_DWN": [6.1654, 5.8812, "..."],
  "T2M": [13.45, 14.02, "..."]
}
```

#### GET /locations

Returns a list of all stored locations with their id, lat, lon, and name.

```bash
curl http://localhost:5000/locations
```

Expected output:

```json
{
  "locations": [
    {"id": "7fe1e7cd-...", "lat": 40.5, "lon": -74.0, "name": "NewYork"},
    {"id": "9a8dee41-...", "lat": 26.0, "lon": -80.0, "name": "Miami"},
    {"id": "b0e5c1e7-...", "lat": 34.0, "lon": -118.0, "name": "LosAngeles"}
  ]
}
```

#### GET /locations/name/{name}

Retrieves the full location record (including all parameter time-series) by friendly name. Case-insensitive.

```bash
curl http://localhost:5000/locations/name/Miami
```

Expected output (truncated):

```json
{
  "location": {
    "id": "9a8dee41-100e-40ba-ac77-a585a94378c5",
    "lat": 26.0,
    "lon": -80.0,
    "name": "Miami",
    "start_date": "20250415",
    "end_date": "20260415",
    "ALLSKY_SFC_SW_DWN": [7.4371, 6.9203, "..."],
    "T2M": [24.36, 25.01, "..."]
  }
}
```

#### GET /locations/{loc_id}

Retrieves the full location record by UUID.

```bash
curl http://localhost:5000/locations/9a8dee41-100e-40ba-ac77-a585a94378c5
```

Expected output: same shape as `GET /locations/name/{name}`.

#### GET /locations/{lat}/{lon}

Retrieves a location by its coordinates. Snaps the input to the nearest 0.5° grid cell before lookup.

```bash
curl http://localhost:5000/locations/26.0/-80.0
```

Expected output (truncated):

```json
{
  "queried_lat": 26.0,
  "queried_lon": -80.0,
  "snapped_lat": 26.0,
  "snapped_lon": -80.0,
  "id": "9a8dee41-...",
  "name": "Miami"
}
```

#### DELETE /locations/{loc_id}

Deletes a stored location and all its associated Redis keys (hash, name mapping, id mapping).

```bash
curl -X DELETE http://localhost:5000/locations/b0e5c1e7-2f30-47b9-8591-07c64b49806e
```

Expected output:

```json
{"deleted": true, "id": "b0e5c1e7-...", "key": "location:34.0:-118.0"}
```

### Job Queue Endpoints

The job queue system allows you to submit asynchronous solar site characterization analyses. Jobs are queued in Redis and processed by the background worker service, which reads the stored time-series data and computes a full set of solar engineering metrics.

#### POST /jobs

Submit characterization jobs for one or more stored location UUIDs. One job is created per location.

```bash
curl -X POST http://localhost:5000/jobs \
  -H "Content-Type: application/json" \
  -d '{"location_ids": ["7fe1e7cd-...", "9a8dee41-...", "b0e5c1e7-..."]}'
```

Expected output:

```json
[
  {"jid": "df89de06-...", "status": "QUEUED", "job_type": "point", "lat": 40.5, "lon": -74.0, "start_date": "20250415", "end_date": "20260415", "start_time": null, "end_time": null},
  {"jid": "b50ba419-...", "status": "QUEUED", "job_type": "point", "lat": 34.0, "lon": -118.0, "start_date": "20250415", "end_date": "20260415", "start_time": null, "end_time": null},
  {"jid": "e5b8770b-...", "status": "QUEUED", "job_type": "point", "lat": 26.0, "lon": -80.0, "start_date": "20250415", "end_date": "20260415", "start_time": null, "end_time": null}
]
```

#### GET /jobs

Retrieve a list of all submitted jobs and their current statuses.

```bash
curl http://localhost:5000/jobs
```

Expected output (truncated):

```json
[
  {
    "jid": "df89de06-...",
    "status": "FINISHED -- SUCCESS",
    "job_type": "point",
    "lat": 40.5,
    "lon": -74.0,
    "start_date": "20250415",
    "end_date": "20260415",
    "start_time": "2026-04-25T07:12:04.123456",
    "end_time": "2026-04-25T07:12:04.789012"
  }
]
```

#### GET /jobs/{jid}

Retrieve status and timing details for a specific job.

```bash
curl http://localhost:5000/jobs/df89de06-8729-40af-815e-cd3a74a6cc3e
```

Expected output:

```json
{
  "jid": "df89de06-...",
  "status": "FINISHED -- SUCCESS",
  "job_type": "point",
  "lat": 40.5,
  "lon": -74.0,
  "start_time": "2026-04-25T07:12:04.123456",
  "end_time": "2026-04-25T07:12:04.789012"
}
```

#### GET /results/{jid}

Retrieve the full solar site characterization result for a completed job.

**For a job still queued or running:**

```bash
curl http://localhost:5000/results/df89de06-8729-40af-815e-cd3a74a6cc3e
```

Expected output:

```json
{"job_id": "df89de06-...", "job_type": "point", "job_status": "RUNNING", "message": "Job is currently running — check back soon."}
```

**For a completed job:**

```json
{
  "status": "success",
  "job_id": "df89de06-...",
  "job_status": "FINISHED -- SUCCESS",
  "start_time": "2026-04-25T07:12:04.123456",
  "end_time": "2026-04-25T07:12:04.789012",
  "results": {
    "location": {
      "id": "7fe1e7cd-...",
      "lat": 40.5,
      "lon": -74.0,
      "name": "NewYork",
      "start_date": "20250415",
      "end_date": "20260415",
      "n_days": 261
    },
    "panel_orientation": {
      "recommended_tilt_deg": 40.5,
      "recommended_azimuth_deg": 180.0,
      "facing": "south",
      "note": "Fixed-tilt rule-of-thumb: tilt ≈ |lat| = 40.5° facing south."
    },
    "energy_yield": {
      "estimated_annual_yield_kwh_per_kwp": 882.11,
      "performance_ratio_used": 0.78,
      "temp_derate_factor": 1.0,
      "delta_t_above_stc_c": 0.0
    },
    "irradiance": {
      "mean_kwh_m2_day": 4.333,
      "monthly_means": {"2025-04": 4.81, "2025-05": 5.72, "2025-07": 6.23, "2025-12": 1.68},
      "best_worst_months": {
        "best_month":  {"month": "2025-07", "mean_kwh_m2_day": 6.2315},
        "worst_month": {"month": "2025-12", "mean_kwh_m2_day": 1.6838}
      },
      "variability_index": 0.5021,
      "clearness_index_mean": 0.5073,
      "clear_sky_utilisation": 0.507
    },
    "temperature": {"mean_c": 12.18, "max_c": 31.97, "min_c": -13.61},
    "wind":         {"mean_ws10m_m_s": 5.34},
    "humidity":     {"mean_rh2m_pct": 67.2},
    "cloud_cover":  {"mean_cloud_amt_pct": 49.8},
    "precipitation":{"mean_mm_day": 2.91},
    "sentinel_counts": {"ALLSKY_SFC_SW_DWN": 105, "T2M": 0}
  }
}
```

## Understanding the results

The worker computes the following metrics for every completed job:

### Panel orientation
- **Recommended tilt** = `|latitude|` degrees — the fixed-tilt angle that maximises annual irradiance for a flat-plate collector.
- **Recommended azimuth** = 180° (south-facing) in the Northern Hemisphere, 0° (north-facing) in the Southern Hemisphere.

### Energy yield estimate

Estimated annual DC output for a 1-kWp nameplate PV system:

```
E = G_daily × N_days × PR × T_derate
```

where `PR = 0.78` (performance ratio) and `T_derate = 1 - 0.004 × max(0, T_mean - 25)` accounts for crystalline-silicon efficiency loss above 25 °C STC.

### Solar Variability Index (SVI)

```
SVI = std(G_daily) / mean(G_daily)
```

- SVI < 0.3 — stable, predictable resource (good for baseload solar).
- SVI > 0.5 — highly variable (strong seasonality or frequent cloud cover).

### Clearness index (ALLSKY_KT)

Ratio of actual to theoretical clear-sky irradiance. Values close to 1 indicate predominantly clear-sky conditions; values below 0.4 indicate heavy cloud cover.

### Sentinel values (-999)

NASA POWER encodes missing or invalid days as `-999`. The worker strips all sentinel values before computing any statistic. The `sentinel_counts` field in the result reports how many missing days were found per parameter.

## Understanding the data

The NASA POWER dataset provides satellite-derived solar and meteorological data on a global 0.5° × 0.5° grid (~55 km resolution). Coordinates submitted to the API are automatically snapped to the nearest grid cell centre. The dataset covers daily values from 1981 to approximately 10 days before the current date (near-real-time latency).

Key concepts:

- **GHI (Global Horizontal Irradiance)** — `ALLSKY_SFC_SW_DWN` — total solar energy reaching a horizontal surface per day. This is the primary metric for flat-plate fixed-tilt PV systems.
- **Clearness index** — `ALLSKY_KT` — a dimensionless ratio (0–1) indicating how much of the theoretical clear-sky irradiance actually reaches the surface. The primary site quality metric.
- **Temperature de-rating** — mean air temperature (`T2M`) above 25 °C reduces PV panel output at ~0.4 %/°C for standard crystalline silicon. Cooler sites have a natural efficiency advantage.
- **Wind cooling** — `WS10M` — higher wind speeds reduce panel operating temperature, partially offsetting thermal losses.
- **Precipitation / cloud correlation** — `PRECTOTCORR` and `CLOUD_AMT` provide complementary views of sky conditions and can serve as a proxy for panel self-cleaning frequency.

### Data citation

This application uses data from the NASA POWER Project. Please cite as follows:

> Stackhouse, P.W., Jr., W.S. Chandler, D.J. Westberg, T. Zhang, J.M. Hoell, B. Eckstein (2018): NASA POWER: Agroclimatology Web Resources for Bioenergy and Agricultural Applications. Preprints, American Meteorological Society Annual Meeting.

For more information about NASA POWER and its data products, visit: https://power.larc.nasa.gov/
