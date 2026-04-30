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

## Instructions to run on Kubernetes

The `kubernetes/` directory contains manifests for deploying the full stack (FastAPI, worker, Redis) to a Kubernetes cluster. Two environments are provided:

| Environment | Directory | Replicas | Log Level | Ingress Host |
|---|---|---|---|---|
| **Production** | `kubernetes/prod/` | 2 (API + worker) | `INFO` | `alozano0304-nasa-power.coe332.tacc.cloud` |
| **Test** | `kubernetes/test/` | 1 (API + worker) | `DEBUG` | `alozano0304-nasa-power-test.coe332.tacc.cloud` |

### Kubernetes manifest overview

Each environment contains the following 8 files, applied in this order:

| File | Kind | Purpose |
|---|---|---|
| `app-{env}-pvc-redis.yml` | PersistentVolumeClaim | Persistent disk for Redis data (prod: 2Gi, test: 1Gi) |
| `app-{env}-deployment-redis.yml` | Deployment | Redis pod with persistence enabled (`--save 1 1`) |
| `app-{env}-service-redis.yml` | Service (ClusterIP) | Internal service so API and worker can reach Redis |
| `app-{env}-deployment-FastAPI.yml` | Deployment | FastAPI application pod(s) |
| `app-{env}-service-FastAPI.yml` | Service (ClusterIP) | Internal service for FastAPI |
| `app-{env}-service-nodeport-FastAPI.yml` | Service (NodePort) | Exposes FastAPI to the Ingress controller |
| `app-{env}-deployment-worker.yml` | Deployment | Background worker pod(s) running `worker.py` |
| `app-{env}-ingress-FastAPI.yml` | Ingress | Routes public HTTP traffic to the NodePort service |

### Prerequisites

- Access to a Kubernetes cluster (e.g. TACC COE332)
- `kubectl` configured with valid credentials (`~/.kube/config`)
- A Docker Hub account with your own images pushed (see [Customising for your own deployment](#customising-for-your-own-deployment) below)

### Customising for your own deployment

The manifests are pre-configured with the original author's Docker Hub username (`alozano0304`) and TACC ingress hostname. Before deploying, replace these with your own values:

**1. Docker Hub username** — appears in the `image:` field of the three deployment files. Change `alozano0304` to your Docker Hub username:

| File | Line to change |
|---|---|
| `kubernetes/prod/app-prod-deployment-FastAPI.yml` | `image: alozano0304/nasa-power-api:1.0` |
| `kubernetes/prod/app-prod-deployment-worker.yml` | `image: alozano0304/nasa-power-worker:1.0` |
| `kubernetes/test/app-test-deployment-FastAPI.yml` | `image: alozano0304/nasa-power-api:1.0` |
| `kubernetes/test/app-test-deployment-worker.yml` | `image: alozano0304/nasa-power-worker:1.0` |
| `docker-compose.yml` | `image: alozano0304/nasa-power-api:1.0` and `image: alozano0304/nasa-power-worker:1.0` |

You can do this in one command with `sed`:

```bash
# Replace throughout all kubernetes manifests and docker-compose
find kubernetes/ docker-compose.yml -type f | xargs sed -i 's/alozano0304/<your-dockerhub-username>/g'
```

Then rebuild and push the images under your own username:

```bash
docker compose build
docker compose push
```

**2. Ingress hostname** — appears in the `host:` field of the two ingress files. Change the hostname to match your cluster's domain:

| File | Line to change |
|---|---|
| `kubernetes/prod/app-prod-ingress-FastAPI.yml` | `host: "alozano0304-nasa-power.coe332.tacc.cloud"` |
| `kubernetes/test/app-test-ingress-FastAPI.yml` | `host: "alozano0304-nasa-power-test.coe332.tacc.cloud"` |

Replace `alozano0304` with your own username and `coe332.tacc.cloud` with your cluster's domain. For example:

```yaml
# kubernetes/prod/app-prod-ingress-FastAPI.yml
host: "<your-username>-nasa-power.<your-cluster-domain>"

# kubernetes/test/app-test-ingress-FastAPI.yml
host: "<your-username>-nasa-power-test.<your-cluster-domain>"
```

> **Note:** If your cluster uses a different `ingressClassName` than `nginx`, update that field too in both ingress files.

### Deploy to the test environment

```bash
# Apply all test manifests at once
kubectl apply -f kubernetes/test/
```

Or step-by-step in dependency order:

```bash
kubectl apply -f kubernetes/test/app-test-pvc-redis.yml
kubectl apply -f kubernetes/test/app-test-deployment-redis.yml
kubectl apply -f kubernetes/test/app-test-service-redis.yml
kubectl apply -f kubernetes/test/app-test-deployment-FastAPI.yml
kubectl apply -f kubernetes/test/app-test-service-FastAPI.yml
kubectl apply -f kubernetes/test/app-test-service-nodeport-FastAPI.yml
kubectl apply -f kubernetes/test/app-test-deployment-worker.yml
kubectl apply -f kubernetes/test/app-test-ingress-FastAPI.yml
```

### Deploy to the production environment

```bash
kubectl apply -f kubernetes/prod/
```

### Verify everything is running

```bash
# Check all pods are in Running state
kubectl get pods

# Check services
kubectl get services

# Check the Ingress and its assigned address
kubectl get ingress
```

Expected pod output (test environment):

```
NAME                                      READY   STATUS    RESTARTS   AGE
nasa-power-test-redis-<hash>              1/1     Running   0          1m
nasa-power-test-fastapi-<hash>            1/1     Running   0          1m
nasa-power-test-worker-<hash>             1/1     Running   0          1m
```

### Access the API

Once the Ingress is active, the API is reachable at the public hostname:

```bash
# Test environment
curl http://alozano0304-nasa-power-test.coe332.tacc.cloud/help

# Production environment
curl http://alozano0304-nasa-power.coe332.tacc.cloud/help
```

### View logs

```bash
# Stream logs from the FastAPI pods
kubectl logs -l app=nasa-power-test-fastapi -f

# Stream logs from the worker pods
kubectl logs -l app=nasa-power-test-worker -f
```

### Tear down an environment

```bash
# Remove all test resources
kubectl delete -f kubernetes/test/

# To also wipe the Redis persistent volume (deletes all stored data):
kubectl delete pvc nasa-power-test-redis-data
```

### Updating the image

When a new version of the app is ready, rebuild, push, and trigger a rolling restart:

```bash
# Rebuild and push both images
docker compose build
docker compose push

# Trigger a rolling restart so Kubernetes pulls the new image
kubectl rollout restart deployment/nasa-power-prod-fastapi
kubectl rollout restart deployment/nasa-power-prod-worker
```

## Running Tests

The project includes unit and integration tests to verify functionality at multiple levels.

### Run all tests

With Docker containers running, execute all test suites:

```bash
# Unit tests — no live server required (mocks Redis)
uv run pytest test/test_jobs.py test/test_api_unit.py -v

# API integration tests — requires containers running
uv run python test/test_FastAPI_api.py

# Worker integration tests — requires containers running
uv run python test/test_worker.py
```

Or run the full suite together:

```bash
uv run pytest test/ -v
```

### Test summary

- **test_jobs.py** — Unit tests for all job queue operations (`add_job`, `get_job_by_id`, `start_job`, `update_job_status`, `save_job_result`, `get_job_result`) using mocked Redis. Also validates the multi-location result structure and `comparison_summary` shape.
- **test_api_unit.py** — Unit tests for the FastAPI application with mocked Redis and NASA POWER calls. Validates coordinate validation, date validation, job submission (single Job response), and polling endpoints.
- **test_FastAPI_api.py** — Integration tests for all HTTP endpoints against the live running containers. Covers location CRUD, multi-location name/coordinate lookups, job submission (single-Job response shape), and result polling.
- **test_worker.py** — Integration tests verifying the worker correctly computes and stores combined solar characterization results end-to-end, including single-location results, southern-hemisphere panel orientation, sentinel filtering, and multi-location comparison summaries.

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
| `/health` | GET | Return Redis connectivity and application health status |
| `/locations` | POST | Fetch a point from NASA POWER and store it as a named location |
| `/locations` | GET | List all stored locations (id, lat, lon, name) |
| `/locations/name/{name}` | GET | Retrieve stored location(s) by friendly name (case-insensitive) |
| `/locations/{loc_id}` | GET | Retrieve a stored location by UUID |
| `/locations/{lat}/{lon}` | GET | Retrieve stored location(s) by snapped coordinates |
| `/locations/{loc_id}` | DELETE | Delete a stored location and all its Redis mappings |
| `/jobs` | POST | Queue **one job** for all supplied location UUIDs; returns a single Job |
| `/jobs` | GET | List all queued / running / finished jobs |
| `/jobs/{jid}` | GET | Retrieve details for a specific job by its UUID |
| `/results/{jid}` | GET | Retrieve the combined solar characterization result for a completed job |
| `/results/{jid}/plot` | GET | Return the daily irradiance overlay PNG for a completed job |

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

The job queue system allows you to submit asynchronous solar site characterization analyses. A single POST /jobs request creates **one job** that covers all supplied location UUIDs. The background worker reads the stored time-series data for every location, computes a full set of solar engineering metrics, generates a combined irradiance overlay plot, and saves the result to Redis db=3.

#### POST /jobs

Submit a characterization job for one or more stored location UUIDs. **One job is always created**, regardless of how many locations are supplied. The combined result (all locations in one response) is available via `GET /results/{jid}` once the job completes.

```bash
curl -X POST http://localhost:5000/jobs \
  -H "Content-Type: application/json" \
  -d '{"location_ids": ["7fe1e7cd-...", "9a8dee41-...", "b0e5c1e7-..."]}'
```

Expected output (single Job object):

```json
{
  "jid": "df89de06-8729-40af-815e-cd3a74a6cc3e",
  "status": "QUEUED",
  "location_ids": ["7fe1e7cd-...", "9a8dee41-...", "b0e5c1e7-..."],
  "start_date": null,
  "end_date": null,
  "start_time": null,
  "end_time": null
}
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
    "location_ids": ["7fe1e7cd-..."],
    "start_date": null,
    "end_date": null,
    "start_time": "2026-04-25T07:12:04.123456+00:00",
    "end_time": "2026-04-25T07:12:08.789012+00:00"
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
  "location_ids": ["7fe1e7cd-..."],
  "start_date": null,
  "end_date": null,
  "start_time": "2026-04-25T07:12:04.123456+00:00",
  "end_time": "2026-04-25T07:12:08.789012+00:00"
}
```

#### GET /results/{jid}

Retrieve the combined solar site characterization result for a completed job.

**For a job still queued or running:**

```bash
curl http://localhost:5000/results/df89de06-8729-40af-815e-cd3a74a6cc3e
```

Expected output:

```json
{"job_id": "df89de06-...", "job_status": "RUNNING", "message": "Job is currently running — check back soon."}
```

**For a completed job (single location):**

```json
{
  "status": "success",
  "job_id": "df89de06-...",
  "job_status": "FINISHED -- SUCCESS",
  "start_time": "2026-04-25T07:12:04.123456+00:00",
  "end_time": "2026-04-25T07:12:08.789012+00:00",
  "location_count": 1,
  "locations": [
    {
      "location": {
        "id": "7fe1e7cd-...",
        "lat": 40.5,
        "lon": -74.0,
        "name": "NewYork",
        "start_date": "20250415",
        "end_date": "20260415",
        "n_days": 366
      },
      "panel_orientation": {
        "recommended_tilt_deg": 40.5,
        "recommended_azimuth_deg": 180.0,
        "facing": "South",
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
        "monthly_means": {"2025-04": 4.81, "2025-05": 5.72, "2025-12": 1.68},
        "best_worst_months": {
          "best_month":  {"month": "2025-07", "mean_kwh_m2_day": 6.23},
          "worst_month": {"month": "2025-12", "mean_kwh_m2_day": 1.68}
        },
        "variability_index": 0.5021,
        "clearness_index_mean": 0.5073,
        "clear_sky_utilisation": 0.507
      },
      "temperature":   {"mean_c": 12.18, "max_c": 31.97, "min_c": -13.61},
      "wind":          {"mean_ws10m_m_s": 5.34},
      "humidity":      {"mean_rh2m_pct": 67.2},
      "cloud_cover":   {"mean_cloud_amt_pct": 49.8},
      "precipitation": {"mean_mm_day": 2.91},
      "sentinel_counts": {"ALLSKY_SFC_SW_DWN": 0, "T2M": 0},
      "peak_sun_hours": {"daily_average": 4.333, "note": "..."},
      "pv_suitability": {"score": 3, "label": "Moderate", "mean_ghi": 4.333}
    }
  ]
}
```

**For a completed multi-location job**, the response also includes a `comparison_summary` key:

```json
{
  "location_count": 2,
  "locations": ["..."],
  "comparison_summary": {
    "ranked": [
      {"rank": 1, "name": "Miami", "mean_irradiance_kwh_m2_day": 5.61, "...": "..."},
      {"rank": 2, "name": "NewYork", "mean_irradiance_kwh_m2_day": 4.33, "...": "..."}
    ],
    "best_site": {"name": "Miami", "mean_irradiance_kwh_m2_day": 5.61},
    "comparison": "Best site: Miami — 5.61 kWh/m²/day ..."
  }
}
```

#### GET /results/{jid}/plot

Returns the daily all-sky irradiance overlay chart as a PNG image. For single-location jobs a single line is drawn; for multi-location jobs all locations are overlaid on the same axes for direct visual comparison.

```bash
curl http://localhost:5000/results/df89de06-8729-40af-815e-cd3a74a6cc3e/plot --output plot.png
```

Returns: `image/png` binary data (HTTP 200), or HTTP 404 if the job has no plot stored.

## Understanding the results

The worker computes the following metrics for every completed job. Each entry in the `locations` list contains:

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

### Clear-sky utilisation

Ratio of mean all-sky irradiance to mean clear-sky irradiance. Values close to 1 indicate low average cloud impact.

### PV suitability score

A simple 1–5 integer score derived from the mean daily GHI:

| Score | Label | Mean GHI (kWh/m²/day) |
|---|---|---|
| 1 | Poor | < 2.0 |
| 2 | Below average | 2.0 – 3.5 |
| 3 | Moderate | 3.5 – 5.0 |
| 4 | Good | 5.0 – 6.0 |
| 5 | Excellent | > 6.0 |

### Irradiance overlay plot

`GET /results/{jid}/plot` returns a PNG chart of daily all-sky irradiance over the full data window. For multi-location jobs all locations are drawn on the same axes so resource differences are immediately visible.

### Multi-location comparison summary

When a job covers two or more locations the result includes a `comparison_summary` with:
- `ranked` — all locations sorted best → worst by mean irradiance (ties broken by clearness index then lower temperature de-rating).
- `best_site` — the top-ranked location with its key metrics.
- `comparison` — a plain-text narrative summarising the ranking.

### Sentinel values (-999)

NASA POWER encodes missing or invalid days as `-999`. The worker strips all sentinel values before computing any statistic. The `sentinel_counts` field in each location result reports how many missing days were found per parameter.

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
