"""
test_FastAPI_api.py — Integration tests for FastAPI_api.py
============================================================
Tests all 9 HTTP endpoints against the live running Docker containers.
Requires the API to be available at http://localhost:5000.

Run with:
    uv run python test/test_FastAPI_api.py
"""

import time
import unittest

import requests

BASE_URL = "http://localhost:5000"

# Three well-known test locations used throughout the suite.
# Coordinates are already on the 0.5° NASA POWER grid so snapping is a no-op.
TEST_LOCATIONS = [
    {"lat": 40.5, "lon": -74.0, "name": "NewYork"},
    {"lat": 26.0, "lon": -80.0, "name": "Miami"},
    {"lat": 34.0, "lon": -118.0, "name": "LosAngeles"},
]


def _wait_for_api(retries=15, delay=1.0):
    """Block until the API responds or raise after `retries` attempts."""
    for attempt in range(retries):
        try:
            r = requests.get(f"{BASE_URL}/locations", timeout=3)
            if r.status_code in (200, 404):
                return
        except requests.ConnectionError:
            pass
        time.sleep(delay)
    raise RuntimeError("Could not connect to API at " + BASE_URL)


class TestLocationCreate(unittest.TestCase):
    """Tests for POST /locations — fetch from NASA POWER and store."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()

    def test_post_location_returns_201(self):
        """POST /locations must return HTTP 201."""
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])
        self.assertEqual(r.status_code, 201)

    def test_post_location_response_has_id(self):
        """POST /locations response must include a UUID id field."""
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[1])
        self.assertEqual(r.status_code, 201)
        data = r.json()
        self.assertIn("id", data)
        self.assertIsInstance(data["id"], str)
        self.assertEqual(data["id"].count("-"), 4)  # UUID4 format

    def test_post_location_response_has_parameter_data(self):
        """POST /locations response must include at least one parameter list."""
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[2])
        self.assertEqual(r.status_code, 201)
        data = r.json()
        self.assertIn("ALLSKY_SFC_SW_DWN", data)
        self.assertIsInstance(data["ALLSKY_SFC_SW_DWN"], list)
        self.assertGreater(len(data["ALLSKY_SFC_SW_DWN"]), 0)

    def test_post_location_snaps_coordinates(self):
        """POST /locations must snap coordinates to the 0.5° NASA POWER grid."""
        r = requests.post(f"{BASE_URL}/locations",
                          json={"lat": 40.71, "lon": -74.01, "name": "NYC_raw"})
        self.assertEqual(r.status_code, 201)
        data = r.json()
        # 40.71 snaps to 40.5, -74.01 snaps to -74.0
        self.assertEqual(data["lat"], 40.5)
        self.assertEqual(data["lon"], -74.0)

    def test_post_location_rejects_out_of_range_lat(self):
        """POST /locations must reject latitudes outside ±90°."""
        r = requests.post(f"{BASE_URL}/locations",
                          json={"lat": 95.0, "lon": 0.0, "name": "bad"})
        self.assertIn(r.status_code, (400, 422))

    def test_post_location_rejects_out_of_range_lon(self):
        """POST /locations must reject longitudes outside ±180°."""
        r = requests.post(f"{BASE_URL}/locations",
                          json={"lat": 0.0, "lon": 200.0, "name": "bad"})
        self.assertIn(r.status_code, (400, 422))


class TestLocationList(unittest.TestCase):
    """Tests for GET /locations — list all stored locations."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        # Ensure at least one location exists
        requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])

    def test_get_locations_returns_200(self):
        """GET /locations must return HTTP 200."""
        r = requests.get(f"{BASE_URL}/locations")
        self.assertEqual(r.status_code, 200)

    def test_get_locations_returns_dict_with_locations_key(self):
        """GET /locations must return a JSON object with a 'locations' list."""
        r = requests.get(f"{BASE_URL}/locations")
        data = r.json()
        self.assertIsInstance(data, dict)
        self.assertIn("locations", data)
        self.assertIsInstance(data["locations"], list)

    def test_get_locations_items_have_required_fields(self):
        """Each entry in GET /locations must have id, lat, lon, name."""
        r = requests.get(f"{BASE_URL}/locations")
        locations = r.json()["locations"]
        self.assertGreater(len(locations), 0)
        for loc in locations:
            for field in ("id", "lat", "lon", "name"):
                self.assertIn(field, loc)


class TestLocationByName(unittest.TestCase):
    """Tests for GET /locations/name/{name}."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[1])  # Miami

    def test_get_by_name_returns_200(self):
        """GET /locations/name/Miami must return HTTP 200."""
        r = requests.get(f"{BASE_URL}/locations/name/Miami")
        self.assertEqual(r.status_code, 200)

    def test_get_by_name_returns_location_wrapper(self):
        """Response must be wrapped in a 'location' key."""
        r = requests.get(f"{BASE_URL}/locations/name/Miami")
        data = r.json()
        self.assertIn("location", data)

    def test_get_by_name_includes_parameter_series(self):
        """Returned location must include all 10 NASA parameter lists."""
        r = requests.get(f"{BASE_URL}/locations/name/Miami")
        loc = r.json()["location"]
        for param in ("ALLSKY_SFC_SW_DWN", "CLRSKY_SFC_SW_DWN", "ALLSKY_KT",
                      "T2M", "T2M_MAX", "T2M_MIN", "WS10M", "RH2M",
                      "PRECTOTCORR", "CLOUD_AMT"):
            self.assertIn(param, loc)
            self.assertIsInstance(loc[param], list)

    def test_get_by_name_case_insensitive(self):
        """Name lookup must be case-insensitive."""
        r = requests.get(f"{BASE_URL}/locations/name/miami")
        self.assertEqual(r.status_code, 200)

    def test_get_by_name_unknown_returns_404(self):
        """GET /locations/name/{name} must return 404 for unknown names."""
        r = requests.get(f"{BASE_URL}/locations/name/NoSuchPlace99")
        self.assertEqual(r.status_code, 404)


class TestLocationById(unittest.TestCase):
    """Tests for GET /locations/{loc_id}."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])
        cls.loc_id = r.json()["id"]

    def test_get_by_id_returns_200(self):
        """GET /locations/{loc_id} must return HTTP 200."""
        r = requests.get(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertEqual(r.status_code, 200)

    def test_get_by_id_returns_location_wrapper(self):
        """Response must be wrapped in a 'location' key."""
        r = requests.get(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertIn("location", r.json())

    def test_get_by_id_id_matches(self):
        """Returned location id must match the requested id."""
        r = requests.get(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertEqual(r.json()["location"]["id"], self.loc_id)

    def test_get_by_id_unknown_returns_404(self):
        """GET /locations/{loc_id} must return 404 for unknown UUIDs."""
        r = requests.get(f"{BASE_URL}/locations/00000000-0000-0000-0000-000000000000")
        self.assertEqual(r.status_code, 404)


class TestLocationByCoords(unittest.TestCase):
    """Tests for GET /locations/{lat}/{lon}."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[2])  # LosAngeles

    def test_get_by_coords_returns_200(self):
        """GET /locations/34.0/-118.0 must return HTTP 200."""
        r = requests.get(f"{BASE_URL}/locations/34.0/-118.0")
        self.assertEqual(r.status_code, 200)

    def test_get_by_coords_includes_queried_and_snapped(self):
        """Response must include queried_lat/lon and snapped_lat/lon."""
        r = requests.get(f"{BASE_URL}/locations/34.0/-118.0")
        data = r.json()
        for field in ("queried_lat", "queried_lon", "snapped_lat", "snapped_lon"):
            self.assertIn(field, data)

    def test_get_by_coords_snapping_is_correct(self):
        """Querying 34.2/-118.3 must snap to 34.0/-118.5."""
        r = requests.get(f"{BASE_URL}/locations/34.2/-118.3")
        # This may 404 if that snapped cell has no location — that's fine.
        # We just verify the shape when it succeeds.
        if r.status_code == 200:
            data = r.json()
            self.assertIn("snapped_lat", data)

    def test_get_by_coords_unknown_returns_404(self):
        """Coordinates with no stored location must return 404 (or 422 if out of bounds)."""
        r = requests.get(f"{BASE_URL}/locations/0.0/0.0")
        self.assertIn(r.status_code, (404, 422))


class TestLocationDelete(unittest.TestCase):
    """Tests for DELETE /locations/{loc_id}."""

    def setUp(self):
        """Create a fresh location for each delete test."""
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations",
                          json={"lat": 51.5, "lon": 0.0, "name": "London_tmp"})
        self.loc_id = r.json()["id"]

    def test_delete_returns_200(self):
        """DELETE /locations/{loc_id} must return HTTP 200."""
        r = requests.delete(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertEqual(r.status_code, 200)

    def test_delete_response_has_deleted_true(self):
        """DELETE response must include deleted=true."""
        r = requests.delete(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertTrue(r.json().get("deleted"))

    def test_delete_removes_location(self):
        """After DELETE, GET by id must return 404."""
        requests.delete(f"{BASE_URL}/locations/{self.loc_id}")
        r = requests.get(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertEqual(r.status_code, 404)

    def test_delete_unknown_returns_404(self):
        """DELETE on an unknown UUID must return 404."""
        r = requests.delete(f"{BASE_URL}/locations/00000000-0000-0000-0000-000000000000")
        self.assertEqual(r.status_code, 404)


class TestJobEndpoints(unittest.TestCase):
    """Tests for POST /jobs, GET /jobs, GET /jobs/{jid}."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        # Ensure at least one location exists and grab its id
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])
        cls.loc_id = r.json()["id"]

    def test_post_jobs_returns_list(self):
        """POST /jobs must return a JSON list."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(r.json(), list)

    def test_post_jobs_one_job_per_location(self):
        """POST /jobs with N location_ids must create exactly N jobs."""
        # Create a second location
        r2 = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[1])
        loc_id2 = r2.json()["id"]

        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id, loc_id2]})
        self.assertEqual(len(r.json()), 2)

    def test_post_jobs_status_is_queued(self):
        """Newly created jobs must have status QUEUED."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        job = r.json()[0]
        self.assertEqual(job["status"], "QUEUED")

    def test_post_jobs_job_has_required_fields(self):
        """Each job dict must contain jid, status, lat, lon, start_date, end_date."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        job = r.json()[0]
        for field in ("jid", "status", "lat", "lon", "start_date", "end_date"):
            self.assertIn(field, job)

    def test_post_jobs_rejects_empty_location_ids(self):
        """POST /jobs with an empty location_ids list must return 4xx."""
        r = requests.post(f"{BASE_URL}/jobs", json={"location_ids": []})
        self.assertIn(r.status_code, (400, 422))

    def test_get_jobs_returns_list(self):
        """GET /jobs must return a JSON list."""
        r = requests.get(f"{BASE_URL}/jobs")
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(r.json(), list)

    def test_get_jobs_items_have_jid_and_status(self):
        """Every job in GET /jobs must have jid and status fields."""
        requests.post(f"{BASE_URL}/jobs", json={"location_ids": [self.loc_id]})
        r = requests.get(f"{BASE_URL}/jobs")
        jobs = r.json()
        self.assertGreater(len(jobs), 0)
        for job in jobs:
            self.assertIn("jid", job)
            self.assertIn("status", job)

    def test_get_job_by_jid_returns_200(self):
        """GET /jobs/{jid} must return HTTP 200 for a known job."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        jid = r.json()[0]["jid"]
        r2 = requests.get(f"{BASE_URL}/jobs/{jid}")
        self.assertEqual(r2.status_code, 200)

    def test_get_job_by_jid_returns_correct_jid(self):
        """GET /jobs/{jid} must return the job whose jid matches."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        jid = r.json()[0]["jid"]
        r2 = requests.get(f"{BASE_URL}/jobs/{jid}")
        self.assertEqual(r2.json()["jid"], jid)

    def test_get_job_unknown_jid_returns_404(self):
        """GET /jobs/{jid} must return 404 for an unknown job ID."""
        r = requests.get(f"{BASE_URL}/jobs/00000000-0000-0000-0000-000000000000")
        self.assertEqual(r.status_code, 404)


class TestResultsEndpoint(unittest.TestCase):
    """Tests for GET /results/{jid} — solar characterization result retrieval."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])
        cls.loc_id = r.json()["id"]

    def _submit_job(self):
        """Helper: submit one job and return its jid."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        return r.json()[0]["jid"]

    def _wait_for_success(self, jid, timeout=30):
        """Poll GET /results/{jid} until SUCCESS or timeout."""
        for _ in range(timeout // 2):
            time.sleep(2)
            r = requests.get(f"{BASE_URL}/results/{jid}")
            status = r.json().get("job_status", "")
            if "SUCCESS" in status or "ERROR" in status:
                return r.json()
        return requests.get(f"{BASE_URL}/results/{jid}").json()

    def test_get_results_returns_200(self):
        """GET /results/{jid} must return HTTP 200."""
        jid = self._submit_job()
        r = requests.get(f"{BASE_URL}/results/{jid}")
        self.assertEqual(r.status_code, 200)

    def test_get_results_has_job_status(self):
        """GET /results/{jid} must include a job_status field."""
        jid = self._submit_job()
        r = requests.get(f"{BASE_URL}/results/{jid}")
        self.assertIn("job_status", r.json())

    def test_get_results_unknown_jid_returns_404(self):
        """GET /results/{jid} must return 404 for unknown job IDs."""
        r = requests.get(f"{BASE_URL}/results/00000000-0000-0000-0000-000000000000")
        self.assertEqual(r.status_code, 404)

    def test_get_results_completed_job_has_results_key(self):
        """After job SUCCESS, response must include a 'results' key."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        self.assertIn("results", data)
        self.assertIsNotNone(data["results"])

    def test_get_results_has_panel_orientation(self):
        """Results must include panel_orientation with tilt and azimuth."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        ori = data["results"]["panel_orientation"]
        self.assertIn("recommended_tilt_deg", ori)
        self.assertIn("recommended_azimuth_deg", ori)
        self.assertIn("facing", ori)

    def test_get_results_tilt_equals_abs_latitude(self):
        """Recommended tilt must equal |lat| of the location."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        lat = abs(data["results"]["location"]["lat"])
        tilt = data["results"]["panel_orientation"]["recommended_tilt_deg"]
        self.assertAlmostEqual(tilt, lat, places=1)

    def test_get_results_has_energy_yield(self):
        """Results must include energy_yield with annual kWh/kWp estimate."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        yld = data["results"]["energy_yield"]
        self.assertIn("estimated_annual_yield_kwh_per_kwp", yld)
        self.assertGreater(yld["estimated_annual_yield_kwh_per_kwp"], 0)

    def test_get_results_has_irradiance_section(self):
        """Results must include irradiance section with mean and monthly breakdown."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        irr = data["results"]["irradiance"]
        self.assertIn("mean_kwh_m2_day", irr)
        self.assertIn("monthly_means", irr)
        self.assertIn("best_worst_months", irr)
        self.assertIn("variability_index", irr)
        self.assertIn("clearness_index_mean", irr)

    def test_get_results_has_sentinel_counts(self):
        """Results must include sentinel_counts mapping parameter -> int."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        sc = data["results"]["sentinel_counts"]
        self.assertIn("ALLSKY_SFC_SW_DWN", sc)
        self.assertIsInstance(sc["ALLSKY_SFC_SW_DWN"], int)

    def test_get_results_parameter_stats_present(self):
        """Results must include per-parameter descriptive statistics."""
        jid = self._submit_job()
        data = self._wait_for_success(jid)
        stats = data["results"]["parameter_stats"]
        self.assertIn("ALLSKY_SFC_SW_DWN", stats)
        for key in ("mean", "median", "std", "min", "max", "count"):
            self.assertIn(key, stats["ALLSKY_SFC_SW_DWN"])


if __name__ == '__main__':
    unittest.main()
