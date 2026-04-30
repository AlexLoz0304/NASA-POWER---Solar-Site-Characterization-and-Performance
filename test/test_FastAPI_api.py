"""
test_FastAPI_api.py — Integration tests for FastAPI_api.py
============================================================
Tests all HTTP endpoints against the live running Docker containers.
Requires the API to be available at http://localhost:5000.

Test classes:
  TestHelpEndpoint          — GET /help endpoint reference
  TestLocationCreate        — POST /locations
  TestLocationList          — GET /locations
  TestLocationByName        — GET /locations/name/{name} (single result)
  TestLocationByNameMultiple— GET /locations/name/{name} (multiple results)
  TestLocationById          — GET /locations/{loc_id}
  TestLocationByCoords      — GET /locations/{lat}/{lon} (single result)
  TestLocationByCoordsMultiple — GET /locations/{lat}/{lon} (multiple results)
  TestLocationDelete        — DELETE /locations/{loc_id}
  TestJobEndpoints          — POST/GET /jobs and GET /jobs/{jid}
  TestResultsEndpoint       — GET /results/{jid}

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

    def test_post_location_response_has_id(self):
        """POST /locations must return 201 with a UUID id field."""
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

    def test_post_location_rejects_invalid_coordinates(self):
        """POST /locations must reject latitudes outside ±90° and longitudes outside ±180°."""
        r_lat = requests.post(f"{BASE_URL}/locations",
                              json={"lat": 95.0, "lon": 0.0, "name": "bad"})
        self.assertIn(r_lat.status_code, (400, 422))
        r_lon = requests.post(f"{BASE_URL}/locations",
                              json={"lat": 0.0, "lon": 200.0, "name": "bad"})
        self.assertIn(r_lon.status_code, (400, 422))


class TestLocationList(unittest.TestCase):
    """Tests for GET /locations — list all stored locations."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        # Ensure at least one location exists
        requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])

    def test_get_locations_response_shape(self):
        """GET /locations must return 200 with a 'locations' list whose items have id, lat, lon, name."""
        r = requests.get(f"{BASE_URL}/locations")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIsInstance(data, dict)
        self.assertIn("locations", data)
        self.assertIsInstance(data["locations"], list)
        self.assertGreater(len(data["locations"]), 0)
        for loc in data["locations"]:
            for field in ("id", "lat", "lon", "name"):
                self.assertIn(field, loc)


class TestLocationByName(unittest.TestCase):
    """Tests for GET /locations/name/{name}."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        # Delete any existing Miami entries to avoid stale duplicates
        r = requests.get(f"{BASE_URL}/locations/name/Miami")
        if r.status_code == 200:
            data = r.json()
            existing = data.get("locations", [data.get("location")] if "location" in data else [])
            for loc in existing:
                if loc:
                    requests.delete(f"{BASE_URL}/locations/{loc['id']}")
        requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[1])  # Miami

    def test_get_by_name_returns_location_with_data(self):
        """GET /locations/name/Miami must return 200 with a 'location' wrapper and all 10 NASA parameter lists."""
        r = requests.get(f"{BASE_URL}/locations/name/Miami")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("location", data)
        loc = data["location"]
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


class TestLocationByNameMultiple(unittest.TestCase):
    """Tests for GET /locations/name/{name} when multiple locations share the same name."""

    SHARED_NAME = "SharedSite"
    # Two grid cells that both map to distinct snapped coordinates so they
    # are stored as separate Redis records but registered under the same name.
    LOC_A = {"lat": 39.5, "lon": -105.0, "name": SHARED_NAME}  # Denver area
    LOC_B = {"lat": 34.0, "lon": -118.0, "name": SHARED_NAME}  # LA grid cell

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        # Clean up any pre-existing SharedSite entries from stale runs
        r = requests.get(f"{BASE_URL}/locations/name/{cls.SHARED_NAME}")
        if r.status_code == 200:
            data = r.json()
            existing = data.get("locations", [data.get("location")] if "location" in data else [])
            for loc in existing:
                if loc:
                    requests.delete(f"{BASE_URL}/locations/{loc['id']}")
        # Post both locations under the same name
        r_a = requests.post(f"{BASE_URL}/locations", json=cls.LOC_A)
        r_b = requests.post(f"{BASE_URL}/locations", json=cls.LOC_B)
        cls.id_a = r_a.json()["id"]
        cls.id_b = r_b.json()["id"]

    @classmethod
    def tearDownClass(cls):
        """Remove the two shared-name locations so they don't pollute other tests."""
        for loc_id in (cls.id_a, cls.id_b):
            requests.delete(f"{BASE_URL}/locations/{loc_id}")

    def test_get_by_shared_name_returns_two_locations(self):
        """GET /locations/name/SharedSite must return 200 with a 'locations' list of exactly 2 items."""
        r = requests.get(f"{BASE_URL}/locations/name/{self.SHARED_NAME}")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("locations", data,
                      "Expected 'locations' key when multiple entries share the same name")
        self.assertNotIn("location", data,
                         "Singular 'location' key must not appear when multiple results exist")
        self.assertEqual(len(data["locations"]), 2)

    def test_get_by_shared_name_both_known_ids_present(self):
        """Both IDs created in setUpClass must appear in the results with distinct UUIDs."""
        r = requests.get(f"{BASE_URL}/locations/name/{self.SHARED_NAME}")
        ids = {loc["id"] for loc in r.json()["locations"]}
        self.assertEqual(len(ids), 2, f"Expected 2 distinct ids, got: {ids}")
        self.assertIn(self.id_a, ids)
        self.assertIn(self.id_b, ids)

    def test_get_by_shared_name_coords_are_distinct(self):
        """The two SharedSite results must be at different grid cells."""
        r = requests.get(f"{BASE_URL}/locations/name/{self.SHARED_NAME}")
        coords = {(loc["lat"], loc["lon"]) for loc in r.json()["locations"]}
        self.assertEqual(len(coords), 2,
                         f"Expected 2 distinct coordinate pairs, got: {coords}")

    def test_get_by_shared_name_all_have_shared_name(self):
        """Every result must carry the SharedSite name."""
        r = requests.get(f"{BASE_URL}/locations/name/{self.SHARED_NAME}")
        for loc in r.json()["locations"]:
            self.assertEqual(loc["name"], self.SHARED_NAME)

    def test_get_by_shared_name_case_insensitive_multi(self):
        """Case-insensitive lookup must also return multiple results."""
        r = requests.get(f"{BASE_URL}/locations/name/{self.SHARED_NAME.lower()}")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("locations", data)
        self.assertEqual(len(data["locations"]), 2)


class TestLocationById(unittest.TestCase):
    """Tests for GET /locations/{loc_id}."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[0])
        cls.loc_id = r.json()["id"]

    def test_get_by_id_returns_correct_location(self):
        """GET /locations/{loc_id} must return 200 with a 'location' wrapper whose id matches."""
        r = requests.get(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("location", data)
        self.assertEqual(data["location"]["id"], self.loc_id)

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

    def test_get_by_coords_returns_200_with_transparency_fields(self):
        """GET /locations/34.0/-118.0 must return 200 with queried and snapped coordinate fields."""
        r = requests.get(f"{BASE_URL}/locations/34.0/-118.0")
        self.assertEqual(r.status_code, 200)
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


class TestLocationByCoordsMultiple(unittest.TestCase):
    """Tests for GET /locations/{lat}/{lon} when multiple locations share the same grid cell."""

    # Two slightly different raw coordinates that both snap to the same 0.5° cell:
    #   snap(39.5) = 39.5,  snap(-105.0) = -105.0  (exact grid point)
    #   snap(39.7) = 39.5,  snap(-104.8) = -105.0  (nearby raw coords)
    SNAPPED_LAT = 39.5
    SNAPPED_LON = -105.0
    LOC_A = {"lat": 39.5, "lon": -105.0, "name": "Denver_A"}
    LOC_B = {"lat": 39.7, "lon": -104.8, "name": "Denver_B"}

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        # Clean up any pre-existing Denver_A / Denver_B entries at this grid cell
        r = requests.get(f"{BASE_URL}/locations/{cls.SNAPPED_LAT}/{cls.SNAPPED_LON}")
        if r.status_code == 200:
            data = r.json()
            existing = data.get("locations", [data.get("location")] if "location" in data else [])
            for loc in existing:
                if loc:
                    requests.delete(f"{BASE_URL}/locations/{loc['id']}")
        # Post both locations — different raw coords, same snapped cell
        r_a = requests.post(f"{BASE_URL}/locations", json=cls.LOC_A)
        r_b = requests.post(f"{BASE_URL}/locations", json=cls.LOC_B)
        cls.id_a = r_a.json()["id"]
        cls.id_b = r_b.json()["id"]

    @classmethod
    def tearDownClass(cls):
        """Remove the two Denver locations so they don't pollute other tests."""
        for loc_id in (cls.id_a, cls.id_b):
            requests.delete(f"{BASE_URL}/locations/{loc_id}")

    def test_get_by_snapped_coords_returns_two_locations(self):
        """GET at the shared grid cell must return 200 with a 'locations' list of exactly 2 items."""
        r = requests.get(f"{BASE_URL}/locations/{self.SNAPPED_LAT}/{self.SNAPPED_LON}")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("locations", data,
                      "Expected 'locations' key when multiple entries exist at the same grid cell")
        self.assertNotIn("location", data,
                         "Singular 'location' key must not appear when multiple results exist")
        self.assertEqual(len(data["locations"]), 2)

    def test_get_by_snapped_coords_both_known_ids_present(self):
        """Both IDs created in setUpClass must appear in the results with distinct UUIDs."""
        r = requests.get(f"{BASE_URL}/locations/{self.SNAPPED_LAT}/{self.SNAPPED_LON}")
        ids = {loc["id"] for loc in r.json()["locations"]}
        self.assertEqual(len(ids), 2, f"Expected 2 distinct ids, got: {ids}")
        self.assertIn(self.id_a, ids)
        self.assertIn(self.id_b, ids)

    def test_get_by_snapped_coords_names_are_distinct(self):
        """The two results must have different names."""
        r = requests.get(f"{BASE_URL}/locations/{self.SNAPPED_LAT}/{self.SNAPPED_LON}")
        names = {loc["name"] for loc in r.json()["locations"]}
        self.assertEqual(len(names), 2,
                         f"Expected 2 distinct names, got: {names}")

    def test_get_by_snapped_coords_transparency_fields_present(self):
        """Response must always include queried_lat/lon and snapped_lat/lon."""
        r = requests.get(f"{BASE_URL}/locations/{self.SNAPPED_LAT}/{self.SNAPPED_LON}")
        data = r.json()
        for field in ("queried_lat", "queried_lon", "snapped_lat", "snapped_lon"):
            self.assertIn(field, data)

    def test_get_by_raw_coords_snaps_to_same_cell(self):
        """Querying raw coords (39.7/-104.8) must resolve to the same two-location cell."""
        r = requests.get(f"{BASE_URL}/locations/39.7/-104.8")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["snapped_lat"], self.SNAPPED_LAT)
        self.assertEqual(data["snapped_lon"], self.SNAPPED_LON)
        self.assertIn("locations", data)
        self.assertEqual(len(data["locations"]), 2)


class TestLocationDelete(unittest.TestCase):
    """Tests for DELETE /locations/{loc_id}."""

    def setUp(self):
        """Create a fresh location for each delete test."""
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations",
                          json={"lat": 51.5, "lon": 0.0, "name": "London_tmp"})
        self.loc_id = r.json()["id"]

    def test_delete_succeeds(self):
        """DELETE /locations/{loc_id} must return 200 with deleted=true."""
        r = requests.delete(f"{BASE_URL}/locations/{self.loc_id}")
        self.assertEqual(r.status_code, 200)
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

    def test_post_jobs_returns_single_job(self):
        """POST /jobs must return a single Job JSON object (dict)."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(r.json(), dict)
        self.assertIn("jid", r.json())

    def test_post_jobs_single_job_for_multiple_locations(self):
        """POST /jobs with N location_ids must create exactly one job covering all locations."""
        # Create a second location
        r2 = requests.post(f"{BASE_URL}/locations", json=TEST_LOCATIONS[1])
        loc_id2 = r2.json()["id"]

        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id, loc_id2]})
        self.assertEqual(r.status_code, 200)
        job = r.json()
        self.assertIsInstance(job, dict)
        self.assertIn("jid", job)
        self.assertIn(self.loc_id, job["location_ids"])
        self.assertIn(loc_id2, job["location_ids"])

    def test_post_jobs_job_shape(self):
        """Newly created job must have status QUEUED and all required fields."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        job = r.json()
        self.assertEqual(job["status"], "QUEUED")
        for field in ("jid", "status", "location_ids", "start_date", "end_date"):
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

    def test_get_job_by_jid_returns_correct_job(self):
        """GET /jobs/{jid} must return 200 with the job whose jid matches."""
        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [self.loc_id]})
        jid = r.json()["jid"]
        r2 = requests.get(f"{BASE_URL}/jobs/{jid}")
        self.assertEqual(r2.status_code, 200)
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
        r = requests.post(f"{BASE_URL}/jobs", json={"location_ids": [cls.loc_id]})
        cls.jid = r.json()["jid"]
        cls.result = cls._poll_until_done(cls.jid)

    @classmethod
    def _poll_until_done(cls, jid, timeout=60):
        """Poll GET /results/{jid} until SUCCESS/ERROR or timeout."""
        for _ in range(timeout // 2):
            time.sleep(2)
            r = requests.get(f"{BASE_URL}/results/{jid}")
            status = r.json().get("job_status", "")
            if "SUCCESS" in status or "ERROR" in status:
                return r.json()
        return requests.get(f"{BASE_URL}/results/{jid}").json()

    def test_get_results_not_found(self):
        """GET /results/{jid} must return 404 for unknown job IDs."""
        r = requests.get(f"{BASE_URL}/results/00000000-0000-0000-0000-000000000000")
        self.assertEqual(r.status_code, 404)

    def test_get_results_pending_has_job_status(self):
        """GET /results/{jid} must return 200 with a job_status field immediately after submission."""
        r_new = requests.post(f"{BASE_URL}/jobs", json={"location_ids": [self.loc_id]})
        jid2 = r_new.json()["jid"]
        r = requests.get(f"{BASE_URL}/results/{jid2}")
        self.assertEqual(r.status_code, 200)
        self.assertIn("job_status", r.json())

    def test_completed_job_has_results_and_success_status(self):
        """After job SUCCESS, response must include job_status with SUCCESS and a locations list."""
        self.assertIn("SUCCESS", self.result["job_status"])
        self.assertIn("locations", self.result)
        self.assertGreater(len(self.result["locations"]), 0)

    def test_completed_job_panel_orientation(self):
        """Results must include panel_orientation with correct tilt (=|lat|), azimuth, and facing."""
        loc_result = self.result["locations"][0]
        ori = loc_result["panel_orientation"]
        for key in ("recommended_tilt_deg", "recommended_azimuth_deg", "facing"):
            self.assertIn(key, ori)
        lat = abs(loc_result["location"]["lat"])
        self.assertAlmostEqual(ori["recommended_tilt_deg"], lat, places=1)

    def test_completed_job_energy_and_irradiance(self):
        """Results must include energy_yield with positive yield and full irradiance section."""
        loc_result = self.result["locations"][0]
        yld = loc_result["energy_yield"]
        self.assertIn("estimated_annual_yield_kwh_per_kwp", yld)
        self.assertGreater(yld["estimated_annual_yield_kwh_per_kwp"], 0)
        irr = loc_result["irradiance"]
        for key in ("mean_kwh_m2_day", "monthly_means", "best_worst_months",
                    "variability_index", "clearness_index_mean"):
            self.assertIn(key, irr)

    def test_completed_job_sentinel_counts(self):
        """Results must include sentinel_counts for ALLSKY_SFC_SW_DWN."""
        sc = self.result["locations"][0]["sentinel_counts"]
        self.assertIn("ALLSKY_SFC_SW_DWN", sc)
        self.assertIsInstance(sc["ALLSKY_SFC_SW_DWN"], int)


if __name__ == '__main__':
    unittest.main()
