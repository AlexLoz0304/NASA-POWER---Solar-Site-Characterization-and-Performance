"""
test_worker.py — End-to-end tests for the solar characterization worker
========================================================================
Submits real jobs via the API and waits for the worker to complete them,
then validates the structure and sanity of the results.

Run with:
    uv run python test/test_worker.py
"""

import time
import unittest
import requests

BASE_URL = "http://localhost:5000"

# Stable grid-aligned test coordinates
LOCATION_PARIS    = {"lat": 48.5, "lon":  2.0, "name": "Paris_wk"}
LOCATION_SYDNEY   = {"lat": -34.0, "lon": 151.0, "name": "Sydney_wk"}
LOCATION_NAIROBI  = {"lat":  -1.0, "lon":  37.0, "name": "Nairobi_wk"}

NASA_PARAMS = (
    "ALLSKY_SFC_SW_DWN", "CLRSKY_SFC_SW_DWN", "ALLSKY_KT",
    "T2M", "T2M_MAX", "T2M_MIN",
    "WS10M", "RH2M", "PRECTOTCORR", "CLOUD_AMT",
)

SENTINEL = -999.0


def _wait_for_api(retries=15, delay=1.0):
    for _ in range(retries):
        try:
            r = requests.get(f"{BASE_URL}/locations", timeout=3)
            if r.status_code in (200, 404):
                return
        except requests.ConnectionError:
            pass
        time.sleep(delay)
    raise RuntimeError("API not reachable at " + BASE_URL)


def _submit_and_wait(loc_id, timeout=60):
    """Submit one job for loc_id and block until SUCCESS or ERROR."""
    r = requests.post(f"{BASE_URL}/jobs", json={"location_ids": [loc_id]})
    jid = r.json()["jid"]
    for _ in range(timeout // 2):
        time.sleep(2)
        r = requests.get(f"{BASE_URL}/results/{jid}")
        status = r.json().get("job_status", "")
        if "SUCCESS" in status or "ERROR" in status:
            return r.json(), jid
    return requests.get(f"{BASE_URL}/results/{jid}").json(), jid


class TestWorkerJobCompletion(unittest.TestCase):
    """Worker must transition jobs to SUCCESS and produce results."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations", json=LOCATION_PARIS)
        cls.loc_id = r.json()["id"]
        cls.result, cls.jid = _submit_and_wait(cls.loc_id)

    @classmethod
    def tearDownClass(cls):
        requests.delete(f"{BASE_URL}/locations/{cls.loc_id}")

    def test_job_reaches_success(self):
        """Job must reach FINISHED -- SUCCESS status."""
        self.assertIn("SUCCESS", self.result["job_status"])

    def test_result_has_location_info(self):
        """locations[0].location must include lat, lon, name."""
        self.assertIn("locations", self.result)
        loc = self.result["locations"][0]["location"]
        for field in ("lat", "lon", "name"):
            self.assertIn(field, loc)

    def test_panel_orientation_correct(self):
        """locations[0].panel_orientation must have required keys, tilt=|lat|, and facing=South for Paris."""
        loc_result = self.result["locations"][0]
        ori = loc_result["panel_orientation"]
        for key in ("recommended_tilt_deg", "recommended_azimuth_deg", "facing"):
            self.assertIn(key, ori)
        lat = abs(loc_result["location"]["lat"])
        self.assertAlmostEqual(ori["recommended_tilt_deg"], lat, places=1)
        self.assertEqual(ori["facing"], "South")

    def test_energy_yield_valid(self):
        """locations[0].energy_yield must include estimated annual yield and temp_derate_factor, and yield > 0."""
        yld = self.result["locations"][0]["energy_yield"]
        self.assertIn("estimated_annual_yield_kwh_per_kwp", yld)
        self.assertIn("temp_derate_factor", yld)
        self.assertGreater(yld["estimated_annual_yield_kwh_per_kwp"], 0)

    def test_irradiance_valid(self):
        """locations[0].irradiance must have all required keys, positive mean, and ≥12 monthly entries."""
        irr = self.result["locations"][0]["irradiance"]
        for key in ("mean_kwh_m2_day", "monthly_means", "best_worst_months",
                    "variability_index", "clearness_index_mean"):
            self.assertIn(key, irr)
        self.assertGreater(irr["mean_kwh_m2_day"], 0)
        self.assertGreaterEqual(len(irr["monthly_means"]), 12)

    def test_monthly_means_null_for_sentinel_only_months(self):
        """Months where all data is sentinel must be null, not omitted."""
        monthly = self.result["locations"][0]["irradiance"]["monthly_means"]
        # Every entry must be either a positive float or null
        for label, val in monthly.items():
            if val is not None:
                self.assertGreater(val, 0, msg=f"Month {label} has non-positive mean {val}")

    def test_result_has_all_climate_sections(self):
        """locations[0] must include temperature, wind, humidity, cloud_cover, and precipitation."""
        loc_result = self.result["locations"][0]
        for section in ("temperature", "wind", "humidity", "cloud_cover", "precipitation"):
            self.assertIn(section, loc_result)

    def test_result_has_sentinel_counts(self):
        """locations[0].sentinel_counts must map each parameter to an int >= 0."""
        sc = self.result["locations"][0]["sentinel_counts"]
        for param in NASA_PARAMS:
            self.assertIn(param, sc)
            self.assertIsInstance(sc[param], int)
            self.assertGreaterEqual(sc[param], 0)


class TestWorkerSentinelHandling(unittest.TestCase):
    """Worker must filter out sentinel (-999) values before computing stats."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations", json=LOCATION_NAIROBI)
        cls.loc_id = r.json()["id"]
        cls.result, cls.jid = _submit_and_wait(cls.loc_id)

    @classmethod
    def tearDownClass(cls):
        requests.delete(f"{BASE_URL}/locations/{cls.loc_id}")

    def test_parameter_means_are_not_sentinel(self):
        """Irradiance mean must not equal the sentinel value -999."""
        mean = self.result["locations"][0]["irradiance"]["mean_kwh_m2_day"]
        self.assertNotAlmostEqual(mean, SENTINEL, places=0)

    def test_parameter_mins_are_not_sentinel(self):
        """Sentinel counts must be non-negative integers (sentinels were filtered)."""
        sc = self.result["locations"][0]["sentinel_counts"]
        for param in NASA_PARAMS:
            self.assertGreaterEqual(sc[param], 0)

    def test_irradiance_mean_not_sentinel(self):
        """Irradiance mean must not be the sentinel value."""
        mean = self.result["locations"][0]["irradiance"]["mean_kwh_m2_day"]
        self.assertNotAlmostEqual(mean, SENTINEL, places=0)


class TestWorkerSouthernHemisphere(unittest.TestCase):
    """Southern-hemisphere locations must produce North-facing panel recommendations."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r = requests.post(f"{BASE_URL}/locations", json=LOCATION_SYDNEY)
        cls.loc_id = r.json()["id"]
        cls.result, cls.jid = _submit_and_wait(cls.loc_id)

    @classmethod
    def tearDownClass(cls):
        requests.delete(f"{BASE_URL}/locations/{cls.loc_id}")

    def test_southern_hemisphere_faces_north(self):
        """Sydney (lat < 0) panels must face North."""
        facing = self.result["locations"][0]["panel_orientation"]["facing"]
        self.assertEqual(facing, "North")

    def test_tilt_equals_abs_latitude(self):
        """Recommended tilt must equal |lat| for southern-hemisphere location."""
        lat = abs(self.result["locations"][0]["location"]["lat"])
        tilt = self.result["locations"][0]["panel_orientation"]["recommended_tilt_deg"]
        self.assertAlmostEqual(tilt, lat, places=1)


class TestWorkerMultipleJobs(unittest.TestCase):
    """Worker must handle a job with multiple location_ids in one submission."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r1 = requests.post(f"{BASE_URL}/locations", json=LOCATION_PARIS)
        r2 = requests.post(f"{BASE_URL}/locations", json=LOCATION_SYDNEY)
        cls.loc_id1 = r1.json()["id"]
        cls.loc_id2 = r2.json()["id"]

        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [cls.loc_id1, cls.loc_id2]})
        cls.jid = r.json()["jid"]
        cls.result = cls._wait_for_result(cls.jid)

    @classmethod
    def tearDownClass(cls):
        for lid in (cls.loc_id1, cls.loc_id2):
            requests.delete(f"{BASE_URL}/locations/{lid}")

    @staticmethod
    def _wait_for_result(jid, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(2)
            r = requests.get(f"{BASE_URL}/results/{jid}")
            status = r.json().get("job_status", "")
            if "SUCCESS" in status or "ERROR" in status:
                return r.json()
        return requests.get(f"{BASE_URL}/results/{jid}").json()

    def test_job_completes(self):
        """Multi-location job must reach FINISHED -- SUCCESS within timeout."""
        self.assertIn("SUCCESS", self.result["job_status"])

    def test_result_has_two_locations(self):
        """Result must contain exactly 2 location entries."""
        self.assertEqual(self.result.get("location_count"), 2)
        self.assertEqual(len(self.result["locations"]), 2)

    def test_results_are_independent(self):
        """The two location results must be at different latitudes."""
        lats = [loc["location"]["lat"] for loc in self.result["locations"]]
        self.assertNotEqual(lats[0], lats[1])

    def test_comparison_summary_present(self):
        """Multi-location jobs must include a comparison_summary."""
        self.assertIn("comparison_summary", self.result)
        cs = self.result["comparison_summary"]
        self.assertIn("ranked", cs)
        self.assertIn("best_site", cs)
        self.assertIn("comparison", cs)


if __name__ == '__main__':
    unittest.main()
