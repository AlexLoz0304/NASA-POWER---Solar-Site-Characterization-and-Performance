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
    jid = r.json()[0]["jid"]
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

    def test_result_has_results_key(self):
        """Completed result must contain a 'results' dict."""
        self.assertIn("results", self.result)
        self.assertIsInstance(self.result["results"], dict)

    def test_result_has_location_info(self):
        """results.location must include lat, lon, name."""
        loc = self.result["results"]["location"]
        for field in ("lat", "lon", "name"):
            self.assertIn(field, loc)

    def test_result_has_panel_orientation(self):
        """results.panel_orientation must include tilt, azimuth, facing."""
        ori = self.result["results"]["panel_orientation"]
        for key in ("recommended_tilt_deg", "recommended_azimuth_deg", "facing"):
            self.assertIn(key, ori)

    def test_tilt_equals_abs_latitude(self):
        """Recommended tilt must equal |lat| of the stored location."""
        lat = abs(self.result["results"]["location"]["lat"])
        tilt = self.result["results"]["panel_orientation"]["recommended_tilt_deg"]
        self.assertAlmostEqual(tilt, lat, places=1)

    def test_northern_hemisphere_faces_south(self):
        """Paris (lat > 0) panels must face South."""
        facing = self.result["results"]["panel_orientation"]["facing"]
        self.assertEqual(facing, "South")

    def test_result_has_energy_yield(self):
        """results.energy_yield must include estimated_annual_yield_kwh_per_kwp."""
        yld = self.result["results"]["energy_yield"]
        self.assertIn("estimated_annual_yield_kwh_per_kwp", yld)
        self.assertIn("temp_derate_factor", yld)

    def test_energy_yield_is_positive(self):
        """Estimated annual yield must be a positive number."""
        yld = self.result["results"]["energy_yield"]["estimated_annual_yield_kwh_per_kwp"]
        self.assertGreater(yld, 0)

    def test_result_has_irradiance(self):
        """results.irradiance must include all required sub-keys."""
        irr = self.result["results"]["irradiance"]
        for key in ("mean_kwh_m2_day", "monthly_means", "best_worst_months",
                    "variability_index", "clearness_index_mean"):
            self.assertIn(key, irr)

    def test_irradiance_mean_is_positive(self):
        """Mean daily irradiance must be a positive number."""
        mean = self.result["results"]["irradiance"]["mean_kwh_m2_day"]
        self.assertGreater(mean, 0)

    def test_monthly_means_has_12_entries(self):
        """monthly_means must cover all calendar months in the data range (at least 12)."""
        monthly = self.result["results"]["irradiance"]["monthly_means"]
        self.assertGreaterEqual(len(monthly), 12)

    def test_monthly_means_null_for_sentinel_only_months(self):
        """Months where all data is sentinel must be null, not omitted."""
        monthly = self.result["results"]["irradiance"]["monthly_means"]
        # Every entry must be either a positive float or null
        for label, val in monthly.items():
            if val is not None:
                self.assertGreater(val, 0, msg=f"Month {label} has non-positive mean {val}")

    def test_result_has_parameter_stats(self):
        """results.parameter_stats must cover all 10 NASA parameters."""
        stats = self.result["results"]["parameter_stats"]
        for param in NASA_PARAMS:
            self.assertIn(param, stats)

    def test_parameter_stats_have_required_keys(self):
        """Each parameter stats entry must include mean, median, std, min, max, count."""
        stats = self.result["results"]["parameter_stats"]["ALLSKY_SFC_SW_DWN"]
        for key in ("mean", "median", "std", "min", "max", "count"):
            self.assertIn(key, stats)

    def test_result_has_temperature(self):
        """results.temperature must be present."""
        self.assertIn("temperature", self.result["results"])

    def test_result_has_wind(self):
        """results.wind must be present."""
        self.assertIn("wind", self.result["results"])

    def test_result_has_humidity(self):
        """results.humidity must be present."""
        self.assertIn("humidity", self.result["results"])

    def test_result_has_cloud_cover(self):
        """results.cloud_cover must be present."""
        self.assertIn("cloud_cover", self.result["results"])

    def test_result_has_precipitation(self):
        """results.precipitation must be present."""
        self.assertIn("precipitation", self.result["results"])

    def test_result_has_sentinel_counts(self):
        """results.sentinel_counts must map each parameter to an int >= 0."""
        sc = self.result["results"]["sentinel_counts"]
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
        """Computed means must not equal the sentinel value -999."""
        stats = self.result["results"]["parameter_stats"]
        for param in NASA_PARAMS:
            mean = stats[param]["mean"]
            self.assertNotAlmostEqual(mean, SENTINEL, places=0,
                                      msg=f"{param} mean is sentinel!")

    def test_parameter_mins_are_not_sentinel(self):
        """Computed minimums must not equal the sentinel value -999."""
        stats = self.result["results"]["parameter_stats"]
        for param in NASA_PARAMS:
            minimum = stats[param]["min"]
            self.assertNotAlmostEqual(minimum, SENTINEL, places=0,
                                      msg=f"{param} min is sentinel!")

    def test_irradiance_mean_not_sentinel(self):
        """Irradiance mean must not be the sentinel value."""
        mean = self.result["results"]["irradiance"]["mean_kwh_m2_day"]
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
        facing = self.result["results"]["panel_orientation"]["facing"]
        self.assertEqual(facing, "North")

    def test_tilt_equals_abs_latitude(self):
        """Recommended tilt must equal |lat| for southern-hemisphere location."""
        lat = abs(self.result["results"]["location"]["lat"])
        tilt = self.result["results"]["panel_orientation"]["recommended_tilt_deg"]
        self.assertAlmostEqual(tilt, lat, places=1)


class TestWorkerMultipleJobs(unittest.TestCase):
    """Worker must handle multiple concurrent queued jobs."""

    @classmethod
    def setUpClass(cls):
        _wait_for_api()
        r1 = requests.post(f"{BASE_URL}/locations", json=LOCATION_PARIS)
        r2 = requests.post(f"{BASE_URL}/locations", json=LOCATION_SYDNEY)
        cls.loc_id1 = r1.json()["id"]
        cls.loc_id2 = r2.json()["id"]

        r = requests.post(f"{BASE_URL}/jobs",
                          json={"location_ids": [cls.loc_id1, cls.loc_id2]})
        cls.jids = [job["jid"] for job in r.json()]

    @classmethod
    def tearDownClass(cls):
        for lid in (cls.loc_id1, cls.loc_id2):
            requests.delete(f"{BASE_URL}/locations/{lid}")

    def _wait_all(self, timeout=90):
        results = {}
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(2)
            all_done = True
            for jid in self.jids:
                r = requests.get(f"{BASE_URL}/results/{jid}")
                status = r.json().get("job_status", "")
                if "SUCCESS" in status or "ERROR" in status:
                    results[jid] = r.json()
                else:
                    all_done = False
            if all_done:
                break
        return results

    def test_both_jobs_complete(self):
        """Both submitted jobs must reach SUCCESS or ERROR within timeout."""
        results = self._wait_all()
        self.assertEqual(len(results), 2)

    def test_both_jobs_succeed(self):
        """Both jobs must reach FINISHED -- SUCCESS."""
        results = self._wait_all()
        for jid, res in results.items():
            self.assertIn("SUCCESS", res["job_status"],
                          msg=f"Job {jid} did not succeed: {res.get('job_status')}")

    def test_results_are_independent(self):
        """Each job must produce results for its own location."""
        results = self._wait_all()
        lats = [res["results"]["location"]["lat"] for res in results.values()]
        # Paris ~48.5, Sydney ~-34.0 — they must differ
        self.assertNotEqual(lats[0], lats[1])


if __name__ == '__main__':
    unittest.main()
