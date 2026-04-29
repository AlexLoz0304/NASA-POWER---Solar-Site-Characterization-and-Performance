"""
test_api_unit.py — Unit tests for FastAPI endpoints (no live Redis or NASA POWER required).

Covers:
  (a) Invalid coordinate and date rejection via POST /locations
  (b) Mocked NASA POWER call returning expected parsed output
  (c) Job submission and status polling with mocked Redis/job functions

Uses FastAPI TestClient (httpx-backed) and unittest.mock for isolation.

Run with:
    uv run pytest test/test_api_unit.py -v
"""

import json
import sys
import pytest
from unittest.mock import Mock, MagicMock, patch

sys.path.insert(0, '/home/ubuntu/NASA-POWER---Solar-Site-Characterization-and-Performance/src')

from fastapi.testclient import TestClient

import FastAPI_api
from FastAPI_api import app, NASA_PARAMETERS

client = TestClient(app)

# ---------------------------------------------------------------------------
# (a) Input validation — coordinate and date bounds
# ---------------------------------------------------------------------------

class TestCoordinateValidation:
    """POST /locations must reject out-of-range lat/lon with HTTP 422."""

    def test_latitude_above_90_rejected(self):
        r = client.post("/locations", json={"lat": 91.0, "lon": 0.0})
        assert r.status_code == 422

    def test_latitude_below_minus90_rejected(self):
        r = client.post("/locations", json={"lat": -91.0, "lon": 0.0})
        assert r.status_code == 422

    def test_longitude_above_180_rejected(self):
        r = client.post("/locations", json={"lat": 0.0, "lon": 181.0})
        assert r.status_code == 422

    def test_longitude_below_minus180_rejected(self):
        r = client.post("/locations", json={"lat": 0.0, "lon": -181.0})
        assert r.status_code == 422

    def test_boundary_values_accepted_by_validator(self):
        """Exact boundary values (±90 lat, ±180 lon) pass field validation."""
        # We patch NASA and Redis so we only test Pydantic validation, not full flow
        mock_params = {p: {"20250101": 4.5} for p in NASA_PARAMETERS}
        with patch.object(FastAPI_api, '_fetch_point_from_nasa', return_value=mock_params), \
             patch('FastAPI_api.rd') as mock_rd:
            mock_rd.sadd = Mock(); mock_rd.hset = Mock(); mock_rd.set = Mock()
            r = client.post("/locations", json={"lat": 90.0, "lon": 180.0,
                                                "start_date": "20250101", "end_date": "20250201"})
        assert r.status_code == 201


class TestDateValidation:
    """POST /locations must reject malformed dates and reversed ranges."""

    def test_invalid_date_format_rejected(self):
        r = client.post("/locations", json={
            "lat": 34.0, "lon": -118.0,
            "start_date": "01-01-2025", "end_date": "20260101",
        })
        assert r.status_code == 422

    def test_start_after_end_rejected(self):
        r = client.post("/locations", json={
            "lat": 34.0, "lon": -118.0,
            "start_date": "20260101", "end_date": "20250101",
        })
        assert r.status_code == 422

    def test_start_equal_end_rejected(self):
        r = client.post("/locations", json={
            "lat": 34.0, "lon": -118.0,
            "start_date": "20250101", "end_date": "20250101",
        })
        assert r.status_code == 422

    def test_future_end_date_rejected(self):
        r = client.post("/locations", json={
            "lat": 34.0, "lon": -118.0,
            "start_date": "20250101", "end_date": "20991231",
        })
        assert r.status_code == 422

    def test_valid_past_date_range_accepted(self):
        mock_params = {p: {"20250101": 4.5} for p in NASA_PARAMETERS}
        with patch.object(FastAPI_api, '_fetch_point_from_nasa', return_value=mock_params), \
             patch('FastAPI_api.rd') as mock_rd:
            mock_rd.sadd = Mock(); mock_rd.hset = Mock(); mock_rd.set = Mock()
            r = client.post("/locations", json={
                "lat": 34.0, "lon": -118.0,
                "start_date": "20250101", "end_date": "20250201",
            })
        assert r.status_code == 201


# ---------------------------------------------------------------------------
# (b) Mocked NASA POWER call — response parsed into Location model
# ---------------------------------------------------------------------------

MOCK_NASA_RESPONSE = {
    "ALLSKY_SFC_SW_DWN": {"20250101": 4.5, "20250102": 5.0, "20250103": -999.0},
    "CLRSKY_SFC_SW_DWN": {"20250101": 6.0, "20250102": 6.2, "20250103": 6.1},
    "ALLSKY_KT":         {"20250101": 0.75, "20250102": 0.81, "20250103": 0.0},
    "T2M":               {"20250101": 15.0, "20250102": 16.0, "20250103": 14.5},
    "T2M_MAX":           {"20250101": 20.0, "20250102": 21.0, "20250103": 19.5},
    "T2M_MIN":           {"20250101": 10.0, "20250102": 11.0, "20250103": 9.5},
    "WS10M":             {"20250101": 3.0,  "20250102": 2.5,  "20250103": 4.0},
    "RH2M":              {"20250101": 60.0, "20250102": 55.0, "20250103": 65.0},
    "PRECTOTCORR":       {"20250101": 0.0,  "20250102": 2.5,  "20250103": 0.0},
    "CLOUD_AMT":         {"20250101": 25.0, "20250102": 20.0, "20250103": 30.0},
}


class TestMockedNASACall:
    """POST /locations with a mocked NASA POWER response parses output correctly."""

    def _post_with_mocked_nasa(self, nasa_response=None):
        if nasa_response is None:
            nasa_response = MOCK_NASA_RESPONSE
        with patch.object(FastAPI_api, '_fetch_point_from_nasa', return_value=nasa_response), \
             patch('FastAPI_api.rd') as mock_rd:
            mock_rd.sadd = Mock(); mock_rd.hset = Mock(); mock_rd.set = Mock()
            return client.post("/locations", json={
                "lat": 34.0, "lon": -118.0, "name": "TestLA",
                "start_date": "20250101", "end_date": "20250104",
            })

    def test_returns_201(self):
        assert self._post_with_mocked_nasa().status_code == 201

    def test_response_contains_all_parameters(self):
        data = self._post_with_mocked_nasa().json()
        for param in NASA_PARAMETERS:
            assert param in data, f"Missing parameter: {param}"

    def test_parameter_data_is_list_of_floats(self):
        data = self._post_with_mocked_nasa().json()
        assert isinstance(data["ALLSKY_SFC_SW_DWN"], list)
        assert all(isinstance(v, float) for v in data["ALLSKY_SFC_SW_DWN"])

    def test_sentinel_value_preserved_in_stored_series(self):
        """Sentinel -999 is kept as-is in the raw stored series (not filtered here)."""
        data = self._post_with_mocked_nasa().json()
        assert -999.0 in data["ALLSKY_SFC_SW_DWN"]

    def test_series_ordered_by_date(self):
        """Values must be in ascending date order."""
        data = self._post_with_mocked_nasa().json()
        ghi = data["ALLSKY_SFC_SW_DWN"]
        assert ghi[0] == 4.5   # 20250101
        assert ghi[1] == 5.0   # 20250102
        assert ghi[2] == -999.0  # 20250103

    def test_coordinates_snapped_to_grid(self):
        data = self._post_with_mocked_nasa().json()
        assert data["lat"] == 34.0
        assert data["lon"] == -118.0

    def test_nasa_timeout_returns_502(self):
        import requests as req_lib
        with patch.object(FastAPI_api, '_fetch_point_from_nasa',
                          side_effect=req_lib.Timeout("timeout")):
            r = client.post("/locations", json={
                "lat": 34.0, "lon": -118.0,
                "start_date": "20250101", "end_date": "20250201",
            })
        assert r.status_code == 502

    def test_nasa_http_error_returns_502(self):
        import requests as req_lib
        with patch.object(FastAPI_api, '_fetch_point_from_nasa',
                          side_effect=req_lib.HTTPError("404 Not Found")):
            r = client.post("/locations", json={
                "lat": 34.0, "lon": -118.0,
                "start_date": "20250101", "end_date": "20250201",
            })
        assert r.status_code == 502


# ---------------------------------------------------------------------------
# (c) Job submission and status polling
# ---------------------------------------------------------------------------

class TestJobSubmissionAndPolling:
    """POST /jobs and GET /jobs/{jid} with fully mocked Redis and job functions."""

    _FAKE_LOC = {
        "id": "loc-uuid-1234",
        "key": "location:34.0:-118.0:loc-uuid-1234",
        "lat": 34.0, "lon": -118.0,
        "start_date": "20250101", "end_date": "20260101",
        "name": "TestLA",
        **{p: [4.5, 5.0, 4.8] for p in NASA_PARAMETERS},
    }

    def _make_fake_job(self, status="QUEUED"):
        from jobs import Job, JobStatus
        status_map = {
            "QUEUED": JobStatus.QUEUED,
            "RUNNING": JobStatus.RUNNING,
            "FINISHED -- SUCCESS": JobStatus.SUCCESS,
            "FINISHED -- ERROR": JobStatus.ERROR,
        }
        return Job(
            jid="job-uuid-5678",
            status=status_map.get(status, JobStatus.QUEUED),
            location_id="loc-uuid-1234",
            lat=34.0, lon=-118.0,
            start_date="20250101", end_date="20260101",
        )

    def test_post_jobs_returns_200_list(self):
        with patch('FastAPI_api.rd') as mock_rd, \
             patch('FastAPI_api.add_job', return_value=self._make_fake_job()), \
             patch('FastAPI_api._read_location_hash', return_value=self._FAKE_LOC):
            mock_rd.get = Mock(return_value=b"location:34.0:-118.0:loc-uuid-1234")
            r = client.post("/jobs", json={"location_ids": ["loc-uuid-1234"]})
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_post_jobs_job_is_queued(self):
        with patch('FastAPI_api.rd') as mock_rd, \
             patch('FastAPI_api.add_job', return_value=self._make_fake_job()), \
             patch('FastAPI_api._read_location_hash', return_value=self._FAKE_LOC):
            mock_rd.get = Mock(return_value=b"location:34.0:-118.0:loc-uuid-1234")
            r = client.post("/jobs", json={"location_ids": ["loc-uuid-1234"]})
        job = r.json()[0]
        assert job["status"] == "QUEUED"
        assert job["jid"] == "job-uuid-5678"

    def test_post_jobs_unknown_location_returns_404(self):
        with patch('FastAPI_api.rd') as mock_rd:
            mock_rd.get = Mock(return_value=None)
            r = client.post("/jobs", json={"location_ids": ["no-such-location"]})
        assert r.status_code == 404

    def test_post_jobs_empty_list_returns_422(self):
        r = client.post("/jobs", json={"location_ids": []})
        assert r.status_code == 422

    def test_get_job_by_id_returns_job(self):
        job = self._make_fake_job("RUNNING")
        with patch('FastAPI_api.get_job_by_id', return_value=job):
            r = client.get("/jobs/job-uuid-5678")
        assert r.status_code == 200
        data = r.json()
        assert data["jid"] == "job-uuid-5678"
        assert data["status"] == "RUNNING"

    def test_get_job_unknown_id_returns_404(self):
        with patch('FastAPI_api.get_job_by_id', side_effect=ValueError("not found")):
            r = client.get("/jobs/does-not-exist")
        assert r.status_code == 404

    def test_get_results_queued_job_returns_status(self):
        job = self._make_fake_job("QUEUED")
        with patch('FastAPI_api.get_job_by_id', return_value=job):
            r = client.get("/results/job-uuid-5678")
        assert r.status_code == 200
        assert r.json()["job_status"] == "QUEUED"

    def test_get_results_unknown_job_returns_404(self):
        with patch('FastAPI_api.get_job_by_id', side_effect=Exception("not found")):
            r = client.get("/results/no-such-job")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    """GET /health reports Redis connectivity without requiring a live server."""

    def test_health_ok_when_redis_up(self):
        with patch('FastAPI_api.rd') as mock_rd:
            mock_rd.ping = Mock(return_value=True)
            r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["redis"] == "ok"

    def test_health_degraded_when_redis_down(self):
        with patch('FastAPI_api.rd') as mock_rd:
            mock_rd.ping = Mock(side_effect=Exception("connection refused"))
            r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "degraded"
        assert data["redis"] == "error"

    def test_health_includes_nasa_url(self):
        with patch('FastAPI_api.rd') as mock_rd:
            mock_rd.ping = Mock(return_value=True)
            r = client.get("/health")
        assert "nasa_power_url" in r.json()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
