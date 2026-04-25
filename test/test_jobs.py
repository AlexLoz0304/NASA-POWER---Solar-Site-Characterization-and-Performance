"""
test_jobs.py — Unit tests for jobs.py
======================================
Tests all job queue functions in isolation using unittest.mock to patch
Redis dependencies (jdb, rdb, q). No live Redis connection is required.

Run with:
    uv run python test/test_jobs.py
"""

import json
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

sys.path.insert(0, '/home/ubuntu/NASA-POWER---Solar-Site-Characterization-and-Performance/src')

from jobs import (
    Job,
    JobStatus,
    _generate_jid,
    _instantiate_job,
    _queue_job,
    _save_job,
    add_job,
    get_job_by_id,
    get_job_result,
    save_job_result,
    start_job,
    update_job_status,
)


class TestJobIDGeneration(unittest.TestCase):
    """Tests for _generate_jid — UUID factory."""

    def test_generate_jid_returns_string(self):
        """_generate_jid must return a string."""
        jid = _generate_jid()
        self.assertIsInstance(jid, str)

    def test_generate_jid_is_non_empty(self):
        """_generate_jid must return a non-empty string."""
        self.assertTrue(len(_generate_jid()) > 0)

    def test_generate_jid_returns_unique_ids(self):
        """Two consecutive calls must produce different UUIDs."""
        self.assertNotEqual(_generate_jid(), _generate_jid())

    def test_generate_jid_looks_like_uuid(self):
        """UUID string should contain exactly 4 hyphens (UUID4 format)."""
        jid = _generate_jid()
        self.assertEqual(jid.count('-'), 4)


class TestJobInstantiation(unittest.TestCase):
    """Tests for _instantiate_job — pure Job model factory."""

    def _make_job(self, status=JobStatus.QUEUED, job_type="point",
                  lat=34.0, lon=-118.0, start="20250415", end="20260415"):
        jid = _generate_jid()
        return jid, _instantiate_job(jid, status, job_type, lat, lon, start, end)

    def test_returns_job_instance(self):
        """_instantiate_job must return a Job Pydantic model."""
        _, job = self._make_job()
        self.assertIsInstance(job, Job)

    def test_jid_is_preserved(self):
        """Job.jid must equal the jid passed in."""
        jid, job = self._make_job()
        self.assertEqual(job.jid, jid)

    def test_status_is_set(self):
        """Job.status must reflect the status argument."""
        _, job = self._make_job(status=JobStatus.QUEUED)
        self.assertEqual(job.status, JobStatus.QUEUED)

    def test_job_type_is_set(self):
        """Job.job_type must reflect the job_type argument."""
        _, job = self._make_job(job_type="point")
        self.assertEqual(job.job_type, "point")

    def test_lat_lon_are_set(self):
        """Job.lat and Job.lon must reflect the coordinate arguments."""
        _, job = self._make_job(lat=40.5, lon=-74.0)
        self.assertEqual(job.lat, 40.5)
        self.assertEqual(job.lon, -74.0)

    def test_date_range_is_set(self):
        """Job.start_date and Job.end_date must reflect the date arguments."""
        _, job = self._make_job(start="20250101", end="20260101")
        self.assertEqual(job.start_date, "20250101")
        self.assertEqual(job.end_date, "20260101")

    def test_timestamps_are_none_on_creation(self):
        """start_time and end_time must be None for a freshly instantiated job."""
        _, job = self._make_job()
        self.assertIsNone(job.start_time)
        self.assertIsNone(job.end_time)

    def test_all_job_statuses_accepted(self):
        """_instantiate_job must accept every JobStatus value."""
        for status in JobStatus:
            _, job = self._make_job(status=status)
            self.assertEqual(job.status, status)


class TestJobPersistence(unittest.TestCase):
    """Tests for _save_job and _queue_job — Redis write helpers."""

    @patch('jobs.jdb')
    def test_save_job_calls_redis_set(self, mock_jdb):
        """_save_job must call jdb.set with the jid as key."""
        mock_jdb.set = Mock(return_value=True)
        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, "point", 34.0, -118.0, "20250415", "20260415")

        result = _save_job(jid, job)

        self.assertTrue(result)
        mock_jdb.set.assert_called_once()
        call_key = mock_jdb.set.call_args[0][0]
        self.assertEqual(call_key, jid)

    @patch('jobs.jdb')
    def test_save_job_serialises_status(self, mock_jdb):
        """_save_job must serialise the job status as a JSON string value."""
        mock_jdb.set = Mock(return_value=True)
        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, "point", 34.0, -118.0, "20250415", "20260415")

        _save_job(jid, job)

        stored_json = mock_jdb.set.call_args[0][1]
        stored = json.loads(stored_json)
        self.assertEqual(stored['status'], JobStatus.QUEUED.value)

    @patch('jobs.jdb')
    def test_save_job_serialises_lat_lon(self, mock_jdb):
        """_save_job must include lat and lon in the serialised payload."""
        mock_jdb.set = Mock(return_value=True)
        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, "point", 26.0, -80.0, "20250415", "20260415")

        _save_job(jid, job)

        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['lat'], 26.0)
        self.assertEqual(stored['lon'], -80.0)

    @patch('jobs.q')
    def test_queue_job_calls_put(self, mock_q):
        """_queue_job must call q.put with the jid."""
        mock_q.put = Mock()
        jid = _generate_jid()

        result = _queue_job(jid)

        self.assertTrue(result)
        mock_q.put.assert_called_once_with(jid)


class TestGetJobById(unittest.TestCase):
    """Tests for get_job_by_id — deserialisation from Redis."""

    def _make_raw(self, jid, status=JobStatus.QUEUED,
                  lat=34.0, lon=-118.0,
                  start_date="20250415", end_date="20260415"):
        return json.dumps({
            'jid': jid,
            'status': status.value,
            'job_type': 'point',
            'lat': lat,
            'lon': lon,
            'start_date': start_date,
            'end_date': end_date,
            'start_time': None,
            'end_time': None,
        }).encode('utf-8')

    @patch('jobs.jdb')
    def test_retrieves_correct_jid(self, mock_jdb):
        """get_job_by_id must return a Job whose jid matches the lookup key."""
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid))

        job = get_job_by_id(jid)

        self.assertEqual(job.jid, jid)
        mock_jdb.get.assert_called_once_with(jid)

    @patch('jobs.jdb')
    def test_retrieves_correct_status(self, mock_jdb):
        """get_job_by_id must deserialise the status enum correctly."""
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid, status=JobStatus.RUNNING))

        job = get_job_by_id(jid)

        self.assertEqual(job.status, JobStatus.RUNNING)

    @patch('jobs.jdb')
    def test_retrieves_lat_lon(self, mock_jdb):
        """get_job_by_id must deserialise lat/lon correctly."""
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid, lat=40.5, lon=-74.0))

        job = get_job_by_id(jid)

        self.assertEqual(job.lat, 40.5)
        self.assertEqual(job.lon, -74.0)

    @patch('jobs.jdb')
    def test_raises_on_missing_job(self, mock_jdb):
        """get_job_by_id must raise ValueError when key is absent."""
        mock_jdb.get = Mock(return_value=None)

        with self.assertRaises(Exception):
            get_job_by_id("nonexistent-jid")


class TestJobLifecycle(unittest.TestCase):
    """Tests for start_job and update_job_status — state transitions."""

    def _queued_raw(self, jid):
        return json.dumps({
            'jid': jid, 'status': JobStatus.QUEUED.value,
            'job_type': 'point', 'lat': 34.0, 'lon': -118.0,
            'start_date': '20250415', 'end_date': '20260415',
            'start_time': None, 'end_time': None,
        }).encode()

    @patch('jobs.jdb')
    def test_start_job_sets_running(self, mock_jdb):
        """start_job must transition a QUEUED job to RUNNING."""
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._queued_raw(jid))
        mock_jdb.set = Mock(return_value=True)

        job = start_job(jid)

        self.assertEqual(job.status, JobStatus.RUNNING)

    @patch('jobs.jdb')
    def test_start_job_sets_start_time(self, mock_jdb):
        """start_job must set start_time to a non-None datetime."""
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._queued_raw(jid))
        mock_jdb.set = Mock(return_value=True)

        job = start_job(jid)

        self.assertIsNotNone(job.start_time)
        self.assertIsInstance(job.start_time, datetime)

    @patch('jobs.jdb')
    def test_update_job_status_to_success(self, mock_jdb):
        """update_job_status must transition a job to SUCCESS and set end_time."""
        jid = _generate_jid()
        running_raw = json.dumps({
            'jid': jid, 'status': JobStatus.RUNNING.value,
            'job_type': 'point', 'lat': 34.0, 'lon': -118.0,
            'start_date': '20250415', 'end_date': '20260415',
            'start_time': datetime.now(timezone.utc).isoformat(), 'end_time': None,
        }).encode()
        mock_jdb.get = Mock(return_value=running_raw)
        mock_jdb.set = Mock(return_value=True)

        update_job_status(jid, JobStatus.SUCCESS)

        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['status'], JobStatus.SUCCESS.value)
        self.assertIsNotNone(stored['end_time'])

    @patch('jobs.jdb')
    def test_update_job_status_to_error(self, mock_jdb):
        """update_job_status must accept ERROR and set end_time."""
        jid = _generate_jid()
        running_raw = json.dumps({
            'jid': jid, 'status': JobStatus.RUNNING.value,
            'job_type': 'point', 'lat': 34.0, 'lon': -118.0,
            'start_date': '20250415', 'end_date': '20260415',
            'start_time': datetime.now(timezone.utc).isoformat(), 'end_time': None,
        }).encode()
        mock_jdb.get = Mock(return_value=running_raw)
        mock_jdb.set = Mock(return_value=True)

        update_job_status(jid, JobStatus.ERROR)

        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['status'], JobStatus.ERROR.value)
        self.assertIsNotNone(stored['end_time'])


class TestJobResults(unittest.TestCase):
    """Tests for save_job_result and get_job_result — db=3 helpers."""

    @patch('jobs.rdb')
    def test_save_job_result_stores_dict(self, mock_rdb):
        """save_job_result must serialise the result dict to db=3."""
        mock_rdb.set = Mock(return_value=True)
        jid = _generate_jid()
        result = {
            'irradiance': {'mean_kwh_m2_day': 5.2},
            'energy_yield': {'estimated_annual_yield_kwh_per_kwp': 1150.0},
        }

        ok = save_job_result(jid, result)

        self.assertTrue(ok)
        stored = json.loads(mock_rdb.set.call_args[0][1])
        self.assertAlmostEqual(stored['irradiance']['mean_kwh_m2_day'], 5.2)

    @patch('jobs.rdb')
    def test_save_job_result_returns_false_on_exception(self, mock_rdb):
        """save_job_result must return False (not raise) when Redis errors."""
        mock_rdb.set = Mock(side_effect=Exception("Redis down"))

        ok = save_job_result(_generate_jid(), {'data': 'test'})

        self.assertFalse(ok)

    @patch('jobs.rdb')
    def test_get_job_result_deserialises(self, mock_rdb):
        """get_job_result must decode and return the stored dict."""
        jid = _generate_jid()
        payload = {'irradiance': {'mean_kwh_m2_day': 4.9}}
        mock_rdb.get = Mock(return_value=json.dumps(payload).encode())

        retrieved = get_job_result(jid)

        self.assertIsNotNone(retrieved)
        self.assertAlmostEqual(retrieved['irradiance']['mean_kwh_m2_day'], 4.9)
        mock_rdb.get.assert_called_once_with(jid)

    @patch('jobs.rdb')
    def test_get_job_result_returns_none_when_absent(self, mock_rdb):
        """get_job_result must return None when no result is stored."""
        mock_rdb.get = Mock(return_value=None)

        result = get_job_result(_generate_jid())

        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()


from jobs import (
    _generate_jid,
    _instantiate_job,
    _save_job,
    _queue_job,
    get_job_by_id,
    add_job,
    start_job,
    update_job_status,
    save_job_result,
    get_job_result,
    Job,
    JobStatus,
)


class TestJobGeneration(unittest.TestCase):
    """Test job ID generation and instantiation (legacy class — kept for coverage)."""

    def test_generate_jid_returns_string(self):
        """Test that _generate_jid returns a string."""
        jid = _generate_jid()
        self.assertIsInstance(jid, str)

    def test_generate_jid_returns_unique_ids(self):
        """Test that _generate_jid returns unique IDs."""
        jid1 = _generate_jid()
        jid2 = _generate_jid()
        self.assertNotEqual(jid1, jid2)

    def test_instantiate_job_creates_job_object(self):
        """Test that _instantiate_job creates a proper Job object."""
        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, "point",
                               40.5, -74.0, "20240101", "20250101")
        self.assertIsInstance(job, Job)
        self.assertEqual(job.jid, jid)
        self.assertEqual(job.status, JobStatus.QUEUED)
        self.assertEqual(job.lat, 40.5)
        self.assertEqual(job.lon, -74.0)

    def test_instantiate_job_with_different_statuses(self):
        """Test _instantiate_job with different job statuses."""
        jid = _generate_jid()
        for status in [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.ERROR]:
            job = _instantiate_job(jid, status, "point",
                                   40.5, -74.0, "20240101", "20250101")
            self.assertEqual(job.status, status)


class TestJobPersistence(unittest.TestCase):
    """Test job storage and retrieval."""

    @patch('jobs.jdb')
    def test_save_job_stores_job(self, mock_jdb):
        """Test that _save_job stores job data correctly."""
        mock_jdb.set = Mock(return_value=True)

        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, "point",
                               40.5, -74.0, "20240101", "20250101")
        result = _save_job(jid, job)

        self.assertTrue(result)
        call_args = mock_jdb.set.call_args
        stored_json = call_args[0][1]
        stored_data = json.loads(stored_json)
        self.assertEqual(stored_data['jid'], jid)
        self.assertEqual(stored_data['status'], JobStatus.QUEUED.value)

    @patch('jobs.q')
    def test_queue_job_adds_to_queue(self, mock_q):
        """Test that _queue_job adds job to the queue."""
        mock_q.put = Mock()

        jid = _generate_jid()
        result = _queue_job(jid)

        self.assertTrue(result)
        mock_q.put.assert_called_once_with(jid)

    @patch('jobs.jdb')
    def test_get_job_by_id_retrieves_job(self, mock_jdb):
        """Test that get_job_by_id retrieves and deserializes a job."""
        jid = _generate_jid()
        job_data = {
            'jid': jid,
            'status': JobStatus.QUEUED.value,
            'job_type': 'point',
            'lat': 40.5,
            'lon': -74.0,
            'start_date': '20240101',
            'end_date': '20250101',
            'start_time': None,
            'end_time': None,
        }

        mock_jdb.get = Mock(return_value=json.dumps(job_data).encode('utf-8'))

        retrieved_job = get_job_by_id(jid)

        self.assertEqual(retrieved_job.jid, jid)
        self.assertEqual(retrieved_job.status, JobStatus.QUEUED)
        self.assertEqual(retrieved_job.lat, 40.5)
        self.assertEqual(retrieved_job.lon, -74.0)
        mock_jdb.get.assert_called_once_with(jid)



if __name__ == '__main__':
    unittest.main()
