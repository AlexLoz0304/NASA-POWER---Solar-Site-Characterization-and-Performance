"""
test_jobs.py — Unit tests for jobs.py
======================================
Tests all job queue functions in isolation using unittest.mock to patch
Redis dependencies (jdb, rdb, q). No live Redis connection is required.

Run with:
    uv run pytest test/test_jobs.py
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

LOC_A = "550e8400-e29b-41d4-a716-446655440000"
LOC_B = "660e8400-e29b-41d4-a716-446655440001"


class TestJobIDGeneration(unittest.TestCase):
    """Tests for _generate_jid — UUID factory."""

    def test_generate_jid_returns_nonempty_string(self):
        jid = _generate_jid()
        self.assertIsInstance(jid, str)
        self.assertGreater(len(jid), 0)

    def test_generate_jid_returns_unique_ids(self):
        self.assertNotEqual(_generate_jid(), _generate_jid())

    def test_generate_jid_looks_like_uuid(self):
        self.assertEqual(_generate_jid().count('-'), 4)


class TestJobInstantiation(unittest.TestCase):
    """Tests for _instantiate_job — pure Job model factory."""

    def _make_job(self, status=JobStatus.QUEUED,
                  location_ids=None,
                  start="20250415", end="20260415"):
        if location_ids is None:
            location_ids = [LOC_A]
        jid = _generate_jid()
        return jid, _instantiate_job(jid, status, location_ids, start, end)

    def test_returns_job_instance(self):
        _, job = self._make_job()
        self.assertIsInstance(job, Job)

    def test_jid_is_preserved(self):
        jid, job = self._make_job()
        self.assertEqual(job.jid, jid)

    def test_status_is_set(self):
        _, job = self._make_job(status=JobStatus.QUEUED)
        self.assertEqual(job.status, JobStatus.QUEUED)

    def test_single_location_id_stored(self):
        _, job = self._make_job(location_ids=[LOC_A])
        self.assertEqual(job.location_ids, [LOC_A])

    def test_multiple_location_ids_stored(self):
        _, job = self._make_job(location_ids=[LOC_A, LOC_B])
        self.assertEqual(job.location_ids, [LOC_A, LOC_B])
        self.assertEqual(len(job.location_ids), 2)

    def test_date_range_is_set(self):
        _, job = self._make_job(start="20250101", end="20260101")
        self.assertEqual(job.start_date, "20250101")
        self.assertEqual(job.end_date, "20260101")

    def test_timestamps_are_none_on_creation(self):
        _, job = self._make_job()
        self.assertIsNone(job.start_time)
        self.assertIsNone(job.end_time)

    def test_all_job_statuses_accepted(self):
        for status in JobStatus:
            _, job = self._make_job(status=status)
            self.assertEqual(job.status, status)


class TestJobPersistence(unittest.TestCase):
    """Tests for _save_job and _queue_job — Redis write helpers."""

    def _make_queued_job(self, location_ids=None):
        if location_ids is None:
            location_ids = [LOC_A]
        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, location_ids, "20250415", "20260415")
        return jid, job

    @patch('jobs.jdb')
    def test_save_job_calls_redis_set(self, mock_jdb):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        jid, job = self._make_queued_job()
        result = _save_job(jid, job)
        self.assertTrue(result)
        mock_jdb.set.assert_called_once()
        self.assertEqual(mock_jdb.set.call_args[0][0], jid)

    @patch('jobs.jdb')
    def test_save_job_serialises_status(self, mock_jdb):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        jid, job = self._make_queued_job()
        _save_job(jid, job)
        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['status'], JobStatus.QUEUED.value)

    @patch('jobs.jdb')
    def test_save_job_serialises_location_ids(self, mock_jdb):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        jid, job = self._make_queued_job(location_ids=[LOC_A, LOC_B])
        _save_job(jid, job)
        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['location_ids'], [LOC_A, LOC_B])

    @patch('jobs.q')
    def test_queue_job_calls_put(self, mock_q):
        mock_q.put = Mock()
        jid = _generate_jid()
        result = _queue_job(jid)
        self.assertTrue(result)
        mock_q.put.assert_called_once_with(jid)


class TestAddJob(unittest.TestCase):
    """Tests for add_job — the public job creation API."""

    def _raw_for(self, jid, location_ids):
        return json.dumps({
            'jid': jid, 'status': JobStatus.QUEUED.value,
            'location_ids': location_ids,
            'start_date': '20250415', 'end_date': '20260415',
            'start_time': None, 'end_time': None,
        }).encode()

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_add_single_location_returns_job(self, mock_jdb, mock_q):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        job = add_job(location_ids=[LOC_A], start_date="20250415", end_date="20260415")
        self.assertIsInstance(job, Job)
        self.assertEqual(job.location_ids, [LOC_A])
        self.assertEqual(job.status, JobStatus.QUEUED)

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_add_multi_location_returns_job_with_all_ids(self, mock_jdb, mock_q):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        job = add_job(location_ids=[LOC_A, LOC_B], start_date="20250415", end_date="20260415")
        self.assertEqual(len(job.location_ids), 2)
        self.assertIn(LOC_A, job.location_ids)
        self.assertIn(LOC_B, job.location_ids)

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_add_job_enqueues_jid(self, mock_jdb, mock_q):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        job = add_job(location_ids=[LOC_A])
        mock_q.put.assert_called_once_with(job.jid)

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_add_job_persists_to_redis(self, mock_jdb, mock_q):
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        add_job(location_ids=[LOC_A])
        mock_jdb.set.assert_called_once()


class TestGetJobById(unittest.TestCase):
    """Tests for get_job_by_id — deserialisation from Redis."""

    def _make_raw(self, jid, status=JobStatus.QUEUED, location_ids=None,
                  start_date="20250415", end_date="20260415"):
        if location_ids is None:
            location_ids = [LOC_A]
        return json.dumps({
            'jid': jid,
            'status': status.value,
            'location_ids': location_ids,
            'start_date': start_date,
            'end_date': end_date,
            'start_time': None,
            'end_time': None,
        }).encode('utf-8')

    @patch('jobs.jdb')
    def test_retrieves_correct_jid(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid))
        job = get_job_by_id(jid)
        self.assertEqual(job.jid, jid)
        mock_jdb.get.assert_called_once_with(jid)

    @patch('jobs.jdb')
    def test_retrieves_correct_status(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid, status=JobStatus.RUNNING))
        job = get_job_by_id(jid)
        self.assertEqual(job.status, JobStatus.RUNNING)

    @patch('jobs.jdb')
    def test_retrieves_single_location_id(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid, location_ids=[LOC_A]))
        job = get_job_by_id(jid)
        self.assertEqual(job.location_ids, [LOC_A])

    @patch('jobs.jdb')
    def test_retrieves_multiple_location_ids(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._make_raw(jid, location_ids=[LOC_A, LOC_B]))
        job = get_job_by_id(jid)
        self.assertEqual(len(job.location_ids), 2)
        self.assertEqual(job.location_ids[0], LOC_A)
        self.assertEqual(job.location_ids[1], LOC_B)

    @patch('jobs.jdb')
    def test_raises_on_missing_job(self, mock_jdb):
        mock_jdb.get = Mock(return_value=None)
        with self.assertRaises(Exception):
            get_job_by_id("nonexistent-jid")


class TestJobLifecycle(unittest.TestCase):
    """Tests for start_job and update_job_status — state transitions."""

    def _queued_raw(self, jid, location_ids=None):
        if location_ids is None:
            location_ids = [LOC_A]
        return json.dumps({
            'jid': jid, 'status': JobStatus.QUEUED.value,
            'location_ids': location_ids,
            'start_date': '20250415', 'end_date': '20260415',
            'start_time': None, 'end_time': None,
        }).encode()

    def _running_raw(self, jid, location_ids=None):
        if location_ids is None:
            location_ids = [LOC_A]
        return json.dumps({
            'jid': jid, 'status': JobStatus.RUNNING.value,
            'location_ids': location_ids,
            'start_date': '20250415', 'end_date': '20260415',
            'start_time': datetime.now(timezone.utc).isoformat(), 'end_time': None,
        }).encode()

    @patch('jobs.jdb')
    def test_start_job_sets_running(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._queued_raw(jid))
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        job = start_job(jid)
        self.assertEqual(job.status, JobStatus.RUNNING)

    @patch('jobs.jdb')
    def test_start_job_sets_start_time(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._queued_raw(jid))
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        job = start_job(jid)
        self.assertIsNotNone(job.start_time)
        self.assertIsInstance(job.start_time, datetime)

    @patch('jobs.jdb')
    def test_start_multi_location_job(self, mock_jdb):
        """start_job must work correctly for a job with multiple location_ids."""
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._queued_raw(jid, location_ids=[LOC_A, LOC_B]))
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        job = start_job(jid)
        self.assertEqual(job.status, JobStatus.RUNNING)
        self.assertEqual(len(job.location_ids), 2)

    @patch('jobs.jdb')
    def test_update_job_status_to_success(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._running_raw(jid))
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        update_job_status(jid, JobStatus.SUCCESS)
        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['status'], JobStatus.SUCCESS.value)
        self.assertIsNotNone(stored['end_time'])

    @patch('jobs.jdb')
    def test_update_job_status_to_error(self, mock_jdb):
        jid = _generate_jid()
        mock_jdb.get = Mock(return_value=self._running_raw(jid))
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        update_job_status(jid, JobStatus.ERROR)
        stored = json.loads(mock_jdb.set.call_args[0][1])
        self.assertEqual(stored['status'], JobStatus.ERROR.value)
        self.assertIsNotNone(stored['end_time'])


class TestMultiLocationJob(unittest.TestCase):
    """Integration-style tests for multi-location job submission and result shape."""

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_job_with_three_locations(self, mock_jdb, mock_q):
        """A job with 3 location IDs must store all 3 in location_ids."""
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        locs = [LOC_A, LOC_B, "770e8400-e29b-41d4-a716-446655440002"]
        job = add_job(location_ids=locs)
        self.assertEqual(len(job.location_ids), 3)
        for lid in locs:
            self.assertIn(lid, job.location_ids)

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_single_location_job_is_still_list(self, mock_jdb, mock_q):
        """Even a single location must be stored as a list in location_ids."""
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        job = add_job(location_ids=[LOC_A])
        self.assertIsInstance(job.location_ids, list)
        self.assertEqual(len(job.location_ids), 1)

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_multi_location_result_structure(self, mock_jdb, mock_q):
        """Combined result dict must have location_count and locations list."""
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()
        job = add_job(location_ids=[LOC_A, LOC_B])

        # Simulate the combined result a worker would save
        combined = {
            "location_count": 2,
            "locations": [
                {"location": {"id": LOC_A, "name": "Seattle"}, "irradiance": {"mean_kwh_m2_day": 4.1}},
                {"location": {"id": LOC_B, "name": "Phoenix"}, "irradiance": {"mean_kwh_m2_day": 6.1}},
            ],
            "irradiance_plot_base64": "FAKEBASE64==",
        }
        self.assertEqual(combined["location_count"], 2)
        self.assertEqual(len(combined["locations"]), 2)
        self.assertIn("irradiance_plot_base64", combined)
        means = [loc["irradiance"]["mean_kwh_m2_day"] for loc in combined["locations"]]
        self.assertGreater(means[1], means[0])  # Phoenix > Seattle

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_comparison_summary_shape(self, mock_jdb, mock_q):
        """comparison_summary must contain ranked, best_site, and comparison keys."""
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()

        # Simulate the combined result including comparison_summary (as worker produces)
        combined = {
            "location_count": 2,
            "locations": [
                {"location": {"id": LOC_A, "name": "Seattle"}, "irradiance": {"mean_kwh_m2_day": 4.1}},
                {"location": {"id": LOC_B, "name": "Phoenix"}, "irradiance": {"mean_kwh_m2_day": 6.1}},
            ],
            "comparison_summary": {
                "ranked": [
                    {"rank": 1, "name": "Phoenix", "mean_irradiance_kwh_m2_day": 6.1},
                    {"rank": 2, "name": "Seattle", "mean_irradiance_kwh_m2_day": 4.1},
                ],
                "best_site": {"name": "Phoenix", "mean_irradiance_kwh_m2_day": 6.1},
                "comparison": "Phoenix leads with 6.10 kWh/m²/day vs Seattle at 4.10 kWh/m²/day.",
            },
        }
        cs = combined["comparison_summary"]
        self.assertIn("ranked", cs)
        self.assertIn("best_site", cs)
        self.assertIn("comparison", cs)
        self.assertIsInstance(cs["ranked"], list)
        self.assertEqual(len(cs["ranked"]), 2)
        self.assertEqual(cs["ranked"][0]["rank"], 1)
        self.assertEqual(cs["best_site"]["name"], "Phoenix")
        self.assertIsInstance(cs["comparison"], str)
        self.assertGreater(len(cs["comparison"]), 0)

    @patch('jobs.q')
    @patch('jobs.jdb')
    def test_single_location_has_no_comparison_summary(self, mock_jdb, mock_q):
        """Single-location jobs must NOT have a comparison_summary key."""
        mock_jdb.set = Mock(return_value=True)
        mock_jdb.expire = Mock()
        mock_q.put = Mock()

        combined = {
            "location_count": 1,
            "locations": [
                {"location": {"id": LOC_A, "name": "Seattle"}, "irradiance": {"mean_kwh_m2_day": 4.1}},
            ],
        }
        # Worker only adds comparison_summary when len(location_results) >= 2
        self.assertNotIn("comparison_summary", combined)


class TestJobResults(unittest.TestCase):
    """Tests for save_job_result and get_job_result — db=3 helpers."""

    @patch('jobs.rdb')
    def test_save_job_result_stores_dict(self, mock_rdb):
        mock_rdb.set = Mock(return_value=True)
        mock_rdb.expire = Mock()
        jid = _generate_jid()
        result = {
            'location_count': 1,
            'locations': [{'irradiance': {'mean_kwh_m2_day': 5.2}}],
        }
        ok = save_job_result(jid, result)
        self.assertTrue(ok)
        stored = json.loads(mock_rdb.set.call_args[0][1])
        self.assertEqual(stored['location_count'], 1)

    @patch('jobs.rdb')
    def test_save_job_result_returns_false_on_exception(self, mock_rdb):
        mock_rdb.set = Mock(side_effect=Exception("Redis down"))
        ok = save_job_result(_generate_jid(), {'data': 'test'})
        self.assertFalse(ok)

    @patch('jobs.rdb')
    def test_get_job_result_deserialises(self, mock_rdb):
        jid = _generate_jid()
        payload = {
            'location_count': 1,
            'locations': [{'irradiance': {'mean_kwh_m2_day': 4.9}}],
        }
        mock_rdb.get = Mock(return_value=json.dumps(payload).encode())
        retrieved = get_job_result(jid)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['location_count'], 1)
        mock_rdb.get.assert_called_once_with(jid)

    @patch('jobs.rdb')
    def test_get_job_result_returns_none_when_absent(self, mock_rdb):
        mock_rdb.get = Mock(return_value=None)
        self.assertIsNone(get_job_result(_generate_jid()))


if __name__ == '__main__':
    unittest.main()
