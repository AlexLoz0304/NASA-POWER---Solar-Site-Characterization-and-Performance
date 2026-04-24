import unittest
import sys
import json
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, '/home/ubuntu/my-coe332-hws/homework08/src')

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
    """Test job ID generation and instantiation."""
    
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
        job = _instantiate_job(jid, JobStatus.QUEUED, 0, 100)
        
        self.assertIsInstance(job, Job)
        self.assertEqual(job.jid, jid)
        self.assertEqual(job.status, JobStatus.QUEUED)
        self.assertEqual(job.start, 0)
        self.assertEqual(job.end, 100)
    
    def test_instantiate_job_with_different_statuses(self):
        """Test _instantiate_job with different job statuses."""
        jid = _generate_jid()
        
        for status in [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.ERROR]:
            job = _instantiate_job(jid, status, 0, 50)
            self.assertEqual(job.status, status)


class TestJobPersistence(unittest.TestCase):
    """Test job storage and retrieval."""
    
    @patch('jobs.jdb')
    def test_save_job_stores_job(self, mock_jdb):
        """Test that _save_job stores job data correctly."""
        mock_jdb.set = Mock(return_value=True)
        
        jid = _generate_jid()
        job = _instantiate_job(jid, JobStatus.QUEUED, 0, 100)
        
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
            'start': 10,
            'end': 20,
            'start_time': None,
            'end_time': None
        }
        
        mock_jdb.get = Mock(return_value=json.dumps(job_data).encode('utf-8'))
        
        retrieved_job = get_job_by_id(jid)
        
        self.assertEqual(retrieved_job.jid, jid)
        self.assertEqual(retrieved_job.status, JobStatus.QUEUED)
        self.assertEqual(retrieved_job.start, 10)
        self.assertEqual(retrieved_job.end, 20)
        mock_jdb.get.assert_called_once_with(jid)


class TestJobResults(unittest.TestCase):
    """Test job result storage and retrieval."""
    
    @patch('jobs.rdb')
    def test_save_job_result_stores_result(self, mock_rdb):
        """Test that save_job_result stores result data."""
        mock_rdb.set = Mock(return_value=True)
        
        jid = _generate_jid()
        result_data = {
            'locus_types': {'protein_coding': 5, 'lncRNA': 3},
            'locus_groups': {'protein_coding gene': 5},
        }
        
        result = save_job_result(jid, result_data)
        
        self.assertTrue(result)
        
        call_args = mock_rdb.set.call_args
        stored_json = call_args[0][1]
        stored_data = json.loads(stored_json)
        self.assertEqual(stored_data['locus_types'], {'protein_coding': 5, 'lncRNA': 3})
    
    @patch('jobs.rdb')
    def test_get_job_result_retrieves_result(self, mock_rdb):
        """Test that get_job_result retrieves and deserializes result data."""
        jid = _generate_jid()
        result_data = {
            'locus_types': {'protein_coding': 10},
            'locus_groups': {'protein_coding gene': 10},
        }
        
        mock_rdb.get = Mock(return_value=json.dumps(result_data).encode('utf-8'))
        
        retrieved = get_job_result(jid)
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['locus_types'], {'protein_coding': 10})
        mock_rdb.get.assert_called_once_with(jid)

    @patch('jobs.rdb')
    def test_save_job_result_handles_exception(self, mock_rdb):
        """Test that save_job_result returns False on exception."""
        mock_rdb.set = Mock(side_effect=Exception("Connection error"))
        
        jid = _generate_jid()
        result = save_job_result(jid, {'data': 'test'})
        
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
