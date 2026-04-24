import requests
import time
import unittest

BASE_URL = "http://localhost:5000"


class TestWorkerJobProcessing(unittest.TestCase):
    """Test worker job processing capabilities."""
    
    @classmethod
    def setUpClass(cls):
        """Ensure API is running and gene data is loaded."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{BASE_URL}/genes")
                if response.status_code in [200, 400]:
                    break
            except requests.ConnectionError:
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                else:
                    raise Exception("Could not connect to API")
        
        requests.post(f"{BASE_URL}/data")
        time.sleep(1)
    
    def test_worker_processes_job(self):
        """Test that worker picks up and processes a QUEUED job."""
        payload = {"startID": 0, "endID": 50}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        self.assertEqual(response.status_code, 200)
        job_data = response.json()
        jid = job_data["jid"]
        initial_status = job_data["status"]
        
        self.assertEqual(initial_status, "QUEUED")
        
        time.sleep(2)
        
        response = requests.get(f"{BASE_URL}/jobs/{jid}")
        self.assertEqual(response.status_code, 200)
        updated_job = response.json()
        
        self.assertIn(updated_job["status"], ["RUNNING", "FINISHED -- SUCCESS"])
    
    def test_worker_processes_multiple_jobs_concurrently(self):
        """Test worker can handle multiple jobs in sequence."""
        job_ids = []
        
        for i in range(3):
            payload = {"startID": i * 30, "endID": (i + 1) * 30}
            response = requests.post(f"{BASE_URL}/jobs", json=payload)
            self.assertEqual(response.status_code, 200)
            job_ids.append(response.json()["jid"])
        
        time.sleep(5)
        
        for jid in job_ids:
            response = requests.get(f"{BASE_URL}/jobs/{jid}")
            self.assertEqual(response.status_code, 200)
            job = response.json()
            
            self.assertIn(job["status"], ["RUNNING", "FINISHED -- SUCCESS"])
    
    def test_worker_analysis_contains_distribution_statistics(self):
        """Test that analysis includes proper distribution statistics."""

        payload = {"startID": 0, "endID": 200}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        jid = response.json()["jid"]
        
        time.sleep(4)
        
        response = requests.get(f"{BASE_URL}/results/{jid}")
        result = response.json()
        
        if "results" in result and result["results"] is not None:
            analysis = result["results"]
            
            locus_types = analysis["locus_type_distribution"]
            self.assertGreater(len(locus_types), 0)
            
            for locus_type, count in locus_types.items():
                self.assertIsInstance(locus_type, str)
                self.assertIsInstance(count, int)
                self.assertGreater(count, 0)
            
            locus_groups = analysis["locus_group_distribution"]
            self.assertGreater(len(locus_groups), 0)
            
            for group, count in locus_groups.items():
                self.assertIsInstance(group, str)
                self.assertIsInstance(count, int)
                self.assertGreater(count, 0)
    
    def test_worker_most_common_locus_type_is_accurate(self):
        """Test that most_common_locus_type matches the distribution."""
        payload = {"startID": 0, "endID": 150}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        jid = response.json()["jid"]
        
        time.sleep(4)
        
        response = requests.get(f"{BASE_URL}/results/{jid}")
        result = response.json()
        
        if "results" in result and result["results"] is not None:
            analysis = result["results"]
            
            most_common = analysis["most_common_locus_type"]
            distribution = analysis["locus_type_distribution"]
            
            self.assertIn(most_common, distribution)
            
            max_count = max(distribution.values())
            self.assertEqual(distribution[most_common], max_count)


class TestWorkerErrorHandling(unittest.TestCase):
    """Test worker error handling capabilities."""
    
    @classmethod
    def setUpClass(cls):
        """Ensure API is running."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                requests.get(f"{BASE_URL}/genes")
                break
            except requests.ConnectionError:
                if attempt < max_retries - 1:
                    time.sleep(0.5)
    
    
    def test_worker_continues_after_failed_job(self):
        """Test worker continues processing after a job failure."""

        payload = {"startID": 0, "endID": 100000}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        
        if response.status_code == 200:
            jid1 = response.json()["jid"]
            
            payload2 = {"startID": 0, "endID": 50}
            response2 = requests.post(f"{BASE_URL}/jobs", json=payload2)
            self.assertEqual(response2.status_code, 200)
            jid2 = response2.json()["jid"]
            
            time.sleep(5)
            
            response1 = requests.get(f"{BASE_URL}/jobs/{jid1}")
            self.assertEqual(response1.status_code, 200)
            job1 = response1.json()
            # Job 1 with large range should have failed or still be processing
            self.assertIn(job1["status"], ["RUNNING", "FINISHED -- ERROR"])
            
            response2 = requests.get(f"{BASE_URL}/jobs/{jid2}")
            self.assertEqual(response2.status_code, 200)

if __name__ == '__main__':
    unittest.main()

