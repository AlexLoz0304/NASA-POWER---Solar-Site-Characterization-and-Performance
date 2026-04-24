import requests
import unittest
import time
import json

BASE_URL = "http://localhost:5000"


class TestGeneDataEndpoints(unittest.TestCase):
    """Test gene data loading and retrieval endpoints."""
    
    @classmethod
    def setUpClass(cls):
        """Verify API is running and load gene data."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{BASE_URL}/genes")
                if response.status_code in [200, 400]:
                    break
            except requests.ConnectionError:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise Exception("Could not connect to API after 10 seconds")
    
    def test_post_data_loads_genes(self):
        """Test POST /data endpoint loads genes into Redis."""
        response = requests.post(f"{BASE_URL}/data")
        
        self.assertIn(response.status_code, [200, 400])
        
        if response.status_code == 200:
            data = response.json()
            self.assertIn("status", data)
            self.assertIn("number_of_genes_parsed", data)
            self.assertGreater(data["number_of_genes_parsed"], 0)
    
    def test_get_data_retrieves_genes(self):
        """Test GET /data endpoint retrieves all genes."""
        requests.post(f"{BASE_URL}/data")
        time.sleep(0.5)
        
        response = requests.get(f"{BASE_URL}/data")
        
        self.assertEqual(response.status_code, 200)
        genes = response.json()
        
        self.assertIsInstance(genes, list)
        if len(genes) > 0:
            gene = genes[0]
            self.assertIn("hgnc_id", gene)
    
    def test_get_genes_returns_hgnc_ids(self):
        """Test GET /genes endpoint returns list of hgnc_ids."""
        requests.post(f"{BASE_URL}/data")
        time.sleep(0.5)
        
        response = requests.get(f"{BASE_URL}/genes")
        
        self.assertEqual(response.status_code, 200)
        gene_ids = response.json()
        
        self.assertIsInstance(gene_ids, list)
    
    def test_get_specific_gene(self):
        """Test GET /genes/{hgnc_id} endpoint retrieves specific gene."""
        requests.post(f"{BASE_URL}/data")
        time.sleep(0.5)
        
        response = requests.get(f"{BASE_URL}/genes")
        hgnc_id = response.json()[0]
            
        response = requests.get(f"{BASE_URL}/genes/{hgnc_id}")
        self.assertEqual(response.status_code, 200)
            
        gene = response.json()
        self.assertEqual(gene["hgnc_id"], hgnc_id)
    
    def test_get_gene_not_found(self):
        """Test GET /genes/{hgnc_id} returns 404 or error for invalid ID."""
        response = requests.get(f"{BASE_URL}/genes/INVALID_JOB")
        
        self.assertIn(response.status_code, [404, 500])
    
    def test_delete_data_clears_genes(self):
        """Test DELETE /data endpoint clears genes from Redis."""
        requests.post(f"{BASE_URL}/data")
        time.sleep(0.5)
        
        response = requests.delete(f"{BASE_URL}/data")
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("status", data)
        self.assertIn("deleted", data)


class TestJobEndpoints(unittest.TestCase):
    """Test job submission and management endpoints."""
    
    def test_post_job_creates_job(self):
        """Test POST /jobs endpoint creates a new job."""
        payload = {
            "startID": 0,
            "endID": 100
        }
        
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        
        self.assertEqual(response.status_code, 200)
        job = response.json()
        
        self.assertIn("jid", job)
        self.assertIn("status", job)
        self.assertEqual(job["status"], "QUEUED")
        self.assertEqual(job["start"], 0)
        self.assertEqual(job["end"], 100)
    
    def test_post_job_invalid_range(self):
        """Test POST /jobs rejects invalid gene ID ranges."""
        payload = {
            "startID": 100,
            "endID": 50 
        }
        
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        
        self.assertIn(response.status_code, [400, 500])
    
    def test_get_jobs_lists_jobs(self):
        """Test GET /jobs endpoint returns list of jobs."""
        payload = {"startID": 0, "endID": 50}
        requests.post(f"{BASE_URL}/jobs", json=payload)
        time.sleep(0.5)
        
        response = requests.get(f"{BASE_URL}/jobs")
        
        self.assertEqual(response.status_code, 200)
        jobs = response.json()
        
        self.assertIsInstance(jobs, list)

        self.assertIn("jid", jobs[0])
        self.assertIn("status", jobs[0])
    
    def test_get_specific_job(self):
        """Test GET /jobs/{jid} endpoint retrieves specific job."""
        payload = {"startID": 10, "endID": 60}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        job_data = response.json()
        jid = job_data["jid"]
        
        time.sleep(0.5)
        
        response = requests.get(f"{BASE_URL}/jobs/{jid}")
        
        self.assertEqual(response.status_code, 200)
        job = response.json()
        self.assertEqual(job["jid"], jid)
    
    def test_get_job_not_found(self):
        """Test GET /jobs/{jid} returns 404 or error for invalid job ID."""
        response = requests.get(f"{BASE_URL}/jobs/INVALID_JOB")
        
        self.assertIn(response.status_code, [404, 500])


class TestResultsEndpoint(unittest.TestCase):
    """Test results retrieval endpoint."""
    
    def test_get_results_queued_job(self):
        """Test GET /results/{jid} for a queued job returns status message."""
        payload = {"startID": 0, "endID": 10}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        jid = response.json()["jid"]
        
        time.sleep(0.1)
        
        response = requests.get(f"{BASE_URL}/results/{jid}")
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertIn("job_status", result)
        self.assertIn(result["job_status"], ["QUEUED", "RUNNING"])
    
    def test_get_results_invalid_jid(self):
        """Test GET /results/{jid} returns error for invalid job ID."""
        response = requests.get(f"{BASE_URL}/results/INVALID_JOB")
        
        self.assertIn(response.status_code, [404, 500])
    
    def test_get_results_after_completion(self):
        """Test GET /results/{jid} returns analysis results after job completes."""
        requests.post(f"{BASE_URL}/data")
        time.sleep(0.5)
        
        payload = {"startID": 0, "endID": 50}
        response = requests.post(f"{BASE_URL}/jobs", json=payload)
        jid = response.json()["jid"]
        
        time.sleep(3)
        
        response = requests.get(f"{BASE_URL}/results/{jid}")
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertIn("job_status", result)
        
        results = result["results"]
        self.assertTrue(
            "locus_types" in results or 
            "locus_type_distribution" in results or
            "genes_analyzed" in results
        )

if __name__ == '__main__':
    unittest.main()
