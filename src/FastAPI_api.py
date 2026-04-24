from typing import Optional, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from jobs import Job, add_job, get_job_by_id, jdb, rd, rdb, get_job_result
import json
import redis
import requests
import os
import logging

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

hgnc_url = "https://storage.googleapis.com/public-download-files/hgnc/json/json/hgnc_complete_set.json"

hgnc_genes = []

class StateVector(BaseModel):
    hgnc_id: Optional[str] = None
    symbol: Optional[str] = None
    name: Optional[str] = None
    locus_group: Optional[str] = None
    locus_type: Optional[str] = None
    status: Optional[str] = None
    location: Optional[str] = None
    location_sortable: Optional[str] = None
    gene_group: Optional[List[str]] = None
    gene_group_id: Optional[List[int]] = None
    merops: Optional[str] = None
    date_approved_reserved: Optional[str] = None
    date_modified: Optional[str] = None

class Jobrequest(BaseModel):
    startID: int
    endID: int

def load_hgnc_data():
    """
    Fetch and parse HGNC gene data from the API.
    
    Retrieves the complete HGNC gene dataset from the remote JSON API and parses
    it into StateVector Pydantic model instances. Extracts only the 14 specified
    fields from each gene record and stores them in the global hgnc_genes list.
    
    Returns:
        list: List of StateVector instances containing parsed gene data
        
    """
    global rd, hgnc_genes

    r = requests.get(hgnc_url)
    data = r.json()
    
    fields_to_extract = [
        'hgnc_id', 'symbol', 'name', 'locus_group', 'locus_type', 
        'status', 'location', 'location_sortable', 'gene_group', 
        'gene_group_id', 'merops', 'date_approved_reserved', 
        'date_modified'
    ]
    
    hgnc_genes = []
    for gene_data in data["response"]["docs"]:
        gene_dict = {}
        for field in fields_to_extract:
            gene_dict[field] = gene_data.get(field)
        
        try:
            gene = StateVector(**gene_dict)
            hgnc_genes.append(gene)
        except Exception as e:
            print(f"Error parsing gene: {e}")
    
    return hgnc_genes

@app.on_event("startup")
async def startup_event():
    """Load HGNC data when the app starts."""
    load_hgnc_data()

@app.post("/data")
def post_data() -> dict:
    """
    Load all HGNC genes into Redis cache.
    
    Fetches the in-memory hgnc_genes list and stores each gene as a JSON string
    in Redis, using hgnc_id as the key.
    
    Returns:
        dict: Status response with count of genes loaded
    """
    try:
        logger.info("POST /data: Starting to load HGNC genes into Redis")
        if not hgnc_genes:
            logger.error("POST /data: No genes loaded in memory")
            raise HTTPException(status_code=400, detail="No genes loaded in memory. Load data first.")
        
        count = 0
        for gene in hgnc_genes:
            try:
                if gene.hgnc_id:
                    rd.set(gene.hgnc_id, gene.model_dump_json())
                    count += 1
            except (AttributeError, ValueError) as e:
                logger.warning(f"POST /data: Failed to serialize gene {gene.hgnc_id}: {e}")
                continue
        
        logger.info(f"POST /data: Successfully loaded {count} genes into Redis")
        return {"status": "success", "number_of_genes_parsed": count}
    
    except redis.ConnectionError as e:
        logger.error(f"POST /data: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"POST /data: Error retrieving gene data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving gene data: {str(e)}")

@app.get("/data")
def get_data() -> list:
    """
    Retrieve all HGNC genes from Redis.
    
    Scans keys in Redis and returns the complete gene data as a JSON-formatted list.
    
    Returns:
        list: List of gene dictionaries containing all 14 fields
    """
    try:
        logger.info("GET /data: Retrieving all genes from Redis")
        genes = []
        for key in rd.scan_iter():
            val = rd.get(key)
            if val:
                try:
                    genes.append(json.loads(val))  # type: ignore
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"GET /data: Failed to deserialize gene with key {key}: {e}")
                    continue
        logger.info(f"GET /data: Retrieved {len(genes)} genes from Redis")
        return genes
    
    except redis.ConnectionError as e:
        logger.error(f"GET /data: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"GET /data: Error retrieving data from Redis: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving data from Redis: {str(e)}")


@app.delete("/data")
def delete_data() -> dict:
    """
    Delete all HGNC genes from Redis.
    
    Returns:
        dict: Status response with count of genes deleted
        
    """
    try:
        logger.info("DELETE /data: Starting to delete all genes from Redis")
        count = 0
        for gene in hgnc_genes:
            try:
                if gene.hgnc_id and rd.delete(gene.hgnc_id):
                    count += 1
            except Exception as e:
                logger.warning(f"DELETE /data: Failed to delete gene {gene.hgnc_id}: {e}")
                continue
        
        logger.info(f"DELETE /data: Successfully deleted {count} genes from Redis")
        return {"status": "success", "deleted": count}
    
    except redis.ConnectionError as e:
        logger.error(f"DELETE /data: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"DELETE /data: Error deleting data from Redis: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error deleting data from Redis: {str(e)}")


@app.get("/genes")
def get_genes() -> list:
    """
    Retrieve a list of all hgnc_id's from Redis
    
    Returns:
        list: List of hgnc_id strings
    """
    try:
        logger.info("GET /genes: Retrieving all hgnc_ids from Redis")
        gene_ids = []
        for key in rd.scan_iter():
            try:
                gene_id = key.decode('utf-8') if isinstance(key, bytes) else key
                gene_ids.append(gene_id)
            except (UnicodeDecodeError, AttributeError) as e:
                logger.warning(f"GET /genes: Failed to decode key {key}: {e}")
                continue
        logger.info(f"GET /genes: Retrieved {len(gene_ids)} hgnc_ids from Redis")
        return gene_ids
    
    except redis.ConnectionError as e:
        logger.error(f"GET /genes: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"GET /genes: Error retrieving gene IDs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving gene IDs: {str(e)}")


@app.get("/genes/{hgnc_id}")
def get_gene_id(hgnc_id: str) -> dict:
    """
    Retrieve all data associated with a specific hgnc_id.
    
    Looks up the gene in Redis by its hgnc_id and returns the complete gene
    object with all 14 fields.
    
    Args:
        hgnc_id (str): The unique HGNC identifier for the gene (e.g., 'HGNC:5')
        
    Returns:
        dict: Complete gene object with all 14 fields
    """
    try:
        logger.debug(f"GET /genes/{hgnc_id}: Looking up gene")
        if not hgnc_id or not isinstance(hgnc_id, str):
            logger.warning(f"GET /genes/{hgnc_id}: Invalid hgnc_id provided")
            raise HTTPException(status_code=400, detail="Invalid hgnc_id: must be a non-empty string")
        
        val = rd.get(hgnc_id)
        
        if val:
            gene_data = json.loads(val)  # type: ignore
            logger.debug(f"GET /genes/{hgnc_id}: Gene found and returned")
            return gene_data
        else:
            logger.info(f"GET /genes/{hgnc_id}: Gene not found")
            raise HTTPException(
                status_code=404, detail=f"Gene with hgnc_id '{hgnc_id}' not found")
    
    except redis.ConnectionError as e:
        logger.error(f"GET /genes/{hgnc_id}: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"GET /genes/{hgnc_id}: Error retrieving gene data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving gene data: {str(e)}")

@app.post("/jobs")
def post_job(jobreq: Jobrequest) -> Job:
    """
    Create a new job and add it to the queue.
    
    Args:
        jobreq (Jobrequest): Job request with start and end parameters
        
    Returns:
        Job: The newly created job object with QUEUED status
    """
    try:
        logger.info(f"POST /jobs: Creating new job with range [{jobreq.startID}, {jobreq.endID}]")
        if jobreq.startID is None or jobreq.endID is None:
            logger.warning("POST /jobs: Missing start or end parameters")
            raise HTTPException(status_code=400, detail="Gene ID start and end parameters are required")
        if jobreq.startID > jobreq.endID:
            logger.warning(f"POST /jobs: Invalid range [{jobreq.startID}, {jobreq.endID}]")
            raise HTTPException(status_code=400, detail="Gene ID start must be less than or equal to end")
        
        new_job = add_job(jobreq.startID, jobreq.endID)
        logger.info(f"POST /jobs: Successfully created job {new_job.jid}")
        return new_job
    except redis.ConnectionError as e:
        logger.error(f"POST /jobs: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"POST /jobs: Error creating job: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error creating job: {str(e)}")

@app.get("/jobs")
def get_jobs() -> list:
    """
    Retrieve a list of all jobs from the job database.
    
    Scans through all job IDs in jdb and returns complete job objects.
    
    Returns:
        list: List of Job objects with all their details
        
    Raises:
        HTTPException: 500 if Redis connection fails
    """
    try:
        logger.info("GET /jobs: Retrieving all jobs from database")
        jobs = []
        for jid in jdb.scan_iter():
            try:
                jid_str = jid.decode('utf-8')
                job = get_job_by_id(jid_str)
                jobs.append(job)
            except Exception as e:
                logger.warning(f"GET /jobs: Failed to retrieve job {jid}: {e}")
                continue
        logger.info(f"GET /jobs: Retrieved {len(jobs)} jobs from database")
        return jobs
    
    except redis.ConnectionError as e:
        logger.error(f"GET /jobs: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"GET /jobs: Error retrieving jobs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving jobs: {str(e)}") 

@app.get("/jobs/{jid}")
def get_job(jid: str) -> Job:
    """
    Retrieve a specific job by its ID.
    
    Args:
        jid (str): The job ID to look up
        
    Returns:
        Job: The job object with all its details
    """
    try:
        logger.debug(f"GET /jobs/{jid}: Looking up job")
        if not jid or not isinstance(jid, str):
            logger.warning(f"GET /jobs/{jid}: Invalid jid provided")
            raise HTTPException(status_code=400, detail="Invalid jid: must be a non-empty string")
        
        job = get_job_by_id(jid)
        logger.debug(f"GET /jobs/{jid}: Found job with status {job.status}")
        return job
    
    except redis.ConnectionError as e:
        logger.error(f"GET /jobs/{jid}: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"GET /jobs/{jid}: Error retrieving job: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving job: {str(e)}")


@app.get("/results/{jid}")
def get_job_results(jid: str) -> dict:
    """
    Retrieve analysis results for a completed job.
    
    This endpoint returns the computed analysis results from a job. If the job
    has not yet completed, a message will be returned indicating the current
    job status.
    
    Args:
        jid (str): The job ID to retrieve results for
        
    Returns:
        dict: Analysis results if the job is complete, or status message if still processing
        
    Raises:
        HTTPException: 400 if invalid jid, 404 if job not found, 500 on server error
    """
    try:
        logger.debug(f"GET /results/{jid}: Requesting analysis results")
        if not jid or not isinstance(jid, str):
            logger.warning(f"GET /results/{jid}: Invalid jid provided")
            raise HTTPException(status_code=400, detail="Invalid jid: must be a non-empty string")
        
        try:
            job = get_job_by_id(jid)
        except ValueError:
            logger.info(f"GET /results/{jid}: Job not found")
            raise HTTPException(status_code=404, detail=f"Job with id '{jid}' not found")
        
        logger.debug(f"GET /results/{jid}: Job status is {job.status}")
        
        if job.status == "QUEUED":
            logger.debug(f"GET /results/{jid}: Returning QUEUED status")
            return {
                "job_status": job.status,
                "message": f"Job {jid} is queued and waiting to be processed"
            }
        elif job.status == "RUNNING":
            logger.debug(f"GET /results/{jid}: Returning RUNNING status")
            return {
                "job_status": job.status,
                "start_time": job.start_time,
                "message": f"Job {jid} is currently running. Check back later for results"
            }
        elif job.status.startswith("FINISHED -- ERROR"):
            logger.info(f"GET /results/{jid}: Job finished with error")
            return {
                "job_status": job.status,
                "start_time": job.start_time,
                "end_time": job.end_time,
                "message": f"Job {jid} encountered an error during processing"
            }
        elif job.status.startswith("FINISHED -- SUCCESS"):
            logger.debug(f"GET /results/{jid}: Job finished successfully, retrieving results")
            results = get_job_result(jid)
            if results is None:
                logger.warning(f"GET /results/{jid}: Results not found for completed job")
                return {
                    "job_status": job.status,
                    "start_time": job.start_time,
                    "end_time": job.end_time,
                    "message": f"Job {jid} completed but results could not be retrieved"
                }
            
            logger.info(f"GET /results/{jid}: Successfully retrieved results for completed job")
            return {
                "status": "success",
                "job_id": jid,
                "job_status": job.status,
                "start_time": job.start_time,
                "end_time": job.end_time,
                "results": results
            }
        else:
            logger.warning(f"GET /results/{jid}: Job has unknown status: {job.status}")
            return {
                "job_status": job.status,
                "message": f"Job {jid} has unknown status: {job.status}",
            }
    
    except redis.ConnectionError as e:
        logger.error(f"GET /results/{jid}: Redis connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Redis connection error: {str(e)}")
    except Exception as e:
        logger.error(f"GET /results/{jid}: Error retrieving results: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving results: {str(e)}")