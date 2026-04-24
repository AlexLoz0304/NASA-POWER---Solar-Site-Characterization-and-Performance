from datetime import datetime
import json
import uuid
import redis
from hotqueue import HotQueue
from enum import Enum
from pydantic import BaseModel
import typing
import os
import logging
from typing import cast

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_redis_ip = os.environ.get("REDIS_HOST", "localhost")
_redis_port = int(os.environ.get("REDIS_PORT", "6379"))

rd = redis.Redis(host=_redis_ip, port=6379, db=0)
q = HotQueue("queue", host=_redis_ip, port=6379, db=1)
jdb = redis.Redis(host=_redis_ip, port=6379, db=2) 
rdb = redis.Redis(host=_redis_ip, port=6379, db=3) 


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "FINISHED -- ERROR"
    SUCCESS = "FINISHED -- SUCCESS"


class Job(BaseModel):
    jid: str
    status: JobStatus
    start: int
    end: int
    start_time: typing.Optional[datetime] = None
    end_time: typing.Optional[datetime] = None


def _generate_jid() -> str:
    """
    Generate a pseudo-random identifier for a job.
    
    Returns:
        str: A UUID string serving as a unique job identifier
    """
    jid = str(uuid.uuid4())
    logger.debug(f"Generated new job ID: {jid}")
    return jid


def _instantiate_job(jid: str, status: JobStatus, start: int, end: int) -> Job:
    """
    Create the job object description as a python dictionary. Requires the job id,
    status, start and end parameters.
    
    Args:
        jid (str): The job ID
        status (JobStatus): The job status (QUEUED, RUNNING, SUCCESS, ERROR)
        start (int): The starting index for gene analysis
        end (int): The ending index for gene analysis
        
    Returns:
        Job: A Pydantic Job model instance
    """
    job = Job(jid=jid, status=status, start=start, end=end)
    logger.debug(f"Instantiated new job {jid} with status {status}, range [{start}, {end}]")
    return job


def _save_job(jid: str, job: Job) -> bool:
    """
    Save a job object in the Redis database (db=2).
    
    Args:
        jid (str): The job ID
        job (Job): The Job object to save
        
    Returns:
        bool: True if successful
    """
    try:
        jdb.set(jid, json.dumps(job.model_dump(mode="json")))
        logger.debug(f"Saved job {jid} to database with status {job.status}")
        return True
    except Exception as e:
        logger.error(f"Failed to save job {jid}: {str(e)}")
        raise


def _queue_job(jid: str) -> bool:
    """Add a job to the redis queue."""
    q.put(jid)
    logger.debug(f"Queued job {jid} for processing")
    return True


def get_job_by_id(jid: str) -> Job:
    """
    Retrieve a job object from the Redis database by job ID.
    
    Args:
        jid (str): The job ID
        
    Returns:
        Job: The Job object
    """
    try:
        raw_data = json.loads(jdb.get(jid))  # type: ignore
        logger.debug(f"Retrieved job {jid} from database with status {raw_data.get('status')}")
        return Job(**raw_data)
    except Exception as e:
        logger.error(f"Failed to retrieve job {jid}: {str(e)}")
        raise


def add_job(start: int, end: int) -> Job:
    """
    Add a job to the redis database and queue.
    
    Args:
        start (int): The starting index for gene analysis
        end (int): The ending index for gene analysis
        
    Returns:
        Job: The created Job object
    """
    jid = _generate_jid()
    job = _instantiate_job(jid, JobStatus.QUEUED, start, end)
    _save_job(jid, job)
    _queue_job(jid)
    logger.info(f"Added job {jid} with range [{start}, {end}] to queue")
    return job


def start_job(jid: str) -> Job:
    """
    Called by worker when starting a new job. Updates the job's status and start time.
    
    Args:
        jid (str): The job ID
        
    Returns:
        Job: The updated Job object with RUNNING status
    """
    start_time = datetime.now()
    job = get_job_by_id(jid)
    job.start_time = start_time
    _save_job(jid=jid, job=job)
    logger.info(f"Started job {jid} at {start_time.isoformat()}")
    return job


def update_job_status(jid: str, status: JobStatus) -> bool:
    """
    Update the status of job with job id `jid` to status `status`.
    
    Args:
        jid (str): The job ID
        status (JobStatus): The new status (QUEUED, RUNNING, SUCCESS, ERROR)
        
    Returns:
        bool: True if successful
    """
    job = get_job_by_id(jid)
    if job:
        old_status = job.status
        job.status = status
        if job.status == JobStatus.ERROR or job.status == JobStatus.SUCCESS:
            job.end_time = datetime.now()
            logger.info(f"Job {jid} transitioned to {status} at {job.end_time.isoformat()}")
        else:
            logger.debug(f"Job {jid} transitioned from {old_status} to {status}")
        return _save_job(jid, job)
    else:
        raise Exception()


def save_job_result(jid: str, result: dict) -> bool:
    """
    Save analysis result for a job in the results database (db=3).
    
    Args:
        jid (str): The job ID
        result (dict): The analysis result dictionary
        
    Returns:
        bool: True if successful, False if error
    """
    try:
        rdb.set(jid, json.dumps(result))
        logger.info(f"Saved analysis result for job {jid}")
        return True
    except Exception as e:
        logger.error(f"Error saving result for job {jid}: {e}", exc_info=True)
        return False


def get_job_result(jid: str) -> typing.Optional[dict]:
    """
    Retrieve analysis result for a job from the results database (db=3).
    
    Args:
        jid (str): The job ID
        
    Returns:
        dict: The analysis result dictionary, or None if not found
    """
    try:
        result_data = rdb.get(jid)
        if result_data is None:
            logger.debug(f"No result found for job {jid}")
            return None
        result_bytes = cast(bytes, result_data)
        result = json.loads(result_bytes.decode('utf-8'))
        logger.debug(f"Retrieved analysis result for job {jid}")
        return result
    except Exception as e:
        logger.error(f"Error retrieving result for job {jid}: {e}", exc_info=True)
        return None