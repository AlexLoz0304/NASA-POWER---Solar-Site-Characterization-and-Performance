"""
jobs.py — Redis-backed job management for the NASA POWER Solar API
==================================================================
Provides the Job data model, Redis connections, and helper functions used by
both FastAPI_api.py (to create and queue jobs) and worker.py (to execute them).

Supports point jobs:
    "point"    — Fetches all 10 parameters for a single lat/lon coordinate via
                 the NASA POWER daily point endpoint.

Redis database layout:
    db=0  rd   — main data store: solar grid-cell records (solar:{lat}:{lon})
    db=1  q    — HotQueue: worker blocks here waiting for job IDs
    db=2  jdb  — job metadata store: Job objects keyed by jid
    db=3  rdb  — job results store:  worker writes result summary keyed by jid
"""

from datetime import datetime, timezone
import json
import uuid
import redis
from hotqueue import HotQueue
from enum import Enum
from pydantic import BaseModel
import typing
import os
import logging
from typing import cast, Optional

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis connection setup
# ---------------------------------------------------------------------------
# Host and port are read from environment variables so Docker/Kubernetes
# deployments can inject the correct Redis service address without rebuilding.
_redis_ip   = os.environ.get("REDIS_HOST", "localhost")
_redis_port = int(os.environ.get("REDIS_PORT", "6379"))

# db=0 — main data store shared with FastAPI (location:* and solar:* keys)
rd  = redis.Redis(host=_redis_ip, port=_redis_port, db=0)

# db=1 — HotQueue: the worker process blocks on this queue waiting for job IDs
q   = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)

# db=2 — job metadata database: stores serialised Job objects keyed by jid
jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)

# db=3 — results database: stores NASA POWER parameter data returned by completed jobs
rdb = redis.Redis(host=_redis_ip, port=_redis_port, db=3)


# ---------------------------------------------------------------------------
# Job status enumeration
# ---------------------------------------------------------------------------

class JobStatus(str, Enum):
    """Lifecycle states for a background data-fetch job."""
    QUEUED  = "QUEUED"
    RUNNING = "RUNNING"
    ERROR   = "FINISHED -- ERROR"
    SUCCESS = "FINISHED -- SUCCESS"


# ---------------------------------------------------------------------------
# Job data model
# ---------------------------------------------------------------------------

class Job(BaseModel):
    """
    Represents a background NASA POWER data-fetch task.

    Fields:
        jid        — Unique job identifier (UUID string).
        status     — Current lifecycle state (see JobStatus enum).
        location_id — UUID of the location to analyze (stored as location_id:{uuid} in Redis db=0).
        lat        — Latitude  in decimal degrees (cached from location record).
        lon        — Longitude in decimal degrees (cached from location record).
        start_date — NASA POWER date range start (YYYYMMDD string).
        end_date   — NASA POWER date range end   (YYYYMMDD string).
        start_time — UTC wall-clock time when the worker began processing.
        end_time   — UTC wall-clock time when the worker finished.
    """
    jid:        str
    status:     JobStatus
    location_id: str  # UUID identifying the specific location to analyze
    lat:        Optional[float] = None
    lon:        Optional[float] = None
    start_date: Optional[str]  = None
    end_date:   Optional[str]  = None
    start_time: Optional[datetime] = None  # Set by start_job() when worker picks it up
    end_time:   Optional[datetime] = None  # Set by update_job_status() on completion


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _generate_jid() -> str:
    """
    Generate a unique job identifier using UUID4.

    Returns:
        str: A random UUID string (e.g. '3f2504e0-4f89-11d3-9a0c-0305e82c3301').
    """
    jid = str(uuid.uuid4())
    logger.debug(f"Generated new job ID: {jid}")
    return jid


def _instantiate_job(jid: str, status: JobStatus,
                     location_id: str,
                     lat: Optional[float], lon: Optional[float],
                     start_date: Optional[str], end_date: Optional[str]) -> Job:
    """
    Construct a Job Pydantic model from the provided parameters.

    Pure factory function — does NOT write to Redis.
    Use _save_job() to persist the returned object.

    Args:
        jid        (str):            Job UUID.
        status     (JobStatus):      Initial status (typically QUEUED).
        location_id (str):           UUID of the location to analyze.
        lat        (float):          Target latitude (cached from location).
        lon        (float):          Target longitude (cached from location).
        start_date (str):            NASA POWER start date (YYYYMMDD).
        end_date   (str):            NASA POWER end   date (YYYYMMDD).

    Returns:
        Job: A fully populated (but not yet persisted) Job instance.
    """
    job = Job(
        jid=jid,
        status=status,
        location_id=location_id,
        lat=lat,
        lon=lon,
        start_date=start_date,
        end_date=end_date,
    )
    logger.debug(
        f"Instantiated point job {jid}: "
        f"location_id={location_id}  ({lat}, {lon})  "
        f"[{start_date} -> {end_date}]  status={status}"
    )
    return job


def _save_job(jid: str, job: Job) -> bool:
    """
    Serialise and save a Job object to the Redis job database (db=2).

    Serialises the Job Pydantic model to JSON (using model_dump with
    mode='json' so datetime fields are converted to ISO strings) and
    stores it under the key <jid> in Redis db=2.

    Args:
        jid (str): The job UUID -- used as the Redis key.
        job (Job): The Job instance to persist.

    Returns:
        bool: True if the write succeeded.

    Raises:
        Exception: Re-raises any Redis or serialisation error.
    """
    try:
        jdb.set(jid, json.dumps(job.model_dump(mode="json")))
        jdb.expire(jid, 7 * 24 * 3600)  # auto-expire stale jobs after 7 days
        logger.debug(f"Saved job {jid} to db=2 with status {job.status}")
        return True
    except Exception as e:
        logger.error(f"Failed to save job {jid}: {e}")
        raise


def _queue_job(jid: str) -> bool:
    """
    Push a job ID onto the HotQueue (Redis db=1) for the worker to consume.

    The worker process blocks on this queue and picks up job IDs in FIFO order.

    Args:
        jid (str): The job UUID to enqueue.

    Returns:
        bool: True after successfully pushing the ID onto the queue.
    """
    q.put(jid)
    logger.debug(f"Enqueued job {jid} onto HotQueue (db=1)")
    return True


# ---------------------------------------------------------------------------
# Public API used by FastAPI_api.py and worker.py
# ---------------------------------------------------------------------------

def get_job_by_id(jid: str) -> Job:
    """
    Retrieve a Job object from the Redis job database (db=2) by its UUID.

    Args:
        jid (str): The job UUID to look up.

    Returns:
        Job: The deserialised Job object.

    Raises:
        ValueError: If no record exists for the given jid (raw_data is None).
        Exception:  Re-raises JSON or Pydantic validation errors.
    """
    try:
        raw = jdb.get(jid)
        if raw is None:
            raise ValueError(f"Job '{jid}' not found in the job database.")
        raw_data = json.loads(raw)
        logger.debug(f"Retrieved job {jid}: status={raw_data.get('status')}")
        return Job(**raw_data)
    except Exception as e:
        logger.error(f"Failed to retrieve job {jid}: {e}")
        raise


def add_job(location_id: str,
            lat: Optional[float] = None,
            lon: Optional[float] = None,
            start_date: Optional[str] = None,
            end_date:   Optional[str] = None) -> Job:
    """
    Create a new point job, persist it in Redis db=2, and enqueue it for the worker.

    Called by POST /jobs.
    The returned Job will have status=QUEUED and no start/end timestamps yet.

    Args:
        location_id (str):         UUID of the location to analyze.
        lat        (float):        Target latitude (cached from location).
        lon        (float):        Target longitude (cached from location).
        start_date (str):          NASA POWER start date (YYYYMMDD).
        end_date   (str):          NASA POWER end   date (YYYYMMDD).

    Returns:
        Job: The newly created Job object with status QUEUED.
    """
    jid = _generate_jid()
    job = _instantiate_job(
        jid=jid,
        status=JobStatus.QUEUED,
        location_id=location_id,
        lat=lat,
        lon=lon,
        start_date=start_date,
        end_date=end_date,
    )
    _save_job(jid, job)   # Persist in Redis db=2
    _queue_job(jid)       # Push jid onto HotQueue so worker picks it up

    logger.info(
        f"Queued point job {jid}: "
        f"location_id={location_id}  ({lat}, {lon})  "
        f"[{start_date} -> {end_date}]"
    )
    return job


def start_job(jid: str) -> Job:
    """
    Mark a job as RUNNING and record its start timestamp.

    Called by the worker process immediately after dequeuing a job ID.
    Updates the Job's status to RUNNING and sets start_time to the current
    UTC wall-clock time, then persists the updated record in Redis db=2.

    Args:
        jid (str): The job UUID to transition to RUNNING.

    Returns:
        Job: The updated Job object with status=RUNNING and start_time set.
    """
    job = get_job_by_id(jid)
    job.status     = JobStatus.RUNNING
    job.start_time = datetime.now(timezone.utc)  # Record UTC start time
    _save_job(jid=jid, job=job)
    logger.info(f"Started job {jid} at {job.start_time.isoformat()}")
    return job


def update_job_status(jid: str, status: JobStatus) -> bool:
    """
    Update the status of a job and record the completion timestamp if applicable.

    Called by the worker to transition jobs from RUNNING to SUCCESS or ERROR.
    If the new status is a terminal state (SUCCESS or ERROR), end_time is also
    set to the current UTC time.

    Args:
        jid    (str):       The job UUID to update.
        status (JobStatus): The new status to set.

    Returns:
        bool: True if the update was persisted successfully.

    Raises:
        Exception: If the job cannot be found or the save fails.
    """
    job = get_job_by_id(jid)
    old_status = job.status
    job.status  = status

    # Record completion time for terminal states
    if job.status in (JobStatus.ERROR, JobStatus.SUCCESS):
        job.end_time = datetime.now(timezone.utc)
        logger.info(
            f"Job {jid} transitioned {old_status} -> {status} "
            f"at {job.end_time.isoformat()}"
        )
    else:
        logger.debug(f"Job {jid} transitioned {old_status} -> {status}")

    return _save_job(jid, job)


def save_job_result(jid: str, result: dict) -> bool:
    """
    Save the NASA POWER parameter data produced by a completed job.

    Stores the result dictionary as a JSON string in Redis db=3, keyed by jid.
    This is called by the worker after a successful NASA POWER fetch so that
    GET /results/{jid} can retrieve the data later.

    Args:
        jid    (str):  The job UUID.
        result (dict): The NASA POWER parameter time-series dict (or a bundled
                       payload including location metadata).

    Returns:
        bool: True if the write succeeded, False on error.
    """
    try:
        rdb.set(jid, json.dumps(result))
        logger.info(f"Saved NASA POWER result for job {jid} to db=3")
        return True
    except Exception as e:
        logger.error(f"Error saving result for job {jid}: {e}", exc_info=True)
        return False


def get_job_result(jid: str) -> typing.Optional[dict]:
    """
    Retrieve the NASA POWER data saved by a completed job from Redis db=3.

    Args:
        jid (str): The job UUID.

    Returns:
        dict: The stored result dictionary, or None if no result exists yet.
    """
    try:
        result_data = rdb.get(jid)
        if result_data is None:
            logger.debug(f"No result found for job {jid} in db=3")
            return None
        # Cast to bytes before decoding (rdb.get returns bytes or None)
        result_bytes = cast(bytes, result_data)
        result       = json.loads(result_bytes.decode("utf-8"))
        logger.debug(f"Retrieved NASA POWER result for job {jid}")
        return result
    except Exception as e:
        logger.error(f"Error retrieving result for job {jid}: {e}", exc_info=True)
        return None
