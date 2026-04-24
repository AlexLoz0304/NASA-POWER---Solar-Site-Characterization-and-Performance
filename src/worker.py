from jobs import q, rdb, rd, Job, JobStatus, start_job, update_job_status, get_job_by_id, save_job_result
import json
import logging
from collections import defaultdict
from typing import cast

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def analyze_gene_locus_distribution(start_idx: int, end_idx: int) -> dict:
    """
    Analyze the distribution of gene locus types within a given index range.
    
    This analysis examines genes by their index position in the HGNC database
    and provides statistics on locus types and groups.
    
    Args:
        start_idx: Starting index in Redis scan
        end_idx: Ending index in Redis scan
        
    Returns:
        dict: Analysis results including locus type distribution and summary statistics
    """
    try:
        all_gene_ids = sorted([key.decode('utf-8') for key in rd.scan_iter()])
        
        gene_ids_in_range = all_gene_ids[start_idx:end_idx+1]
        
        locus_distribution = defaultdict(int)
        gene_count = 0
        locus_groups = defaultdict(int)
        
        for gene_id in gene_ids_in_range:
            gene_data_raw = rd.get(gene_id)
            if gene_data_raw:
                try:
                    gene_data = json.loads(cast(bytes, gene_data_raw))  # type: ignore
                    gene_count += 1
                    
                    locus_type = gene_data.get('locus_type', 'Unknown')
                    if locus_type:
                        locus_distribution[locus_type] += 1
                    
                    locus_group = gene_data.get('locus_group', 'Unknown')
                    if locus_group:
                        locus_groups[locus_group] += 1
                except json.JSONDecodeError:
                    continue
        
        most_common_locus_type = max(locus_distribution.items(), 
                                     key=lambda x: x[1])[0] if locus_distribution else None
        most_common_locus_group = max(locus_groups.items(), 
                                      key=lambda x: x[1])[0] if locus_groups else None
        
        result = {
            "analysis_type": "gene_locus_distribution",
            "start_index": start_idx,
            "end_index": end_idx,
            "genes_analyzed": gene_count,
            "locus_type_distribution": dict(locus_distribution),
            "locus_group_distribution": dict(locus_groups),
            "most_common_locus_type": most_common_locus_type,
            "most_common_locus_group": most_common_locus_group,
            "unique_locus_types": len(locus_distribution),
            "unique_locus_groups": len(locus_groups)
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error during gene analysis: {e}")
        raise

@q.worker
def do_work(jid: str):
    """
    Process a job from the queue.
    
    Retrieves a job ID, performs HGNC gene analysis on the specified range,
    stores results, and updates the job status.
    
    Args:
        jid (str): The job ID to process
    """
    try:
        job = start_job(jid)
        logger.info(f"[JOB STARTED] Job ID: {jid} | Analyzing genes {job.start} to {job.end}")
        
        update_job_status(jid, JobStatus.RUNNING)
        logger.info(f"[JOB RUNNING] Job ID: {jid} | Starting analysis...")
        
        analysis_result = analyze_gene_locus_distribution(job.start, job.end)
        
        save_job_result(jid, analysis_result)
        logger.info(f"[ANALYSIS COMPLETE] Job ID: {jid} | Analyzed {analysis_result['genes_analyzed']} genes")
        
        update_job_status(jid, JobStatus.SUCCESS)
        logger.info(f"[JOB COMPLETED] Job ID: {jid} | Status: SUCCESS")
        
    except Exception as e:
        logger.error(f"[JOB ERROR] Job ID: {jid} | Error: {str(e)}", exc_info=True)
        update_job_status(jid, JobStatus.ERROR)
        logger.error(f"[JOB FAILED] Job ID: {jid} | Status: ERROR")

if __name__ == "__main__":
    logger.info("Worker started and listening for jobs...")
    do_work()