"""
Jobs Service for Databricks MCP Server

Handles Databricks Jobs API operations like listing, creating, and managing jobs.
"""
import requests
from typing import Optional, Dict, Any, List
from databricks_mcp_server.common.auth import get_databricks_base_url, get_databricks_headers


def list_jobs(
    limit: Optional[int] = 20,
    expand_tasks: Optional[bool] = False,
    name: Optional[str] = None,
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    List jobs in Databricks workspace.
    
    Args:
        limit (Optional[int]): Number of jobs to return (1-100, default 20)
        expand_tasks (Optional[bool]): Include task and cluster details in response
        name (Optional[str]): Filter by exact job name (case insensitive)
        page_token (Optional[str]): Token for pagination from previous request
    
    Returns:
        Dict[str, Any]: Response with jobs list and pagination tokens
        
    Raises:
        Exception: If API request fails
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.2/jobs/list"
    
    # Build query parameters
    params: Dict[str, Any] = {}
    
    if limit is not None:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        params["limit"] = limit
    
    if expand_tasks is not None:
        params["expand_tasks"] = str(expand_tasks).lower()
    
    if name is not None:
        params["name"] = name
        
    if page_token is not None:
        params["page_token"] = page_token
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error listing jobs: {response.status_code} - {response.text}")


def get_job(job_id: int) -> Dict[str, Any]:
    """
    Get detailed information about a specific job.
    
    Args:
        job_id (int): The unique identifier of the job
        
    Returns:
        Dict[str, Any]: Job details including settings, tasks, and configuration
        
    Raises:
        Exception: If API request fails
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.1/jobs/get"
    params = {"job_id": job_id}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error getting job {job_id}: {response.status_code} - {response.text}")


def list_job_runs(
    job_id: Optional[int] = None,
    active_only: Optional[bool] = None,
    completed_only: Optional[bool] = None,
    limit: Optional[int] = 20,
    page_token: Optional[str] = None,
    run_type: Optional[str] = None,
    start_time_from: Optional[int] = None,
    start_time_to: Optional[int] = None
) -> Dict[str, Any]:
    """
    List runs for jobs in the workspace.
    
    Args:
        job_id (Optional[int]): Filter runs for specific job ID
        active_only (Optional[bool]): Filter to only active runs
        completed_only (Optional[bool]): Filter to only completed runs
        limit (Optional[int]): Number of runs to return (1-1000, default 20)
        page_token (Optional[str]): Token for pagination
        run_type (Optional[str]): Filter by run type (JOB_RUN, WORKFLOW_RUN, SUBMIT_RUN)
        start_time_from (Optional[int]): Filter runs started after this time (epoch ms)
        start_time_to (Optional[int]): Filter runs started before this time (epoch ms)
        
    Returns:
        Dict[str, Any]: Response with runs list and pagination tokens
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.1/jobs/runs/list"
    
    params: Dict[str, Any] = {}
    
    if job_id is not None:
        params["job_id"] = job_id
    if active_only is not None:
        params["active_only"] = str(active_only).lower()
    if completed_only is not None:
        params["completed_only"] = str(completed_only).lower()
    if limit is not None:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        params["limit"] = limit
    if page_token is not None:
        params["page_token"] = page_token
    if run_type is not None:
        params["run_type"] = run_type
    if start_time_from is not None:
        params["start_time_from"] = start_time_from
    if start_time_to is not None:
        params["start_time_to"] = start_time_to
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error listing job runs: {response.status_code} - {response.text}")


def register_jobs_tools(mcp_instance):
    """Register Jobs service tools with the MCP server"""
    
    @mcp_instance.tool()
    def jobs_list(
        limit: Optional[int] = 20,
        expand_tasks: Optional[bool] = False,
        name: Optional[str] = None,
        page_token: Optional[str] = None
    ) -> dict:
        """
        Tool to list jobs in Databricks workspace.
        
        Returns jobs with optional filtering and pagination support.
        Use expand_tasks=True to include detailed task and cluster information.
        """
        try:
            result = list_jobs(
                limit=limit,
                expand_tasks=expand_tasks,
                name=name,
                page_token=page_token
            )
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @mcp_instance.tool()
    def jobs_get(job_id: int) -> dict:
        """
        Tool to get detailed information about a specific job.
        
        Returns complete job configuration including tasks, schedules, and clusters.
        """
        try:
            result = get_job(job_id)
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @mcp_instance.tool()
    def jobs_list_runs(
        job_id: Optional[int] = None,
        active_only: Optional[bool] = None,
        completed_only: Optional[bool] = None,
        limit: Optional[int] = 20,
        page_token: Optional[str] = None,
        run_type: Optional[str] = None,
        start_time_from: Optional[int] = None,
        start_time_to: Optional[int] = None
    ) -> dict:
        """
        Tool to list job runs with filtering and pagination.
        
        Filter by job_id, completion status, run type, or time range.
        Times should be in epoch milliseconds.
        """
        try:
            result = list_job_runs(
                job_id=job_id,
                active_only=active_only,
                completed_only=completed_only,
                limit=limit,
                page_token=page_token,
                run_type=run_type,
                start_time_from=start_time_from,
                start_time_to=start_time_to
            )
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}