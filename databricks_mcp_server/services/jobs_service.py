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


def register_jobs_tools(server, tools, tool_handlers):
    """Register Jobs service tools with the MCP server"""
    
    async def list_jobs_tools():
        """List available Jobs tools."""
        from mcp.types import Tool
        return [
            Tool(
                name="jobs_list",
                description="List jobs in Databricks workspace with optional filtering and pagination support.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "default": 20,
                            "description": "Number of jobs to return (1-100, default 20)"
                        },
                        "expand_tasks": {
                            "type": "boolean",
                            "default": False,
                            "description": "Include task and cluster details in response"
                        },
                        "name": {
                            "type": "string",
                            "description": "Filter by exact job name (case insensitive)"
                        },
                        "page_token": {
                            "type": "string",
                            "description": "Token for pagination from previous request"
                        }
                    }
                }
            ),
            Tool(
                name="jobs_get",
                description="Get detailed information about a specific job including tasks, schedules, and clusters.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "integer",
                            "description": "The unique identifier of the job"
                        }
                    },
                    "required": ["job_id"]
                }
            ),
            Tool(
                name="jobs_list_runs",
                description="List job runs with filtering and pagination. Filter by job_id, completion status, run type, or time range.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "integer",
                            "description": "Filter runs for specific job ID"
                        },
                        "active_only": {
                            "type": "boolean",
                            "description": "Filter to only active runs"
                        },
                        "completed_only": {
                            "type": "boolean",
                            "description": "Filter to only completed runs"
                        },
                        "limit": {
                            "type": "integer",
                            "default": 20,
                            "description": "Number of runs to return (1-1000, default 20)"
                        },
                        "page_token": {
                            "type": "string",
                            "description": "Token for pagination"
                        },
                        "run_type": {
                            "type": "string",
                            "description": "Filter by run type (JOB_RUN, WORKFLOW_RUN, SUBMIT_RUN)"
                        },
                        "start_time_from": {
                            "type": "integer",
                            "description": "Filter runs started after this time (epoch ms)"
                        },
                        "start_time_to": {
                            "type": "integer",
                            "description": "Filter runs started before this time (epoch ms)"
                        }
                    }
                }
            )
        ]
    
    async def handle_jobs_list(arguments: dict):
        """Handle job listing."""
        from mcp.types import TextContent
        
        try:
            result = list_jobs(
                limit=arguments.get("limit", 20),
                expand_tasks=arguments.get("expand_tasks", False),
                name=arguments.get("name"),
                page_token=arguments.get("page_token")
            )
            return [TextContent(type="text", text=f"Jobs listed successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to list jobs: {str(e)}")]
    
    async def handle_jobs_get(arguments: dict):
        """Handle job details retrieval."""
        from mcp.types import TextContent
        
        try:
            result = get_job(arguments["job_id"])
            return [TextContent(type="text", text=f"Job details retrieved successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to get job details: {str(e)}")]
    
    async def handle_jobs_list_runs(arguments: dict):
        """Handle job runs listing."""
        from mcp.types import TextContent
        
        try:
            result = list_job_runs(
                job_id=arguments.get("job_id"),
                active_only=arguments.get("active_only"),
                completed_only=arguments.get("completed_only"),
                limit=arguments.get("limit", 20),
                page_token=arguments.get("page_token"),
                run_type=arguments.get("run_type"),
                start_time_from=arguments.get("start_time_from"),
                start_time_to=arguments.get("start_time_to")
            )
            return [TextContent(type="text", text=f"Job runs listed successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to list job runs: {str(e)}")]
    
    # Register the tools and handlers
    tools.append(list_jobs_tools)
    tool_handlers["jobs_list"] = handle_jobs_list
    tool_handlers["jobs_get"] = handle_jobs_get
    tool_handlers["jobs_list_runs"] = handle_jobs_list_runs