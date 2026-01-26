"""
Databricks Unified MCP Server

A federated Model Context Protocol server that provides access to:
- Databricks SQL Statement Execution
- Unity Catalog operations
- Workspace management
- Jobs management

All services use service-prefixed tool naming for clarity.
"""

from typing import Optional, Dict, Any, List

from mcp.server.fastmcp import FastMCP

from databricks_mcp_server.services.sql_service import (
    execute_sql_statement, 
    DEFAULT_DISPOSITION, 
    DEFAULT_FORMAT, 
    DEFAULT_WAIT_TIMEOUT, 
    DEFAULT_ON_WAIT_TIMEOUT
)
from databricks_mcp_server.services.jobs_service import (
    list_jobs,
    get_job,
    list_job_runs
)
from databricks_mcp_server.services.uc_service import (
    list_unity_catalogs,
    list_unity_tables,
    get_unity_table_info
)
from databricks_mcp_server.services.ws_service import (
    download_databricks_notebook,
    get_workspace_status,
    list_workspace_contents
)

# Constants
SERVER_NAME = "Databricks Unified MCP Server"

# Create the default MCP server instance
# Note: This must be created before the decorators below are applied
mcp = FastMCP(SERVER_NAME)

# =====================
# SQL Service Tools
# =====================

@mcp.tool()
def sql_execute_statement(
    statement: str,
    warehouse_id: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    disposition: str = DEFAULT_DISPOSITION,
    format: str = DEFAULT_FORMAT,
    wait_timeout: str = DEFAULT_WAIT_TIMEOUT,
    on_wait_timeout: str = DEFAULT_ON_WAIT_TIMEOUT,
    parameters: Optional[List[Dict[str, Any]]] = None,
    byte_limit: Optional[int] = None,
    row_limit: Optional[int] = None
) -> str:
    """Execute a SQL statement using Databricks SQL Statement Execution API.
    
    Args:
        statement: The SQL statement to execute
        warehouse_id: The SQL warehouse ID
        catalog: Default catalog for execution
        schema: Default schema for execution
        disposition: INLINE or EXTERNAL_LINKS (default: INLINE)
        format: JSON_ARRAY, ARROW_STREAM, or CSV (default: JSON_ARRAY)
        wait_timeout: Timeout for waiting for results (default: 10s)
        on_wait_timeout: CONTINUE or CANCEL (default: CONTINUE)
        parameters: List of parameters for parameterized SQL
        byte_limit: Byte limit for result size
        row_limit: Row limit for result set
    
    Returns:
        str: SQL execution result as JSON string
    """
    try:
        result = execute_sql_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            disposition=disposition,
            format=format,
            wait_timeout=wait_timeout,
            on_wait_timeout=on_wait_timeout,
            parameters=parameters,
            byte_limit=byte_limit,
            row_limit=row_limit
        )
        return f"SQL execution successful: {result}"
    except Exception as e:
        return f"SQL execution failed: {str(e)}"


# =====================
# Jobs Service Tools
# =====================

@mcp.tool()
def jobs_list(
    limit: int = 20,
    expand_tasks: bool = False,
    name: Optional[str] = None,
    page_token: Optional[str] = None
) -> str:
    """List jobs in Databricks workspace with optional filtering and pagination support.
    
    Args:
        limit: Number of jobs to return (1-100, default 20)
        expand_tasks: Include task and cluster details in response
        name: Filter by exact job name (case insensitive)
        page_token: Token for pagination from previous request
    
    Returns:
        str: Jobs list as JSON string
    """
    try:
        result = list_jobs(
            limit=limit,
            expand_tasks=expand_tasks,
            name=name,
            page_token=page_token
        )
        return f"Jobs listed successfully: {result}"
    except Exception as e:
        return f"Failed to list jobs: {str(e)}"


@mcp.tool()
def jobs_get(job_id: int) -> str:
    """Get detailed information about a specific job including tasks, schedules, and clusters.
    
    Args:
        job_id: The unique identifier of the job
    
    Returns:
        str: Job details as JSON string
    """
    try:
        result = get_job(job_id)
        return f"Job details retrieved successfully: {result}"
    except Exception as e:
        return f"Failed to get job details: {str(e)}"


@mcp.tool()
def jobs_list_runs(
    job_id: Optional[int] = None,
    active_only: Optional[bool] = None,
    completed_only: Optional[bool] = None,
    limit: int = 20,
    page_token: Optional[str] = None,
    run_type: Optional[str] = None,
    start_time_from: Optional[int] = None,
    start_time_to: Optional[int] = None
) -> str:
    """List job runs with filtering and pagination. Filter by job_id, completion status, run type, or time range.
    
    Args:
        job_id: Filter runs for specific job ID
        active_only: Filter to only active runs
        completed_only: Filter to only completed runs
        limit: Number of runs to return (1-1000, default 20)
        page_token: Token for pagination
        run_type: Filter by run type (JOB_RUN, WORKFLOW_RUN, SUBMIT_RUN)
        start_time_from: Filter runs started after this time (epoch ms)
        start_time_to: Filter runs started before this time (epoch ms)
    
    Returns:
        str: Job runs list as JSON string
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
        return f"Job runs listed successfully: {result}"
    except Exception as e:
        return f"Failed to list job runs: {str(e)}"


# =====================
# Unity Catalog Tools
# =====================

@mcp.tool()
def uc_list_catalogs(
    include_browse: bool = False,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None
) -> str:
    """List catalogs in the Unity Catalog metastore.
    
    Args:
        include_browse: Whether to include browse-only catalogs
        max_results: Maximum number of catalogs to return
        page_token: Pagination token for next page
    
    Returns:
        str: Catalogs list as JSON string
    """
    try:
        result = list_unity_catalogs(
            include_browse=include_browse,
            max_results=max_results,
            page_token=page_token
        )
        return f"Catalogs listed successfully: {result}"
    except Exception as e:
        return f"Failed to list catalogs: {str(e)}"


@mcp.tool()
def uc_list_tables(
    catalog_name: str,
    schema_name: str,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
    omit_columns: Optional[bool] = None,
    omit_properties: Optional[bool] = None,
    omit_username: Optional[bool] = None,
    include_browse: Optional[bool] = None,
    include_manifest_capabilities: Optional[bool] = None
) -> str:
    """List tables in a given Unity Catalog catalog and schema.
    
    Args:
        catalog_name: Name of the parent catalog
        schema_name: Name of the parent schema
        max_results: Max number of tables to return
        page_token: Opaque token for pagination
        omit_columns: Whether to omit table columns from response
        omit_properties: Whether to omit table properties from response
        omit_username: Whether to omit usernames
        include_browse: Include tables with browse-only access
        include_manifest_capabilities: Include manifest capabilities in response
    
    Returns:
        str: Tables list as JSON string
    """
    try:
        result = list_unity_tables(
            catalog_name=catalog_name,
            schema_name=schema_name,
            max_results=max_results,
            page_token=page_token,
            omit_columns=omit_columns,
            omit_properties=omit_properties,
            omit_username=omit_username,
            include_browse=include_browse,
            include_manifest_capabilities=include_manifest_capabilities
        )
        return f"Tables listed successfully: {result}"
    except Exception as e:
        return f"Failed to list tables: {str(e)}"


@mcp.tool()
def uc_get_table_info(full_table_name: str) -> str:
    """Get detailed information about a Unity Catalog table.
    
    Args:
        full_table_name: Full table name (catalog.schema.table)
    
    Returns:
        str: Table info as JSON string
    """
    try:
        result = get_unity_table_info(full_table_name)
        return f"Table info retrieved successfully: {result}"
    except Exception as e:
        return f"Failed to get table info: {str(e)}"


# =====================
# Workspace Tools
# =====================

@mcp.tool()
def ws_download_notebook(
    path: str,
    format: str = "SOURCE",
    direct_download: bool = False
) -> str:
    """Download a notebook from Databricks workspace.
    
    Args:
        path: The absolute path of the notebook or directory to export
        format: The format of the exported file (default: SOURCE)
        direct_download: If True, the response is the exported file itself
    
    Returns:
        str: Notebook content
    """
    try:
        content = download_databricks_notebook(
            path=path,
            format=format,
            direct_download=direct_download
        )
        if content:
            content_str = content.decode('utf-8') if isinstance(content, bytes) else content
            return f"Notebook downloaded successfully: {content_str}"
        else:
            return "No content returned"
    except Exception as e:
        return f"Failed to download notebook: {str(e)}"


@mcp.tool()
def ws_get_status(path: str) -> str:
    """Get the status of a workspace object or directory.
    
    Args:
        path: The absolute path of the notebook or directory
    
    Returns:
        str: Workspace status as JSON string
    """
    try:
        result = get_workspace_status(path)
        return f"Workspace status retrieved successfully: {result}"
    except Exception as e:
        return f"Failed to get workspace status: {str(e)}"


@mcp.tool()
def ws_list_contents(
    path: str,
    notebooks_modified_after: Optional[int] = None
) -> str:
    """List contents of workspace directory.
    
    Args:
        path: The absolute path of the notebook or directory
        notebooks_modified_after: UTC timestamp in milliseconds to filter notebooks modified after this time
    
    Returns:
        str: Workspace contents as JSON string
    """
    try:
        result = list_workspace_contents(
            path=path,
            notebooks_modified_after=notebooks_modified_after
        )
        return f"Workspace contents listed successfully: {result}"
    except Exception as e:
        return f"Failed to list workspace contents: {str(e)}"



