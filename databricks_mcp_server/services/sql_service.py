"""
SQL Service for Databricks MCP Server

Handles SQL statement execution and warehouse management.
"""
import requests
from typing import Optional, List, Dict, Any
from databricks_mcp_server.common.auth import get_databricks_base_url, get_databricks_headers


def execute_sql_statement(
    statement: str,
    warehouse_id: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    disposition: str = "INLINE",
    format: str = "JSON_ARRAY",
    wait_timeout: str = "10s",
    on_wait_timeout: str = "CONTINUE",
    parameters: Optional[List[Dict[str, Any]]] = None,
    byte_limit: Optional[int] = None,
    row_limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Executes a SQL statement using the Databricks SQL Statement Execution API.

    Args:
        statement (str): The SQL statement to execute.
        warehouse_id (str): The SQL warehouse ID.
        catalog (Optional[str]): Default catalog for execution.
        schema (Optional[str]): Default schema for execution.
        disposition (str): INLINE or EXTERNAL_LINKS.
        format (str): JSON_ARRAY, ARROW_STREAM, or CSV.
        wait_timeout (str): Timeout for waiting for results.
        on_wait_timeout (str): CONTINUE or CANCEL.
        parameters (Optional[List[Dict[str, Any]]]): List of parameters for parameterized SQL.
        byte_limit (Optional[int]): Byte limit for result size.
        row_limit (Optional[int]): Row limit for result set.

    Returns:
        Dict[str, Any]: The response from the API.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    headers["Content-Type"] = "application/json"
    
    url = f"{base_url}/api/2.0/sql/statements/"
    
    payload: Dict[str, Any] = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "disposition": disposition,
        "format": format,
        "wait_timeout": wait_timeout,
        "on_wait_timeout": on_wait_timeout
    }
    
    if catalog:
        payload["catalog"] = catalog
    if schema:
        payload["schema"] = schema
    if parameters:
        payload["parameters"] = parameters
    if byte_limit is not None:
        payload["byte_limit"] = str(byte_limit)
    if row_limit is not None:
        payload["row_limit"] = str(row_limit)
        
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def register_sql_tools(mcp_instance):
    """Register SQL service tools with the MCP server"""
    
    @mcp_instance.tool()
    def sql_execute_statement(
        statement: str,
        warehouse_id: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        disposition: str = "INLINE",
        format: str = "JSON_ARRAY",
        wait_timeout: str = "10s",
        on_wait_timeout: str = "CONTINUE",
        parameters: Optional[List[Dict[str, Any]]] = None,
        byte_limit: Optional[int] = None,
        row_limit: Optional[int] = None
    ) -> dict:
        """
        Tool to execute a SQL statement using Databricks SQL Statement Execution API.
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
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}