"""
SQL Service for Databricks MCP Server

Handles SQL statement execution and warehouse management.
"""

from typing import Dict, Any, List, Optional

import requests

from databricks_mcp_server.common.auth import get_databricks_base_url, get_databricks_headers


# Constants
DEFAULT_WAIT_TIMEOUT = "10s"
DEFAULT_ON_WAIT_TIMEOUT = "CONTINUE"
DEFAULT_DISPOSITION = "INLINE"
DEFAULT_FORMAT = "JSON_ARRAY"


def execute_sql_statement(
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
) -> Dict[str, Any]:
    """
    Execute a SQL statement using the Databricks SQL Statement Execution API.

    Args:
        statement: The SQL statement to execute.
        warehouse_id: The SQL warehouse ID.
        catalog: Default catalog for execution.
        schema: Default schema for execution.
        disposition: INLINE or EXTERNAL_LINKS.
        format: JSON_ARRAY, ARROW_STREAM, or CSV.
        wait_timeout: Timeout for waiting for results.
        on_wait_timeout: CONTINUE or CANCEL.
        parameters: List of parameters for parameterized SQL.
        byte_limit: Byte limit for result size.
        row_limit: Row limit for result set.

    Returns:
        The response from the API.

    Raises:
        Exception: If the API call fails.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    headers["Content-Type"] = "application/json"

    url = f"{base_url}/api/2.0/sql/statements/"

    payload = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "disposition": disposition,
        "format": format,
        "wait_timeout": wait_timeout,
        "on_wait_timeout": on_wait_timeout
    }

    # Add optional parameters
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
        raise Exception(f"SQL execution failed: {response.status_code} - {response.text}")


def register_sql_tools(server) -> None:
    """Register SQL service tools with the MCP server."""

    async def list_sql_tools():
        """List available SQL tools."""
        from mcp.types import Tool

        return [
            Tool(
                name="sql_execute_statement",
                description="Execute a SQL statement using Databricks SQL Statement Execution API.",
                inputSchema=_get_sql_tool_schema()
            )
        ]

    async def handle_sql_execute_statement(arguments: dict):
        """Handle SQL statement execution."""
        from mcp.types import TextContent

        try:
            result = execute_sql_statement(
                statement=arguments["statement"],
                warehouse_id=arguments["warehouse_id"],
                catalog=arguments.get("catalog"),
                schema=arguments.get("schema"),
                disposition=arguments.get("disposition", DEFAULT_DISPOSITION),
                format=arguments.get("format", DEFAULT_FORMAT),
                wait_timeout=arguments.get("wait_timeout", DEFAULT_WAIT_TIMEOUT),
                on_wait_timeout=arguments.get("on_wait_timeout", DEFAULT_ON_WAIT_TIMEOUT),
                parameters=arguments.get("parameters"),
                byte_limit=arguments.get("byte_limit"),
                row_limit=arguments.get("row_limit")
            )
            return [TextContent(type="text", text=f"SQL execution successful: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"SQL execution failed: {str(e)}")]

    # Register the tools and handlers
    server._tools.append(list_sql_tools)
    server._tool_handlers["sql_execute_statement"] = handle_sql_execute_statement


def _get_sql_tool_schema() -> Dict[str, Any]:
    """Get the input schema for the SQL execution tool."""
    return {
        "type": "object",
        "properties": {
            "statement": {
                "type": "string",
                "description": "The SQL statement to execute"
            },
            "warehouse_id": {
                "type": "string",
                "description": "The SQL warehouse ID"
            },
            "catalog": {
                "type": "string",
                "description": "Default catalog for execution"
            },
            "schema": {
                "type": "string",
                "description": "Default schema for execution"
            },
            "disposition": {
                "type": "string",
                "enum": ["INLINE", "EXTERNAL_LINKS"],
                "default": DEFAULT_DISPOSITION,
                "description": "INLINE or EXTERNAL_LINKS"
            },
            "format": {
                "type": "string",
                "enum": ["JSON_ARRAY", "ARROW_STREAM", "CSV"],
                "default": DEFAULT_FORMAT,
                "description": "JSON_ARRAY, ARROW_STREAM, or CSV"
            },
            "wait_timeout": {
                "type": "string",
                "default": DEFAULT_WAIT_TIMEOUT,
                "description": "Timeout for waiting for results"
            },
            "on_wait_timeout": {
                "type": "string",
                "enum": ["CONTINUE", "CANCEL"],
                "default": DEFAULT_ON_WAIT_TIMEOUT,
                "description": "CONTINUE or CANCEL"
            },
            "parameters": {
                "type": "array",
                "items": {"type": "object"},
                "description": "List of parameters for parameterized SQL"
            },
            "byte_limit": {
                "type": "integer",
                "description": "Byte limit for result size"
            },
            "row_limit": {
                "type": "integer",
                "description": "Row limit for result set"
            }
        },
        "required": ["statement", "warehouse_id"]
    }