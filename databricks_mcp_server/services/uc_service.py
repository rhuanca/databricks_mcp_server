"""
Unity Catalog Service for Databricks MCP Server

Handles Unity Catalog operations like listing catalogs, tables, and getting metadata.
"""
import requests
from typing import Optional, Dict, Any
from databricks_mcp_server.common.auth import get_databricks_base_url, get_databricks_headers


def list_unity_catalogs(
    include_browse: Optional[bool] = None, 
    max_results: Optional[int] = None, 
    page_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Lists catalogs in the Databricks Unity Catalog metastore.

    Args:
        include_browse (Optional[bool]): Whether to include browse-only catalogs.
        max_results (Optional[int]): Maximum number of catalogs to return.
        page_token (Optional[str]): Pagination token for next page.

    Returns:
        Dict[str, Any]: The response from the Unity Catalog API.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.1/unity-catalog/catalogs"
    params = {}
    
    if include_browse is not None:
        params["include_browse"] = str(include_browse).lower()
    if max_results is not None:
        params["max_results"] = max_results
    if page_token is not None:
        params["page_token"] = page_token
        
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def list_unity_tables(
    catalog_name: str,
    schema_name: str,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
    omit_columns: Optional[bool] = None,
    omit_properties: Optional[bool] = None,
    omit_username: Optional[bool] = None,
    include_browse: Optional[bool] = None,
    include_manifest_capabilities: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Lists tables under a given Unity Catalog catalog and schema.

    Args:
        catalog_name (str): Name of the parent catalog.
        schema_name (str): Name of the parent schema.
        max_results (Optional[int]): Max number of tables to return (<= 50 recommended to use paging).
        page_token (Optional[str]): Opaque token for pagination.
        omit_columns (Optional[bool]): Whether to omit table columns from response.
        omit_properties (Optional[bool]): Whether to omit table properties from response.
        omit_username (Optional[bool]): Whether to omit usernames (owner/created_by/updated_by).
        include_browse (Optional[bool]): Include tables with browse-only access.
        include_manifest_capabilities (Optional[bool]): Include manifest capabilities in response.

    Returns:
        Dict[str, Any]: Unity Catalog API response containing "tables" and optional "next_page_token".
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()

    url = f"{base_url}/api/2.1/unity-catalog/tables"
    params: Dict[str, Any] = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }
    
    if max_results is not None:
        params["max_results"] = max_results
    if page_token is not None:
        params["page_token"] = page_token
    if omit_columns is not None:
        params["omit_columns"] = str(omit_columns).lower()
    if omit_properties is not None:
        params["omit_properties"] = str(omit_properties).lower()
    if omit_username is not None:
        params["omit_username"] = str(omit_username).lower()
    if include_browse is not None:
        params["include_browse"] = str(include_browse).lower()
    if include_manifest_capabilities is not None:
        params["include_manifest_capabilities"] = str(include_manifest_capabilities).lower()

    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def get_unity_table_info(full_table_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a Unity Catalog table.
    
    Args:
        full_table_name (str): Full table name (catalog.schema.table)
        
    Returns:
        Dict[str, Any]: Table metadata including storage location, format, etc.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.1/unity-catalog/tables/{full_table_name}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def register_uc_tools(mcp_instance):
    """Register Unity Catalog service tools with the MCP server"""
    
    async def list_uc_tools():
        """List available Unity Catalog tools."""
        from mcp.types import Tool
        return [
            Tool(
                name="uc_list_catalogs",
                description="List catalogs in the Unity Catalog metastore.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "include_browse": {
                            "type": "boolean",
                            "default": False,
                            "description": "Whether to include browse-only catalogs"
                        },
                        "max_results": {
                            "type": "integer",
                            "description": "Maximum number of catalogs to return"
                        },
                        "page_token": {
                            "type": "string",
                            "description": "Pagination token for next page"
                        }
                    }
                }
            ),
            Tool(
                name="uc_list_tables",
                description="List tables in a given Unity Catalog catalog and schema.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the parent catalog"
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Name of the parent schema"
                        },
                        "max_results": {
                            "type": "integer",
                            "description": "Max number of tables to return"
                        },
                        "page_token": {
                            "type": "string",
                            "description": "Opaque token for pagination"
                        },
                        "omit_columns": {
                            "type": "boolean",
                            "description": "Whether to omit table columns from response"
                        },
                        "omit_properties": {
                            "type": "boolean",
                            "description": "Whether to omit table properties from response"
                        },
                        "omit_username": {
                            "type": "boolean",
                            "description": "Whether to omit usernames"
                        },
                        "include_browse": {
                            "type": "boolean",
                            "description": "Include tables with browse-only access"
                        },
                        "include_manifest_capabilities": {
                            "type": "boolean",
                            "description": "Include manifest capabilities in response"
                        }
                    },
                    "required": ["catalog_name", "schema_name"]
                }
            ),
            Tool(
                name="uc_get_table_info",
                description="Get detailed information about a Unity Catalog table.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "full_table_name": {
                            "type": "string",
                            "description": "Full table name (catalog.schema.table)"
                        }
                    },
                    "required": ["full_table_name"]
                }
            )
        ]
    
    async def handle_uc_list_catalogs(arguments: dict):
        """Handle catalog listing."""
        from mcp.types import TextContent
        
        try:
            result = list_unity_catalogs(
                include_browse=arguments.get("include_browse", False),
                max_results=arguments.get("max_results"),
                page_token=arguments.get("page_token")
            )
            return [TextContent(type="text", text=f"Catalogs listed successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to list catalogs: {str(e)}")]
    
    async def handle_uc_list_tables(arguments: dict):
        """Handle table listing."""
        from mcp.types import TextContent
        
        try:
            result = list_unity_tables(
                catalog_name=arguments["catalog_name"],
                schema_name=arguments["schema_name"],
                max_results=arguments.get("max_results"),
                page_token=arguments.get("page_token"),
                omit_columns=arguments.get("omit_columns"),
                omit_properties=arguments.get("omit_properties"),
                omit_username=arguments.get("omit_username"),
                include_browse=arguments.get("include_browse"),
                include_manifest_capabilities=arguments.get("include_manifest_capabilities")
            )
            return [TextContent(type="text", text=f"Tables listed successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to list tables: {str(e)}")]
    
    async def handle_uc_get_table_info(arguments: dict):
        """Handle table info retrieval."""
        from mcp.types import TextContent
        
        try:
            result = get_unity_table_info(arguments["full_table_name"])
            return [TextContent(type="text", text=f"Table info retrieved successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to get table info: {str(e)}")]
    
    # Register the tools and handlers
    mcp_instance._tools.append(list_uc_tools)
    mcp_instance._tool_handlers["uc_list_catalogs"] = handle_uc_list_catalogs
    mcp_instance._tool_handlers["uc_list_tables"] = handle_uc_list_tables
    mcp_instance._tool_handlers["uc_get_table_info"] = handle_uc_get_table_info
