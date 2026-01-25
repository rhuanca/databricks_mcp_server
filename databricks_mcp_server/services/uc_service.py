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
    
    @mcp_instance.tool()
    def uc_list_catalogs(
        include_browse: bool = False, 
        max_results: Optional[int] = None, 
        page_token: Optional[str] = None
    ) -> dict:
        """
        Tool to list catalogs in the Unity Catalog metastore.
        """
        try:
            result = list_unity_catalogs(
                include_browse=include_browse, 
                max_results=max_results, 
                page_token=page_token
            )
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @mcp_instance.tool()
    def uc_list_tables(
        catalog_name: str,
        schema_name: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        omit_columns: Optional[bool] = None,
        omit_properties: Optional[bool] = None,
        omit_username: Optional[bool] = None,
        include_browse: Optional[bool] = None,
        include_manifest_capabilities: Optional[bool] = None,
    ) -> dict:
        """
        Tool to list tables in a given Unity Catalog catalog and schema.
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
                include_manifest_capabilities=include_manifest_capabilities,
            )
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @mcp_instance.tool()
    def uc_get_table_info(full_table_name: str) -> dict:
        """
        Tool to get detailed information about a Unity Catalog table.
        """
        try:
            result = get_unity_table_info(full_table_name)
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}
