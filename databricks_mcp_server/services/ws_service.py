"""
Workspace Service for Databricks MCP Server

Handles Databricks workspace operations like downloading notebooks and listing contents.
"""
import base64
import requests
from typing import Optional, Dict, Any
from databricks_mcp_server.common.auth import get_databricks_base_url, get_databricks_headers


def download_databricks_notebook(
    path: str, 
    format: str = "SOURCE", 
    direct_download: bool = False
) -> Optional[bytes]:
    """
    Downloads a Databricks notebook or directory.

    Args:
        path (str): The absolute path of the notebook or directory to export.
        format (str): The format of the exported file. Default is "SOURCE".
        direct_download (bool): If True, the response is the exported file itself. Default is False.

    Returns:
        Optional[bytes]: The content of the exported file if successful, None otherwise.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()

    url = f"{base_url}/api/2.0/workspace/export"
    params = {
        "path": path,
        "format": format,
        "direct_download": str(direct_download).lower()
    }

    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        if direct_download:
            return response.content
        else:
            response_data = response.json()
            content = response_data.get("content")
            if content:
                # Content is base64 encoded, decode it
                return base64.b64decode(content)
            return None
    else:
        raise Exception(f"Error downloading notebook: {response.status_code} - {response.text}")


def get_workspace_status(path: str) -> Dict[str, Any]:
    """
    Gets the status of a workspace object or directory.

    Args:
        path (str): The absolute path of the notebook or directory.

    Returns:
        Dict[str, Any]: The response from the Workspace Get Status API.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.0/workspace/get-status"
    params = {"path": path}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
        return {
            "error_code": error_data.get("error_code", "UNKNOWN_ERROR"), 
            "message": error_data.get("message", response.text)
        }


def list_workspace_contents(
    path: str, 
    notebooks_modified_after: Optional[int] = None
) -> Dict[str, Any]:
    """
    Lists the contents of a directory or object in the Databricks workspace.

    Args:
        path (str): The absolute path of the notebook or directory.
        notebooks_modified_after (int, optional): UTC timestamp in milliseconds to filter notebooks modified after this time.

    Returns:
        Dict[str, Any]: The response from the Workspace List API.
    """
    base_url = get_databricks_base_url()
    headers = get_databricks_headers()
    
    url = f"{base_url}/api/2.0/workspace/list"
    params = {"path": path}
    
    if notebooks_modified_after is not None:
        params["notebooks_modified_after"] = str(notebooks_modified_after)
        
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
        return {
            "error_code": error_data.get("error_code", "UNKNOWN_ERROR"), 
            "message": error_data.get("message", response.text)
        }


def register_ws_tools(mcp_instance):
    """Register Workspace service tools with the MCP server"""
    
    @mcp_instance.tool()
    def ws_download_notebook(
        path: str, 
        format: str = "SOURCE", 
        direct_download: bool = False
    ) -> dict:
        """
        Tool to download a notebook from Databricks workspace.
        """
        try:
            content = download_databricks_notebook(
                path=path, 
                format=format, 
                direct_download=direct_download
            )
            if content:
                # Convert bytes to string for JSON serialization
                content_str = content.decode('utf-8') if isinstance(content, bytes) else content
                return {"status": "success", "content": content_str}
            else:
                return {"status": "error", "message": "No content returned"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @mcp_instance.tool()
    def ws_get_status(path: str) -> dict:
        """
        Tool to get the status of a workspace object or directory.
        """
        try:
            result = get_workspace_status(path)
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @mcp_instance.tool()
    def ws_list_contents(
        path: str, 
        notebooks_modified_after: Optional[int] = None
    ) -> dict:
        """
        Tool to list contents of workspace directory.
        """
        try:
            result = list_workspace_contents(
                path=path, 
                notebooks_modified_after=notebooks_modified_after
            )
            return {"status": "success", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}