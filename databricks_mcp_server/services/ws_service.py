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


def register_ws_tools(server, tools, tool_handlers):
    """Register Workspace service tools with the MCP server"""
    
    async def list_ws_tools():
        """List available Workspace tools."""
        from mcp.types import Tool
        return [
            Tool(
                name="ws_download_notebook",
                description="Download a notebook from Databricks workspace.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The absolute path of the notebook or directory to export"
                        },
                        "format": {
                            "type": "string",
                            "default": "SOURCE",
                            "description": "The format of the exported file"
                        },
                        "direct_download": {
                            "type": "boolean",
                            "default": False,
                            "description": "If True, the response is the exported file itself"
                        }
                    },
                    "required": ["path"]
                }
            ),
            Tool(
                name="ws_get_status",
                description="Get the status of a workspace object or directory.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The absolute path of the notebook or directory"
                        }
                    },
                    "required": ["path"]
                }
            ),
            Tool(
                name="ws_list_contents",
                description="List contents of workspace directory.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The absolute path of the notebook or directory"
                        },
                        "notebooks_modified_after": {
                            "type": "integer",
                            "description": "UTC timestamp in milliseconds to filter notebooks modified after this time"
                        }
                    },
                    "required": ["path"]
                }
            )
        ]
    
    async def handle_ws_download_notebook(arguments: dict):
        """Handle notebook download."""
        from mcp.types import TextContent
        
        try:
            content = download_databricks_notebook(
                path=arguments["path"],
                format=arguments.get("format", "SOURCE"),
                direct_download=arguments.get("direct_download", False)
            )
            if content:
                # Convert bytes to string for JSON serialization
                content_str = content.decode('utf-8') if isinstance(content, bytes) else content
                return [TextContent(type="text", text=f"Notebook downloaded successfully: {content_str}")]
            else:
                return [TextContent(type="text", text="No content returned")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to download notebook: {str(e)}")]
    
    async def handle_ws_get_status(arguments: dict):
        """Handle workspace status retrieval."""
        from mcp.types import TextContent
        
        try:
            result = get_workspace_status(arguments["path"])
            return [TextContent(type="text", text=f"Workspace status retrieved successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to get workspace status: {str(e)}")]
    
    async def handle_ws_list_contents(arguments: dict):
        """Handle workspace contents listing."""
        from mcp.types import TextContent
        
        try:
            result = list_workspace_contents(
                path=arguments["path"],
                notebooks_modified_after=arguments.get("notebooks_modified_after")
            )
            return [TextContent(type="text", text=f"Workspace contents listed successfully: {result}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to list workspace contents: {str(e)}")]
    
    # Register the tools and handlers
    tools.append(list_ws_tools)
    tool_handlers["ws_download_notebook"] = handle_ws_download_notebook
    tool_handlers["ws_get_status"] = handle_ws_get_status
    tool_handlers["ws_list_contents"] = handle_ws_list_contents