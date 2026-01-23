"""
Databricks Unified MCP Server

A federated Model Context Protocol server that provides access to:
- Databricks SQL Statement Execution
- Unity Catalog operations  
- Workspace management

All services use service-prefixed tool naming for clarity.
"""
import sys
import os
from typing import Optional

# Add parent directory to Python path to enable absolute imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp.server.fastmcp import FastMCP
from databricks_mcp_server.common.config import DatabricksConfig
from databricks_mcp_server.services.sql_service import register_sql_tools
from databricks_mcp_server.services.uc_service import register_uc_tools
from databricks_mcp_server.services.ws_service import register_ws_tools
from databricks_mcp_server.services.jobs_service import register_jobs_tools


def create_databricks_mcp_server(config: Optional[DatabricksConfig] = None) -> FastMCP:
    """
    Create and configure the unified Databricks MCP server.
    
    Args:
        config (DatabricksConfig, optional): Configuration for services. 
                                           Defaults to all services enabled.
    
    Returns:
        FastMCP: Configured MCP server instance
    """
    if config is None:
        config = DatabricksConfig()
    
    mcp = FastMCP(name="Databricks Unified MCP Server")
    
    # Register services based on configuration
    if config.is_service_enabled("sql"):
        register_sql_tools(mcp)
    
    if config.is_service_enabled("uc"):
        register_uc_tools(mcp)
        
    if config.is_service_enabled("ws"):
        register_ws_tools(mcp)
        
    if config.is_service_enabled("jobs"):
        register_jobs_tools(mcp)
    
    return mcp


# Create default server instance
mcp = create_databricks_mcp_server()


if __name__ == "__main__":
    mcp.run()