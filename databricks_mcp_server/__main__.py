"""
Entry point for running the Databricks MCP server as a module.
Usage: python -m databricks_mcp_server
"""
from databricks_mcp_server.server import mcp

if __name__ == "__main__":
    mcp.run()