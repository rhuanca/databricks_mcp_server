"""
Entry point for running the Databricks MCP server as a module.
Usage: python -m databricks_mcp_server [--transport {stdio,streamable-http}]
"""

import argparse
import asyncio
import sys

from databricks_mcp_server.server import create_databricks_mcp_server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, ToolsCapability


def main() -> None:
    """Main entry point for the Databricks MCP server."""
    parser = argparse.ArgumentParser(description="Run the Databricks MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="Transport mode for the MCP server (default: stdio)"
    )
    args = parser.parse_args()

    # Create default server instance
    server = create_databricks_mcp_server()

    print(f"Running Databricks MCP server in {args.transport} mode...")

    if args.transport == "stdio":
        _run_stdio_server(server)
    elif args.transport == "streamable-http":
        _run_http_server(server)


def _run_stdio_server(server) -> None:
    """Run the server with stdio transport."""

    async def run_stdio():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="Databricks Unified MCP Server",
                    server_version="0.1.0",
                    capabilities=ServerCapabilities(
                        tools=ToolsCapability()
                    ),
                ),
            )

    asyncio.run(run_stdio())


def _run_http_server(server) -> None:
    """Run the server with HTTP transport."""
    # TODO: Implement streamable HTTP transport
    print("Streamable HTTP transport not yet implemented")
    sys.exit(1)


if __name__ == "__main__":
    main()