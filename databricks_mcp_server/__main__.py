"""
Entry point for running the Databricks MCP server as a module.
Usage: python -m databricks_mcp_server [--transport {stdio,streamable-http}] [--port PORT] [--host HOST]
"""

import argparse

from databricks_mcp_server.server import mcp


def main() -> None:
    """Main entry point for the Databricks MCP server."""
    parser = argparse.ArgumentParser(description="Run the Databricks MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="Transport mode for the MCP server (default: stdio)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP server (default: 8000)"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for HTTP server (default: 127.0.0.1)"
    )
    args = parser.parse_args()

    print(f"Running Databricks MCP server in {args.transport} mode...")

    # Update settings if host/port are specified
    if args.host != "127.0.0.1" or args.port != 8000:
        mcp.settings.host = args.host
        mcp.settings.port = args.port
    
    if args.transport == "stdio":
        mcp.run()
    elif args.transport == "streamable-http":
        mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()