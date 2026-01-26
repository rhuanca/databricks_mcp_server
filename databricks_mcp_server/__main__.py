"""
Entry point for running the Databricks MCP server as a module.
Usage: python -m databricks_mcp_server [--transport {stdio,streamable-http}] [--port PORT] [--host HOST]
"""

import argparse
import asyncio

from databricks_mcp_server.server import create_databricks_mcp_server
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
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
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP server (default: 8000)"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host for HTTP server (default: 0.0.0.0)"
    )
    args = parser.parse_args()

    # Create default server instance
    server = create_databricks_mcp_server()

    print(f"Running Databricks MCP server in {args.transport} mode...")

    if args.transport == "stdio":
        _run_stdio_server(server)
    elif args.transport == "streamable-http":
        _run_http_server(server, args.host, args.port)


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


def _run_http_server(server, host: str, port: int) -> None:
    """Run the server with HTTP SSE transport."""
    from starlette.applications import Starlette
    from starlette.routing import Route
    import uvicorn
    
    async def run_http():
        sse = SseServerTransport(endpoint="/sse")
        init_options = InitializationOptions(
            server_name="Databricks Unified MCP Server",
            server_version="0.1.0",
            capabilities=ServerCapabilities(tools=ToolsCapability()),
        )
        
        async def handle_connection(request, connect_method):
            async with connect_method(request.scope, request.receive, request._send) as streams:
                await server.run(streams[0], streams[1], init_options)
        
        app = Starlette(
            routes=[
                Route("/sse", lambda req: handle_connection(req, sse.connect_sse), methods=["GET"]),
                Route("/messages", lambda req: handle_connection(req, sse.connect_messages), methods=["POST"]),
            ]
        )
        
        print(f"Starting HTTP server on {host}:{port}")
        print(f"SSE endpoint: http://{host}:{port}/sse")
        print(f"Messages endpoint: http://{host}:{port}/messages")
        
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        uvicorn_server = uvicorn.Server(config)
        await uvicorn_server.serve()
    
    asyncio.run(run_http())


if __name__ == "__main__":
    main()