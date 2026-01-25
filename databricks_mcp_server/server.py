"""
Databricks Unified MCP Server

A federated Model Context Protocol server that provides access to:
- Databricks SQL Statement Execution
- Unity Catalog operations
- Workspace management
- Jobs management

All services use service-prefixed tool naming for clarity.
"""

from typing import Optional

from mcp.server import Server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    Implementation,
    InitializeRequest,
    InitializeResult,
    ListToolsRequest,
    ListToolsResult,
    PingRequest,
    ServerCapabilities,
    ToolsCapability,
)

from databricks_mcp_server.common.config import DatabricksConfig
from databricks_mcp_server.services.jobs_service import register_jobs_tools
from databricks_mcp_server.services.sql_service import register_sql_tools
from databricks_mcp_server.services.uc_service import register_uc_tools
from databricks_mcp_server.services.ws_service import register_ws_tools

# Constants
SERVER_NAME = "Databricks Unified MCP Server"
SERVER_VERSION = "0.1.0"
PROTOCOL_VERSION = "2024-11-05"


def create_databricks_mcp_server(config: Optional[DatabricksConfig] = None) -> Server:
    """
    Create and configure the unified Databricks MCP server.

    Args:
        config: Configuration for services. Defaults to all services enabled.

    Returns:
        Configured MCP server instance
    """
    if config is None:
        config = DatabricksConfig()

    server = Server(name=SERVER_NAME)
    tools = []
    tool_handlers = {}

    # Register services and collect tools/handlers
    service_registry = {
        "sql": register_sql_tools,
        "uc": register_uc_tools,
        "ws": register_ws_tools,
        "jobs": register_jobs_tools,
    }
    for service_name, register_func in service_registry.items():
        if config.is_service_enabled(service_name):
            register_func(server, tools, tool_handlers)

    # Register MCP protocol handlers
    _register_protocol_handlers(server, tools, tool_handlers)

    return server


def _register_protocol_handlers(
    server: Server,
    tools: list,
    tool_handlers: dict
) -> None:
    """Register MCP protocol request handlers."""

    async def handle_initialize(request: InitializeRequest) -> InitializeResult:
        return InitializeResult(
            serverInfo=Implementation(
                name=SERVER_NAME,
                version=SERVER_VERSION
            ),
            capabilities=ServerCapabilities(
                tools=ToolsCapability()
            ),
            protocolVersion=PROTOCOL_VERSION
        )

    async def handle_ping(request: PingRequest) -> None:
        return None

    async def handle_list_tools(request: ListToolsRequest) -> ListToolsResult:
        all_tools = []
        for tool_list_func in tools:
            tool_list = await tool_list_func()
            all_tools.extend(tool_list)
        return ListToolsResult(tools=all_tools)

    async def handle_call_tool(request: CallToolRequest) -> CallToolResult:
        name = request.params.name
        arguments = request.params.arguments
        if name not in tool_handlers:
            raise ValueError(f"Unknown tool: {name}")
        content = await tool_handlers[name](arguments)
        return CallToolResult(content=content)

    # Register handlers
    handlers = {
        InitializeRequest: handle_initialize,
        PingRequest: handle_ping,
        ListToolsRequest: handle_list_tools,
        CallToolRequest: handle_call_tool,
    }
    server.request_handlers.update(handlers)



