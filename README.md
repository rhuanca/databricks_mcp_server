# Databricks Unified MCP Server

A federated Model Context Protocol server that provides unified access to Databricks services.

## Architecture

```
databricks_mcp_server/
├── __init__.py
├── server.py              # Main unified MCP server
├── common/                # Shared utilities
│   ├── auth.py           # Authentication helpers
│   └── config.py         # Configuration management
└── services/             # Individual service modules
    ├── sql_service.py    # SQL statement execution
    ├── uc_service.py     # Unity Catalog operations
    └── ws_service.py     # Workspace management
```

## Services and Tools

### SQL Service (`sql_*`)
- `sql_execute_statement()` - Execute SQL queries using Databricks SQL warehouse

### Unity Catalog Service (`uc_*`)
- `uc_list_catalogs()` - List available catalogs
- `uc_list_tables()` - List tables in a catalog/schema
- `uc_get_table_info()` - Get detailed table metadata and S3 paths

### Workspace Service (`ws_*`)
- `ws_download_notebook()` - Download notebook source code
- `ws_list_contents()` - List workspace directory contents
- `ws_get_status()` - Get workspace object status

## Usage

### As MCP Server (requires Python 3.10+)
```bash
# Run with stdio transport (default)
uv run python -m databricks_mcp_server

# Run with streamable HTTP transport (default port 8000)
uv run python -m databricks_mcp_server --transport streamable-http

# Run with streamable HTTP transport on custom port
uv run python -m databricks_mcp_server --transport streamable-http --port 3000

```

### With Docker
```bash
# Build the Docker image
docker build -t databricks-mcp-server .

# Run with default settings (streamable-http on port 8000)
docker run -p 8000:8000 \
  -e DATABRICKS_HOST="https://your-workspace.cloud.databricks.com" \
  -e DATABRICKS_TOKEN="your-token" \
  databricks-mcp-server

# Run on custom port (e.g., 3000)
docker run -p 3000:3000 \
  -e DATABRICKS_HOST="https://your-workspace.cloud.databricks.com" \
  -e DATABRICKS_TOKEN="your-token" \
  -e MCP_PORT="3000" \
  databricks-mcp-server

# Run with stdio transport (for terminal use)
docker run -it \
  -e DATABRICKS_HOST="https://your-workspace.cloud.databricks.com" \
  -e DATABRICKS_TOKEN="your-token" \
  -e MCP_TRANSPORT="stdio" \
  databricks-mcp-server

# For MCP client configurations (Claude, etc.) - use -i only, not -it
docker run -i \
  -e DATABRICKS_HOST="https://your-workspace.cloud.databricks.com" \
  -e DATABRICKS_TOKEN="your-token" \
  -e MCP_TRANSPORT="stdio" \
  databricks-mcp-server
```

## MCP Client Configuration

### Claude Desktop

Add to your Claude Desktop configuration file (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS or `%APPDATA%\Claude\claude_desktop_config.json` on Windows):

```json
{
  "mcpServers": {
    "databricks_mcp_server": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "-e",
        "DATABRICKS_HOST=https://your-workspace.cloud.databricks.com",
        "-e",
        "DATABRICKS_TOKEN=dapi1234567890abcdef",
        "-e",
        "MCP_TRANSPORT=stdio",
        "databricks-mcp-server"
      ]
    }
  }
}
```

**Important Notes:**
- Use `-i` flag only (not `-it`) for MCP clients
- Docker daemon must be running before starting Claude
- Replace `DATABRICKS_HOST` with your workspace URL and include the scheme `https://` (for example: `https://your-workspace.cloud.databricks.com`)
- Replace `DATABRICKS_TOKEN` with your Databricks personal access token
- Restart Claude Desktop after updating the configuration

### Alternative: Direct Python Execution

If you prefer not to use Docker:

```json
{
  "mcpServers": {
    "databricks_mcp_server": {
      "command": "uv",
      "args": ["run", "python", "-m", "databricks_mcp_server"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi1234567890abcdef"
      }
    }
  }
}
```

## MCP Inspector
```bash
# Test with MCP Inspector (stdio mode)
npx -y @modelcontextprotocol/inspector uv run python -m databricks_mcp_server

# Test streamable-http with browser
# 1. Start server: uv run python -m databricks_mcp_server --transport streamable-http
# 2. Open inspector: npx -y @modelcontextprotocol/inspector
# 3. In the inspector UI, connect using:
#    - Transport: Streamable HTTP
#    - URL: http://localhost:8000
#
# If using Docker:
# 1. Start server: docker run -p 8000:8000 -e DATABRICKS_HOST="..." -e DATABRICKS_TOKEN="..." databricks-mcp-server
# 2. Open inspector: npx -y @modelcontextprotocol/inspector
# 3. In the inspector UI, connect using:
#    - Transport: Streamable HTTP
#    - URL: http://localhost:8000
```

### As Python Library
```python
from databricks_mcp_server.services.sql_service import execute_sql_statement
from databricks_mcp_server.services.uc_service import list_unity_tables
from databricks_mcp_server.services.ws_service import download_databricks_notebook

# Execute SQL
result = execute_sql_statement("SELECT * FROM catalog.schema.table", warehouse_id="abc123")

# List Unity Catalog tables
tables = list_unity_tables("catalog_name", "schema_name")

# Download notebook
content = download_databricks_notebook("/Workspace/path/to/notebook")
```

## Environment Variables

Required for all services:
- `DATABRICKS_HOST` - Your Databricks workspace URL including scheme, e.g. `https://your-workspace.cloud.databricks.com`
- `DATABRICKS_TOKEN` - Personal access token or service principal token

## Tool Naming Convention

All MCP tools use service-prefixed naming:
- `sql_*` for SQL operations
- `uc_*` for Unity Catalog operations  
- `ws_*` for Workspace operations

This prevents naming collisions and makes it clear which service provides each tool.
