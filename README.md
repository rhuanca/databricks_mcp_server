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
python -m databricks_mcp_server.server
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
- `DATABRICKS_HOST` - Your Databricks workspace hostname
- `DATABRICKS_TOKEN` - Personal access token or service principal token

## Migration from Individual Servers

### Old Structure
- `databricks_sql_mcp_server/` → `databricks_mcp_server/services/sql_service.py`
- `databricks_uc_mcp_server/` → `databricks_mcp_server/services/uc_service.py`  
- `databricks_ws_mcp_server/` → `databricks_mcp_server/services/ws_service.py`

### Import Changes
```python
# Old imports
from databricks_sql_mcp_server.sqlutils import execute_sql_statement
from databricks_uc_mcp_server.uc_utils import list_unity_catalogs
from databricks_ws_mcp_server.wsutils import download_databricks_notebook

# New imports
from databricks_mcp_server.services.sql_service import execute_sql_statement
from databricks_mcp_server.services.uc_service import list_unity_catalogs
from databricks_mcp_server.services.ws_service import download_databricks_notebook
```

## Tool Naming Convention

All MCP tools use service-prefixed naming:
- `sql_*` for SQL operations
- `uc_*` for Unity Catalog operations  
- `ws_*` for Workspace operations

This prevents naming collisions and makes it clear which service provides each tool.