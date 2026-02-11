# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml README.md ./
COPY databricks_mcp_server ./databricks_mcp_server

# Install dependencies using uv
RUN uv pip install --system --no-cache -e .

# Expose port for streamable-http mode
EXPOSE 8000

# Set environment variables (can be overridden at runtime)
# NOTE: DATABRICKS_HOST must include the https:// scheme, e.g. https://your-workspace.cloud.databricks.com
ENV DATABRICKS_HOST="https://TOOVERWRITE"
ENV DATABRICKS_TOKEN="TOOVERWRITE"
ENV MCP_TRANSPORT="streamable-http"
ENV MCP_PORT="8000"
ENV MCP_HOST="0.0.0.0"

# Run the MCP server
CMD ["sh", "-c", "python -m databricks_mcp_server --transport ${MCP_TRANSPORT} --port ${MCP_PORT} --host ${MCP_HOST}"]
