"""
RKD Unified dbt MCP Server
Exposes 10 core dbt Cloud tools across all configured client environments.
Each tool accepts a `client` parameter to route to the correct project.
"""

from dotenv import load_dotenv
from fastmcp import FastMCP

from .client_registry import list_clients
from .tools import jobs, lineage, models, sources

load_dotenv()

mcp = FastMCP(
    name="RKD Unified dbt MCP",
    instructions=(
        "Multi-client dbt Cloud interface for all RKD client environments. "
        "Every tool requires a `client` parameter identifying the target client "
        "(e.g. 'FCCTX', 'BCMNY', 'HFBTX', 'NMSS', 'FTC', 'ABS', 'BFF'). "
        "Use list_jobs first to discover available jobs for a client."
    ),
)

mcp.mount(jobs.router)
mcp.mount(models.router)
mcp.mount(lineage.router)
mcp.mount(sources.router)


@mcp.tool()
def list_available_clients() -> dict:
    """List all configured client identifiers available in this MCP server."""
    return {"clients": list_clients()}


if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8000)
