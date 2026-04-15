"""
RKD Unified dbt MCP Server
Exposes 10 core dbt Cloud tools across all configured client environments.
Each tool accepts a `client` parameter to route to the correct project.
"""

from dotenv import load_dotenv
from fastmcp import FastMCP

from .client_registry import get_client_by_project_id, list_clients
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


@mcp.tool()
def resolve_client_from_webhook(project_id: str) -> dict:
    """
    Resolve a dbt Cloud project ID to a client identifier.

    Call this first when handling a dbt Cloud webhook payload to map the
    projectId field to the client slug required by all other MCP tools.

    Args:
        project_id: The dbt Cloud project ID from the webhook payload
                    (e.g. '211140592273239')

    Returns:
        {"client": "LSSOH", "project_id": "211140592273239"}
    """
    client_id = get_client_by_project_id(project_id)
    return {"client": client_id, "project_id": project_id}


if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8000)
