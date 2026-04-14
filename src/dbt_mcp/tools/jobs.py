from fastmcp import FastMCP

from ..client_registry import get_client
from ..dbt_client import DbtCloudClient

router = FastMCP("jobs")


@router.tool()
async def list_jobs(client: str) -> dict:
    """
    List all dbt Cloud jobs for the specified client.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    return await dbt.admin_get("jobs/", params={"project_id": cfg.project_id})


@router.tool()
async def get_job_run_details(client: str, run_id: int) -> dict:
    """
    Get full details for a specific job run including timing, steps, and status.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        run_id: The dbt Cloud run ID to fetch
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    return await dbt.admin_get(
        f"runs/{run_id}/",
        params={"include_related": ["trigger", "job", "debug_logs"]},
    )


@router.tool()
async def get_job_run_error(client: str, run_id: int) -> dict:
    """
    Get error details and node-level failure information for a failed job run.
    Fetches run_results.json artifacts which contain test and model-level errors.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        run_id: The dbt Cloud run ID to inspect
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    return await dbt.admin_get(f"runs/{run_id}/artifacts/run_results.json")
