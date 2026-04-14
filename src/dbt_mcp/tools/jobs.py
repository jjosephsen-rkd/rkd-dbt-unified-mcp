import asyncio

from fastmcp import FastMCP

from ..client_registry import get_client, list_clients
from ..dbt_client import DbtCloudClient

router = FastMCP("jobs")

_BATCH_SIZE = 6


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
async def list_all_jobs() -> dict:
    """
    List jobs for every configured client in batches to avoid rate limiting.
    Use this instead of calling list_jobs() repeatedly when you need a full
    cross-client overview. Returns a dict keyed by client identifier.
    """
    all_clients = list_clients()
    results = {}

    for i in range(0, len(all_clients), _BATCH_SIZE):
        batch = all_clients[i : i + _BATCH_SIZE]

        async def fetch(client_id: str) -> tuple[str, dict]:
            try:
                data = await list_jobs(client_id)
                return client_id, {"status": "ok", "data": data}
            except Exception as e:
                return client_id, {"status": "error", "error": str(e)}

        batch_results = await asyncio.gather(*[fetch(c) for c in batch])
        results.update(dict(batch_results))

    return results


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
