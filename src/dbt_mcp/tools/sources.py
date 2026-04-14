from fastmcp import FastMCP

from ..client_registry import get_client
from ..dbt_client import DbtCloudClient

router = FastMCP("sources")


@router.tool()
async def get_all_sources(client: str, include_freshness: bool = True) -> dict:
    """
    Get all dbt sources for the client including freshness status.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        include_freshness: Whether to include source freshness status (default: True)
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetAllSources($environmentId: BigInt!) {
        environment(id: $environmentId) {
            applied {
                sources(first: 500) {
                    edges {
                        node {
                            uniqueId
                            name
                            sourceName
                            description
                            schema
                            database
                            freshness {
                                freshnessChecked
                                freshnessStatus
                                maxLoadedAt
                                snapshottedAt
                                freshnessRunGeneratedAt
                            }
                        }
                    }
                }
            }
        }
    }
    """
    return await dbt.discovery_query(query, {"environmentId": cfg.environment_id})
