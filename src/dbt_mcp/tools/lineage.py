from fastmcp import FastMCP

from ..client_registry import get_client
from ..dbt_client import DbtCloudClient

router = FastMCP("lineage")


@router.tool()
async def get_lineage(client: str, node_unique_id: str, depth: int = 3) -> dict:
    """
    Get the full lineage graph for a model (upstream and downstream dependencies).

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        node_unique_id: Full unique ID of the node (e.g. 'model.project.model_name')
        depth: How many levels of lineage to traverse (default: 3)
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetLineage($environmentId: BigInt!, $uniqueId: String!) {
        environment(id: $environmentId) {
            definition {
                resourceNodes(uniqueIds: [$uniqueId]) {
                    ... on ModelNode {
                        uniqueId
                        name
                        parents {
                            uniqueId
                            name
                            resourceType
                        }
                        children {
                            uniqueId
                            name
                            resourceType
                        }
                    }
                }
            }
        }
    }
    """
    return await dbt.discovery_query(
        query,
        {"environmentId": cfg.environment_id, "uniqueId": node_unique_id},
    )


@router.tool()
async def get_exposure_details(client: str, exposure_name: str | None = None) -> dict:
    """
    Get details about dbt exposures (downstream consumers like dashboards or reports).

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        exposure_name: Optional specific exposure name. Omit for all exposures.
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetExposures($environmentId: BigInt!) {
        environment(id: $environmentId) {
            definition {
                exposures(first: 500) {
                    edges {
                        node {
                            uniqueId
                            name
                            description
                            exposureType
                            url
                            ownerName
                            ownerEmail
                            parents {
                                uniqueId
                                name
                                resourceType
                            }
                        }
                    }
                }
            }
        }
    }
    """
    result = await dbt.discovery_query(query, {"environmentId": cfg.environment_id})
    if exposure_name:
        exposures = (
            result.get("environment", {})
            .get("definition", {})
            .get("exposures", {})
            .get("edges", [])
        )
        filtered = [e for e in exposures if e["node"]["name"] == exposure_name]
        return {"edges": filtered}
    return result
