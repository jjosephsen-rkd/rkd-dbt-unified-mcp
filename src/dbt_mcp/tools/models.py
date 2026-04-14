from fastmcp import FastMCP

from ..client_registry import get_client
from ..dbt_client import DbtCloudClient

router = FastMCP("models")

# Discovery API GraphQL fragments for model queries
_MODEL_FIELDS = """
    uniqueId
    name
    description
    status
    executionTime
    runGeneratedAt
    materializedType
    schema
    database
"""


@router.tool()
async def get_mart_models(client: str) -> dict:
    """
    List all mart-layer models for the client's dbt project.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetMartModels($environmentId: BigInt!) {
        environment(id: $environmentId) {
            applied {
                models(first: 500, filter: { tags: ["mart"] }) {
                    edges {
                        node {
                            uniqueId
                            name
                            description
                            schema
                            materializedType
                        }
                    }
                }
            }
        }
    }
    """
    return await dbt.discovery_query(query, {"environmentId": cfg.environment_id})


@router.tool()
async def get_model_health(client: str, model_name: str | None = None) -> dict:
    """
    Get health status (test results, freshness, errors) for models.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        model_name: Optional specific model name to filter to. Omit for all models.
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetModelHealth($environmentId: BigInt!, $filter: ModelAppliedFilter) {
        environment(id: $environmentId) {
            applied {
                models(first: 500, filter: $filter) {
                    edges {
                        node {
                            uniqueId
                            name
                            executionInfo {
                                lastRunStatus
                                lastRunError
                                executeCompletedAt
                                executionTime
                            }
                            tests {
                                name
                                lastKnownResult
                            }
                        }
                    }
                }
            }
        }
    }
    """
    variables: dict = {"environmentId": cfg.environment_id}
    if model_name:
        variables["filter"] = {"uniqueIds": [f"model.{model_name}"]}
    return await dbt.discovery_query(query, variables)


@router.tool()
async def get_model_performance(client: str, job_id: int | None = None) -> dict:
    """
    Get execution time and performance metrics for models.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        job_id: Optional job ID to scope performance metrics to a specific job
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetModelPerformance($environmentId: BigInt!) {
        environment(id: $environmentId) {
            applied {
                models(first: 500) {
                    edges {
                        node {
                            uniqueId
                            name
                            materializedType
                            executionInfo {
                                executionTime
                                executeCompletedAt
                                lastRunStatus
                                lastRunId
                            }
                        }
                    }
                }
            }
        }
    }
    """
    return await dbt.discovery_query(query, {"environmentId": cfg.environment_id})


@router.tool()
async def get_model_parents(client: str, model_unique_id: str) -> dict:
    """
    Get the parent nodes (upstream dependencies) of a specific model.

    Args:
        client: Client identifier (e.g. 'FCCTX', 'HFBTX', 'NMSS')
        model_unique_id: Full unique ID of the model
            (e.g. 'model.project_name.model_name')
    """
    cfg = get_client(client)
    dbt = DbtCloudClient(cfg)
    query = """
    query GetModelParents($environmentId: BigInt!, $uniqueId: String!) {
        environment(id: $environmentId) {
            applied {
                modelHistoricalRuns(uniqueId: $uniqueId) {
                    parentsModels {
                        uniqueId
                        name
                        resourceType
                    }
                    parentsSources {
                        uniqueId
                        name
                        sourceName
                    }
                }
            }
        }
    }
    """
    return await dbt.discovery_query(
        query,
        {"environmentId": cfg.environment_id, "uniqueId": model_unique_id},
    )
