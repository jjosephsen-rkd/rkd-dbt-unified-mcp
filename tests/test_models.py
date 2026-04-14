from src.dbt_mcp.tools.models import (
    get_mart_models,
    get_model_health,
    get_model_parents,
    get_model_performance,
)

MODELS_RESP = {"environment": {"applied": {"models": {"edges": []}}}}


async def test_get_mart_models(mock_dbt_discovery):
    mock_dbt_discovery.return_value = MODELS_RESP
    result = await get_mart_models("TESTCLIENT")
    assert "environment" in result
    mock_dbt_discovery.assert_called_once()


async def test_get_model_health_all(mock_dbt_discovery):
    mock_dbt_discovery.return_value = MODELS_RESP
    result = await get_model_health("TESTCLIENT")
    assert "environment" in result


async def test_get_model_health_filtered(mock_dbt_discovery):
    mock_dbt_discovery.return_value = MODELS_RESP
    await get_model_health("TESTCLIENT", model_name="my_model")
    call_kwargs = mock_dbt_discovery.call_args[0][1]
    assert "filter" in call_kwargs


async def test_get_model_performance(mock_dbt_discovery):
    mock_dbt_discovery.return_value = MODELS_RESP
    result = await get_model_performance("TESTCLIENT")
    assert "environment" in result


async def test_get_model_parents(mock_dbt_discovery):
    mock_dbt_discovery.return_value = {
        "environment": {"applied": {"modelHistoricalRuns": []}}
    }
    result = await get_model_parents("TESTCLIENT", "model.project.my_model")
    assert "environment" in result
