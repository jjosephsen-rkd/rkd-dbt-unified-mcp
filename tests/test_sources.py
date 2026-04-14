from src.dbt_mcp.tools.sources import get_all_sources


async def test_get_all_sources(mock_dbt_discovery):
    mock_dbt_discovery.return_value = {
        "environment": {"applied": {"sources": {"edges": []}}}
    }
    result = await get_all_sources("TESTCLIENT")
    assert "environment" in result
    mock_dbt_discovery.assert_called_once()


async def test_get_all_sources_passes_environment_id(mock_dbt_discovery):
    mock_dbt_discovery.return_value = {
        "environment": {"applied": {"sources": {"edges": []}}}
    }
    await get_all_sources("TESTCLIENT")
    call_kwargs = mock_dbt_discovery.call_args[0][1]
    assert call_kwargs["environmentId"] == "888888"
