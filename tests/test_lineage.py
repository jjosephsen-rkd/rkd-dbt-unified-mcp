from src.dbt_mcp.tools.lineage import get_exposure_details, get_lineage


async def test_get_lineage(mock_dbt_discovery):
    mock_dbt_discovery.return_value = {
        "environment": {"definition": {"resourceNodes": []}}
    }
    result = await get_lineage("TESTCLIENT", "model.project.my_model")
    assert "environment" in result
    mock_dbt_discovery.assert_called_once()


async def test_get_exposure_details_all(mock_dbt_discovery):
    mock_dbt_discovery.return_value = {
        "environment": {"definition": {"exposures": {"edges": []}}}
    }
    result = await get_exposure_details("TESTCLIENT")
    assert "environment" in result


async def test_get_exposure_details_filtered(mock_dbt_discovery):
    mock_dbt_discovery.return_value = {
        "environment": {
            "definition": {
                "exposures": {
                    "edges": [
                        {
                            "node": {
                                "name": "my_dashboard",
                                "uniqueId": "exposure.project.my_dashboard",
                            }
                        },
                        {
                            "node": {
                                "name": "other_dashboard",
                                "uniqueId": "exposure.project.other",
                            }
                        },
                    ]
                }
            }
        }
    }
    result = await get_exposure_details("TESTCLIENT", exposure_name="my_dashboard")
    assert len(result["edges"]) == 1
    assert result["edges"][0]["node"]["name"] == "my_dashboard"
