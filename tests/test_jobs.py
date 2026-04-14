import pytest

from src.dbt_mcp.tools.jobs import (
    get_job_run_details,
    get_job_run_error,
    list_all_jobs,
    list_jobs,
)


async def test_list_jobs_success(mock_dbt_admin):
    mock_dbt_admin.return_value = {"data": [{"id": 1, "name": "Daily Run"}]}
    result = await list_jobs("TESTCLIENT")
    assert "data" in result
    mock_dbt_admin.assert_called_once()


async def test_list_jobs_unknown_client():
    with pytest.raises(ValueError, match="Unknown client"):
        await list_jobs("DOESNOTEXIST")


async def test_list_all_jobs(mock_dbt_admin):
    mock_dbt_admin.return_value = {"data": [{"id": 1, "name": "Daily Run"}]}
    result = await list_all_jobs()
    assert "TESTCLIENT" in result
    assert result["TESTCLIENT"]["status"] == "ok"
    assert "data" in result["TESTCLIENT"]["data"]


async def test_list_all_jobs_error_is_captured(mock_dbt_admin):
    mock_dbt_admin.side_effect = RuntimeError("Admin API 403")
    result = await list_all_jobs()
    assert result["TESTCLIENT"]["status"] == "error"
    assert "403" in result["TESTCLIENT"]["error"]


async def test_get_job_run_details(mock_dbt_admin):
    mock_dbt_admin.return_value = {"data": {"id": 42, "status": 10}}
    result = await get_job_run_details("TESTCLIENT", 42)
    assert result["data"]["id"] == 42


async def test_get_job_run_error(mock_dbt_admin):
    mock_dbt_admin.return_value = {
        "results": [{"status": "error", "unique_id": "model.project.bad_model"}]
    }
    result = await get_job_run_error("TESTCLIENT", 42)
    assert "results" in result
