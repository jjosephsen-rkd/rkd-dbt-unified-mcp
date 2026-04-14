import pytest

from src.dbt_mcp.client_registry import get_client, list_clients


def test_get_client_success():
    client = get_client("TESTCLIENT")
    assert client.account_id == "211140592271429"
    assert client.service_token == "dbtc_test_token"


def test_get_client_case_insensitive():
    client = get_client("testclient")
    assert client.project_id == "999999"


def test_get_client_unknown_raises():
    with pytest.raises(ValueError, match="Unknown client"):
        get_client("NOPE")


def test_list_clients():
    clients = list_clients()
    assert "TESTCLIENT" in clients
