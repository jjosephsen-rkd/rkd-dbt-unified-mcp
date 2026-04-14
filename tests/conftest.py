from unittest.mock import AsyncMock, patch

import pytest

from src.dbt_mcp.client_registry import ClientConfig

MOCK_CLIENT = ClientConfig(
    account_id="211140592271429",
    project_id="999999",
    environments={"prod": "888888", "dev": "777777"},
    service_token="dbtc_test_token",
)


@pytest.fixture(autouse=True)
def mock_registry(monkeypatch):
    """Replace the registry with a single test client for all tests."""
    monkeypatch.setenv("SECRET_BACKEND", "local")
    monkeypatch.setenv("DBT_TOKEN_TESTCLIENT", "dbtc_test_token")
    with patch("src.dbt_mcp.client_registry.load_registry") as mock:
        mock.return_value = {"TESTCLIENT": MOCK_CLIENT}
        yield mock


@pytest.fixture
def mock_dbt_admin():
    with patch(
        "src.dbt_mcp.dbt_client.DbtCloudClient.admin_get", new_callable=AsyncMock
    ) as mock:
        yield mock


@pytest.fixture
def mock_dbt_discovery():
    with patch(
        "src.dbt_mcp.dbt_client.DbtCloudClient.discovery_query", new_callable=AsyncMock
    ) as mock:
        yield mock
