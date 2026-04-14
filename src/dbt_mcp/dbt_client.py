"""
Async wrapper around the dbt Cloud Admin API (REST) and Discovery API (GraphQL).
Admin API docs: https://docs.getdbt.com/dbt-cloud/api-v2
Discovery API docs: https://docs.getdbt.com/docs/dbt-cloud-apis/discovery-api
"""

import os

import httpx

from .client_registry import ClientConfig

# Base URL for the dbt Cloud instance (cell-based deployments use a unique subdomain)
DBT_HOST = os.getenv("DBT_HOST", "https://gm766.us2.dbt.com")

ADMIN_BASE = f"{DBT_HOST}/api/v2"

# Discovery (Metadata) API — separate host for this cell-based deployment
# Source: account JSON field "discovery_api_url"
# Override with DBT_DISCOVERY_URL env var if needed
DISCOVERY_URL = os.getenv(
    "DBT_DISCOVERY_URL", "https://gm766.metadata.us2.dbt.com/graphql"
)


class DbtCloudClient:
    def __init__(self, config: ClientConfig):
        self.config = config
        # Admin API uses "Token", Discovery API uses "Bearer"
        self._admin_headers = {
            "Authorization": f"Token {config.service_token}",
            "Content-Type": "application/json",
        }
        self._discovery_headers = {
            "Authorization": f"Bearer {config.service_token}",
            "Content-Type": "application/json",
        }

    async def admin_get(self, path: str, params: dict | None = None) -> dict:
        """Call the dbt Cloud Admin (REST) API."""
        url = f"{ADMIN_BASE}/accounts/{self.config.account_id}/{path}"
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(url, headers=self._admin_headers, params=params or {})
            resp.raise_for_status()
            return resp.json()

    async def discovery_query(self, query: str, variables: dict | None = None) -> dict:
        """Call the dbt Discovery (GraphQL) API."""
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.post(
                DISCOVERY_URL,
                headers=self._discovery_headers,
                json={"query": query, "variables": variables or {}},
            )
            if not resp.is_success:
                raise RuntimeError(
                    f"Discovery API {resp.status_code} at {DISCOVERY_URL}: {resp.text}"
                )
            result = resp.json()
            if "errors" in result:
                raise RuntimeError(f"GraphQL errors: {result['errors']}")
            return result.get("data", {})
