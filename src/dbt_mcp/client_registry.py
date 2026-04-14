"""
Client registry — loads client config from YAML and injects service tokens.
Tokens come from env vars in local dev, AWS Secrets Manager in production.
"""

import json
import os
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel


class ClientConfig(BaseModel):
    account_id: str
    project_id: str
    environments: dict[str, str]  # e.g. {"prod": "123", "dev": "456"}
    service_token: str  # injected at runtime, never stored in YAML

    def get_environment_id(self, env: str = "prod") -> str:
        """Return the environment ID for the given env name (default: prod)."""
        if env not in self.environments:
            available = sorted(self.environments.keys())
            raise ValueError(
                f"Unknown environment '{env}'. Available: {available}"
            )
        return self.environments[env]

    @property
    def environment_id(self) -> str:
        """Shortcut: prod environment ID, used as default by all tools."""
        return self.get_environment_id("prod")


@lru_cache(maxsize=1)
def load_registry() -> dict[str, ClientConfig]:
    config_path = Path(__file__).parent.parent.parent / "config" / "clients.yaml"
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    registry = {}
    for client_id, cfg in raw["clients"].items():
        token = _resolve_token(client_id)
        registry[client_id.upper()] = ClientConfig(**cfg, service_token=token)
    return registry


def _resolve_token(client_id: str) -> str:
    """
    Local dev: reads DBT_TOKEN_<CLIENT> from environment.
    Production: fetches from AWS Secrets Manager at path dbt-mcp/<CLIENT>/service-token.
    Controlled by SECRET_BACKEND env var: 'local' | 'aws' (default: 'aws')
    """
    backend = os.getenv("SECRET_BACKEND", "aws")

    if backend == "local":
        env_key = f"DBT_TOKEN_{client_id.upper()}"
        token = os.getenv(env_key) or os.getenv("DBT_TOKEN_DEFAULT")
        if not token:
            raise EnvironmentError(
                f"Missing env var {env_key} (and no DBT_TOKEN_DEFAULT fallback). "
                f"Copy .env.example to .env and set either {env_key} or DBT_TOKEN_DEFAULT."
            )
        return token

    # AWS Secrets Manager path
    import boto3

    secret_name = f"dbt-mcp/{client_id.upper()}/service-token"
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])["token"]


def get_client(client_id: str) -> ClientConfig:
    registry = load_registry()
    key = client_id.upper()
    if key not in registry:
        available = sorted(registry.keys())
        raise ValueError(
            f"Unknown client '{client_id}'. " f"Available clients: {available}"
        )
    return registry[key]


def list_clients() -> list[str]:
    return sorted(load_registry().keys())
