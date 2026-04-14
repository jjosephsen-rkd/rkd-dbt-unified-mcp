"""
Diagnostic script — calls list_jobs() (the real MCP tool) for every client
and reports success/failure with the exact error.

Usage:
    uv run python scripts/diagnose_clients.py
    uv run python scripts/diagnose_clients.py GBRFB HFBTX NMSS   # specific clients only
"""

import asyncio
import os
import sys
from pathlib import Path

# Add project root to path so `src.dbt_mcp` resolves (mirrors pytest's pythonpath=["."])
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

# Load .env so DBT_TOKEN_DEFAULT (and any per-client tokens) are available
load_dotenv(ROOT / ".env")

# Must happen after load_dotenv so env vars are in place before registry loads
from src.dbt_mcp.client_registry import load_registry  # noqa: E402
from src.dbt_mcp.tools.jobs import list_jobs  # noqa: E402


async def check_client(client_id: str, project_id: str) -> dict:
    try:
        result = await list_jobs(client_id)
        job_count = len(result.get("data", []))
        return {"status": "OK", "jobs": job_count, "project_id": project_id}
    except Exception as e:
        return {"status": "ERROR", "error": f"{type(e).__name__}: {e}", "project_id": project_id}


async def main():
    print(f"\nLoading registry (SECRET_BACKEND={os.getenv('SECRET_BACKEND', 'aws')})...")
    try:
        registry = load_registry()
    except Exception as e:
        print(f"FATAL: registry failed to load: {e}")
        return

    # Use CLI args if provided, otherwise test every client in the registry
    targets = [c.upper() for c in sys.argv[1:]] if sys.argv[1:] else sorted(registry)
    unknown = [c for c in targets if c not in registry]
    targets = [c for c in targets if c in registry]

    print(f"Registry loaded — {len(registry)} clients")
    print(f"Testing {len(targets)} clients via list_jobs()...\n")
    print(f"{'CLIENT':<12} {'PROJECT_ID':<22} {'RESULT'}")
    print("-" * 70)

    results = await asyncio.gather(
        *[check_client(c, registry[c].project_id) for c in targets]
    )

    ok, errors = [], []
    for client_id, result in zip(targets, results):
        if result["status"] == "OK":
            ok.append(client_id)
            print(f"{client_id:<12} {result['project_id']:<22} OK ({result['jobs']} jobs)")
        else:
            errors.append((client_id, result))
            print(
                f"{client_id:<12} {result['project_id']:<22} "
                f"{result['status']}: {result['error']}"
            )

    for c in unknown:
        print(f"{c:<12} {'(not in registry)':<22} NOT IN REGISTRY")

    print(f"\nSummary: {len(ok)} OK, {len(errors)} failed, {len(unknown)} unknown")


if __name__ == "__main__":
    asyncio.run(main())
