#!/usr/bin/env python3
"""
Export Azure Databricks audit log events for a workspace across the primary service categories
listed in the Microsoft documentation:
https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/audit-logs

The script queries the `system.access.audit` system table through a SQL warehouse, filters by
workspace ID, date range, and a configurable set of `service_name` values, and writes the results to
JSON Lines.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
import sys
from typing import Any, Iterable, List, Optional, Sequence

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from databricks import sql

_LOGGER = logging.getLogger("fetch_workspace_audit_categories")
_DEFAULT_SCOPE = "https://databricks.azure.net/.default"
_FETCH_BATCH_SIZE = 500

# Default service categories pulled from the workspace-level events section of the audit log doc.
DEFAULT_SERVICES: Sequence[str] = (
    "accounts",
    "dashboards",
    "aibiGenie",
    "alerts",
    "clusters",
    "clusterLibraries",
    "clusterPolicies",
    "apps",
    "databrickssql",
    "dataMonitoring",
    "dbfs",
    "featureStore",
    "filesystem",
    "genie",
    "gitCredentials",
    "repos",
    "globalInitScripts",
    "groups",
    "iamRole",
    "ingestion",
    "instancePools",
    "jobs",
    "databaseInstances",
)


def _parse_datetime(value: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp into an aware UTC datetime."""
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:  # pragma: no cover - defensive
        raise argparse.ArgumentTypeError(f"Invalid datetime value: {value}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _parse_services(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_SERVICES)
    services = [item.strip() for item in raw.split(",") if item.strip()]
    if not services:
        raise argparse.ArgumentTypeError("--services must contain at least one service name")
    return services


def _ensure_path(path: pathlib.Path) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _acquire_token(scope: str, managed_identity_client_id: Optional[str]) -> str:
    if managed_identity_client_id:
        credential = ManagedIdentityCredential(client_id=managed_identity_client_id)
    else:
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token = credential.get_token(scope)
    return token.token


def _build_query(include_limit: bool, service_count: int) -> str:
    placeholders = ", ".join(["?"] * service_count)
    query_lines = [
        "SELECT",
        "  event_time,",
        "  workspace_id,",
        "  service_name,",
        "  action_name,",
        "  user_identity.email AS user_email,",
        "  source_ip_address,",
        "  request_id,",
        "  request_params,",
        "  response",
        "FROM system.access.audit",
        "WHERE event_time BETWEEN ? AND ?",
        "  AND workspace_id = ?",
        f"  AND service_name IN ({placeholders})",
        "ORDER BY event_time",
    ]
    if include_limit:
        query_lines.append("LIMIT ?")
    return "\n".join(query_lines)


def _serialize_row(columns: Iterable[str], row: Iterable[Any]) -> dict:
    def _convert(value: Any) -> Any:
        if isinstance(value, dt.datetime):
            return value.astimezone(dt.timezone.utc).isoformat()
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    return {name: _convert(value) for name, value in zip(columns, row)}


def _stream_results(
    connection: sql.client.Connection,
    query: str,
    params: Sequence[Any],
    output_path: pathlib.Path,
) -> int:
    rows_written = 0
    with connection.cursor() as cursor:
        cursor.execute(query, tuple(params))
        column_names = [desc[0] for desc in cursor.description]
        with output_path.open("w", encoding="utf-8") as handle:
            while True:
                rows = cursor.fetchmany(_FETCH_BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    record = _serialize_row(column_names, row)
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    rows_written += 1
    return rows_written


def fetch_workspace_events(args: argparse.Namespace) -> None:
    services = _parse_services(args.services)
    start = _parse_datetime(args.start)
    end = _parse_datetime(args.end)
    if end <= start:
        raise ValueError("--end must be later than --start")

    params: List[Any] = [start, end, args.workspace_id]
    params.extend(services)

    query = _build_query(args.limit is not None, len(services))
    if args.limit is not None:
        params.append(int(args.limit))

    if args.access_token:
        access_token = args.access_token
    else:
        _LOGGER.debug("Acquiring Azure AD token using DefaultAzureCredential")
        access_token = _acquire_token(args.scope or _DEFAULT_SCOPE, args.managed_identity_client_id)

    output_path = _ensure_path(pathlib.Path(args.output))

    _LOGGER.info("Querying system.access.audit for workspace %s", args.workspace_id)
    with sql.connect(
        server_hostname=args.server_hostname,
        http_path=args.http_path,
        access_token=access_token,
    ) as connection:
        count = _stream_results(connection, query, params, output_path)

    _LOGGER.info("Wrote %s audit events to %s", count, output_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-hostname", required=True, help="Workspace hostname, e.g. adb-<id>.azuredatabricks.net")
    parser.add_argument("--http-path", required=True, help="SQL warehouse HTTP path")
    parser.add_argument("--workspace-id", required=True, help="Workspace ID to filter (numeric)")
    parser.add_argument("--start", required=True, help="Inclusive start timestamp (ISO-8601)")
    parser.add_argument("--end", required=True, help="Exclusive end timestamp (ISO-8601)")
    parser.add_argument(
        "--services",
        help="Comma-separated list of service_name values. Defaults to the key categories from the audit log documentation.",
    )
    parser.add_argument("--output", default="workspace_audit.jsonl", help="Output JSONL path")
    parser.add_argument("--limit", type=int, help="Optional maximum number of rows")
    parser.add_argument("--scope", help="Azure AD scope to request a token for (defaults to Databricks Azure scope)")
    parser.add_argument("--managed-identity-client-id", help="Client ID of a user-assigned managed identity to use")
    parser.add_argument(
        "--access-token",
        help="Use an existing Databricks access token instead of acquiring one via Azure AD",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        fetch_workspace_events(args)
    except Exception as exc:  # pragma: no cover - CLI surface area
        _LOGGER.error("Failed to export workspace audit events: %s", exc, exc_info=args.log_level == "DEBUG")
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
