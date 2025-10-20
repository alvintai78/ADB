#!/usr/bin/env python3
"""Quick audit prerequisite scanner for an Azure Databricks workspace.

The script validates the key requirements needed by the workspace audit exporter:

* Token acquisition via Azure AD (DefaultAzureCredential, Azure CLI session, device code, or managed identity)
* SQL warehouse connectivity
* Access to the `system.access.audit` system table
* Ability to filter by the supplied workspace ID

Each check reports PASS/FAIL plus diagnostic details. The script exits with status 0 when all
checks succeed; otherwise it prints the failing checks and exits 1.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional

from azure.identity import (
    AzureCliCredential,
    CredentialUnavailableError,
    DefaultAzureCredential,
    DeviceCodeCredential,
    ManagedIdentityCredential,
)
from databricks import sql
from databricks.sql.exc import DatabaseError, OperationalError

_LOGGER = logging.getLogger("check_prerequisites")
_DEFAULT_SCOPE = "https://databricks.azure.net/.default"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "passed": self.passed, "detail": self.detail}


def _acquire_token(args: argparse.Namespace) -> str:
    scope = args.scope or _DEFAULT_SCOPE

    if args.managed_identity_client_id:
        credential = ManagedIdentityCredential(client_id=args.managed_identity_client_id)
        cred_label = f"ManagedIdentityCredential({args.managed_identity_client_id})"
        _LOGGER.debug("Requesting Azure AD token using %s", cred_label)
        return credential.get_token(scope).token

    if args.prefer_device_code:
        credential = DeviceCodeCredential(tenant_id=args.tenant_id)
        cred_label = "DeviceCodeCredential"
        _LOGGER.info("Initiating device code flow. Follow the instructions shown in the terminal.")
        _LOGGER.debug("Requesting Azure AD token using %s", cred_label)
        return credential.get_token(scope).token

    try:
        cli_credential = AzureCliCredential(tenant_id=args.tenant_id)
        _LOGGER.debug("Attempting Azure CLI credential")
        return cli_credential.get_token(scope).token
    except CredentialUnavailableError:
        _LOGGER.debug("Azure CLI credential unavailable; falling back to DefaultAzureCredential")
    except Exception as exc:  # pragma: no cover - defensive logging path
        _LOGGER.warning("Azure CLI credential failed: %s", exc)

    credential = DefaultAzureCredential(
        exclude_interactive_browser_credential=not args.allow_interactive_login
    )
    cred_label = "DefaultAzureCredential"
    _LOGGER.debug("Requesting Azure AD token using %s", cred_label)
    return credential.get_token(scope).token


def _check_sql_connectivity(args: argparse.Namespace, access_token: str) -> CheckResult:
    try:
        with sql.connect(
            server_hostname=args.server_hostname,
            http_path=args.http_path,
            access_token=access_token,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return CheckResult("sql_connectivity", True, "Successfully executed SELECT 1")
    except OperationalError as exc:
        return CheckResult(
            "sql_connectivity",
            False,
            f"Unable to connect to SQL warehouse: {exc}",
        )


def _check_system_table_access(args: argparse.Namespace, access_token: str) -> CheckResult:
    try:
        with sql.connect(
            server_hostname=args.server_hostname,
            http_path=args.http_path,
            access_token=access_token,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM system.access.audit LIMIT 1")
                cursor.fetchone()
        return CheckResult("system_table_access", True, "system.access.audit is readable")
    except DatabaseError as exc:
        return CheckResult(
            "system_table_access",
            False,
            f"Failed to read system.access.audit: {exc}",
        )


def _check_workspace_filter(args: argparse.Namespace, access_token: str) -> CheckResult:
    lookback_start: Optional[dt.datetime] = None
    if args.workspace_check_lookback_hours is not None and args.workspace_check_lookback_hours >= 0:
        lookback_start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=args.workspace_check_lookback_hours)

    query = "SELECT workspace_id FROM system.access.audit WHERE workspace_id = ?"
    params: list[Any] = [args.workspace_id]
    if lookback_start is not None:
        query += " AND event_time >= ?"
        params.append(lookback_start)
    query += " LIMIT 1"
    try:
        with sql.connect(
            server_hostname=args.server_hostname,
            http_path=args.http_path,
            access_token=access_token,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, tuple(params))
                row = cursor.fetchone()
        if row:
            return CheckResult("workspace_filter", True, "Events found for workspace id")
        return CheckResult(
            "workspace_filter",
            True,
            "Query succeeded but returned no events; verify the workspace ID and time range when exporting.",
        )
    except DatabaseError as exc:
        return CheckResult(
            "workspace_filter",
            False,
            f"Failed to query workspace id {args.workspace_id}: {exc}",
        )


def run_checks(args: argparse.Namespace) -> int:
    if args.access_token:
        access_token = args.access_token
        token_source = "supplied access token"
    else:
        access_token = _acquire_token(args)
        token_source = "Azure AD token"

    _LOGGER.info("Using %s", token_source)

    results = [
        _check_sql_connectivity(args, access_token),
        _check_system_table_access(args, access_token),
        _check_workspace_filter(args, access_token),
    ]

    failed = [result for result in results if not result.passed]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        _LOGGER.info("[%s] %s - %s", status, result.name, result.detail)

    if failed:
        _LOGGER.error("Prerequisite scan failed")
        return 1

    _LOGGER.info("All prerequisite checks succeeded")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-hostname", required=True, help="Workspace hostname")
    parser.add_argument("--http-path", required=True, help="SQL warehouse HTTP path")
    parser.add_argument("--workspace-id", required=True, help="Numeric workspace id")
    parser.add_argument("--scope", help="Custom Azure AD scope for token acquisition")
    parser.add_argument("--managed-identity-client-id", help="User-assigned managed identity client id")
    parser.add_argument("--access-token", help="Databricks personal access token override")
    parser.add_argument(
        "--tenant-id",
        help="Optional tenant ID to use with Azure CLI or device code authentication",
    )
    parser.add_argument(
        "--workspace-check-lookback-hours",
        type=int,
        default=24,
        help="Limit the workspace filter check to events newer than this many hours (default: 24). Set to -1 to scan the full history.",
    )
    parser.add_argument(
        "--prefer-device-code",
        action="store_true",
        help="Force device code authentication instead of launching a browser",
    )
    parser.add_argument(
        "--allow-interactive-login",
        action="store_true",
        help="Permit interactive browser login if other credential sources are unavailable",
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

    if logging.getLogger().level > logging.DEBUG:
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("msal").setLevel(logging.WARNING)

    try:
        exit_code = run_checks(args)
    except Exception as exc:  # pragma: no cover - CLI wrapper
        _LOGGER.error("Failed to run prerequisite checks: %s", exc, exc_info=args.log_level == "DEBUG")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":  # pragma: no cover
    main()
