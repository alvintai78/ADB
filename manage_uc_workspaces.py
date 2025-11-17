#!/usr/bin/env python3
"""Ensure a Databricks SQL warehouse is running, manage system.access views, and list account workspaces."""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import requests

# Local environment file support.
_ENV_FILE = "adb.env"
# Warehouse states considered healthy.
_HEALTHY_WAREHOUSE_STATES = {"RUNNING", "RESIZING"}
# Warehouse states considered terminal failures.
_FAILED_WAREHOUSE_STATES = {"FAILED", "DEGRADED", "CANCELLED"}
_DEFAULT_AAD_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
_SQL_POLL_INTERVAL_SECONDS = 2
_ADMIN_PERMISSION_VALUES = {"WORKSPACE_ADMIN", "ADMIN"}


@dataclass(frozen=True)
class PrincipalGrantTarget:
    principal_type: str
    principal_name: str


def _load_env_file(path: str) -> None:
    """Load key=value pairs from a simple env file into os.environ."""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as env_file:
            for line in env_file:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())
    except OSError as err:
        logging.warning("Failed to load environment file %s: %s", path, err)


def _env_get(key: str) -> str | None:
    """Fetch an environment variable, returning None when unset or empty."""
    value = os.getenv(key)
    return value if value else None


def _env_get_bool(key: str) -> bool | None:
    """Return a boolean environment variable if set."""
    raw_value = _env_get(key)
    if raw_value is None:
        return None
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    logging.warning("Ignoring boolean env var %s with unrecognised value: %s", key, raw_value)
    return None

def _warehouse_endpoint(workspace_url: str, warehouse_id: str, action: str = "") -> str:
    base = workspace_url.rstrip("/")
    suffix = f"/api/2.0/sql/warehouses/{warehouse_id}"
    if action:
        suffix = f"{suffix}/{action}"
    return f"{base}{suffix}"


def get_warehouse_state(workspace_url: str, warehouse_id: str, token: str) -> str:
    """Return the current warehouse state."""
    response = requests.get(
        _warehouse_endpoint(workspace_url, warehouse_id),
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return str(payload.get("state", "UNKNOWN")).upper()


def start_warehouse(workspace_url: str, warehouse_id: str, token: str) -> None:
    """Send a start request to the warehouse."""
    response = requests.post(
        _warehouse_endpoint(workspace_url, warehouse_id, "start"),
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()


def ensure_warehouse_running(
    workspace_url: str,
    warehouse_id: str,
    token: str,
    poll_interval: int,
    max_wait: int,
) -> None:
    """Ensure the warehouse is running, starting it when required."""
    state = get_warehouse_state(workspace_url, warehouse_id, token)
    logging.info("Warehouse %s initial state: %s", warehouse_id, state)

    if state not in _HEALTHY_WAREHOUSE_STATES:
        if state in _FAILED_WAREHOUSE_STATES:
            raise RuntimeError(f"Warehouse {warehouse_id} is in a terminal state: {state}")
        logging.info("Requesting warehouse start...")
        start_warehouse(workspace_url, warehouse_id, token)

    waited = 0
    while state not in _HEALTHY_WAREHOUSE_STATES:
        if waited >= max_wait:
            raise TimeoutError(
                f"Warehouse {warehouse_id} did not reach a running state within {max_wait} seconds (last state: {state})."
            )
        logging.debug("Waiting %s seconds for warehouse to reach RUNNING (current: %s)", poll_interval, state)
        time.sleep(poll_interval)
        waited += poll_interval
        state = get_warehouse_state(workspace_url, warehouse_id, token)

    logging.info("Warehouse %s is ready (state: %s)", warehouse_id, state)


def list_account_workspaces(account_url: str, account_id: str, token: str) -> List[Dict[str, str]]:
    """Query the Databricks account API to enumerate every workspace in the tenant."""
    endpoint = f"{account_url.rstrip('/')}/api/2.0/accounts/{account_id}/workspaces"
    response = requests.get(endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if isinstance(payload, dict):
        raw_entries = payload.get("workspaces")
        if raw_entries is None:
            raw_entries = payload.get("items")
        if raw_entries is None:
            raw_entries = [payload]
    elif isinstance(payload, list):
        raw_entries = payload
    else:
        raise RuntimeError("Unexpected workspace response payload; expected list or dict")

    workspaces: List[Dict[str, str]] = []
    for workspace in raw_entries:
        if not isinstance(workspace, dict):
            continue
        workspace_id = str(workspace.get("workspace_id", workspace.get("id", "")))
        if not workspace_id:
            continue
        workspaces.append(
            {
                "workspace_id": workspace_id,
                "workspace_name": workspace.get("workspace_name", workspace.get("name", "")),
                "workspace_url": workspace.get("workspace_url", ""),
                "deployment_name": workspace.get("deployment_name", ""),
                "workspace_status": workspace.get("workspace_status", workspace.get("status", "")),
                "location": workspace.get("location", ""),
            }
        )
    return workspaces


def _contains_workspace_admin(value: Any) -> bool:
    if isinstance(value, str):
        return value.upper() in _ADMIN_PERMISSION_VALUES
    if isinstance(value, dict):
        return any(_contains_workspace_admin(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_contains_workspace_admin(item) for item in value)
    return False


def fetch_workspace_admin_principals(
    account_url: str,
    account_id: str,
    workspace_id: str,
    token: str,
) -> List[PrincipalGrantTarget]:
    """Return principal identities assigned the WORKSPACE_ADMIN permission level."""

    endpoint = f"{account_url.rstrip('/')}/api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments"
    params = {"permission_level": "WORKSPACE_ADMIN", "max_results": 1000}
    try:
        response = requests.get(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        response.raise_for_status()
    except requests.HTTPError as err:
        logging.warning(
            "Unable to retrieve workspace admin assignments for workspace %s: %s",
            workspace_id,
            err,
        )
        return []

    payload = response.json()
    logging.debug("Workspace %s permission assignments payload: %s", workspace_id, payload)
    assignments: Sequence[Any]
    if isinstance(payload, dict):
        for key in ("permission_assignments", "assignments", "items", "results"):
            if key in payload and isinstance(payload[key], list):
                assignments = payload[key]
                break
        else:
            assignments = [payload]
    elif isinstance(payload, list):
        assignments = payload
    else:
        assignments = []

    principals: List[PrincipalGrantTarget] = []

    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue

        if not _contains_workspace_admin(assignment):
            continue

        principal_info = assignment.get("principal")
        if not isinstance(principal_info, dict):
            principal_info = assignment

        raw_type = (
            principal_info.get("principal_type")
            or principal_info.get("principalType")
            or principal_info.get("type")
            or ""
        )
        raw_name = (
            principal_info.get("principal_name")
            or principal_info.get("user_name")
            or principal_info.get("group_name")
            or principal_info.get("service_principal_name")
            or principal_info.get("application_id")
            or principal_info.get("email")
            or principal_info.get("display_name")
            or principal_info.get("displayName")
            or principal_info.get("principal_id")
        )

        if not raw_type:
            if principal_info.get("service_principal_name") or principal_info.get("application_id"):
                raw_type = "SERVICE_PRINCIPAL"
            elif principal_info.get("group_name"):
                raw_type = "GROUP"
            elif principal_info.get("user_name") or principal_info.get("email"):
                raw_type = "USER"


        if not raw_type or not raw_name:
            logging.debug("Skipping workspace admin entry with missing fields: %s", assignment)
            continue

        principals.append(PrincipalGrantTarget(raw_type.upper(), str(raw_name)))

    # Deduplicate while preserving order of first appearance.
    deduped: List[PrincipalGrantTarget] = []
    seen: set[PrincipalGrantTarget] = set()
    for principal in principals:
        if principal not in seen:
            deduped.append(principal)
            seen.add(principal)

    if not deduped:
        logging.warning(
            "No WORKSPACE_ADMIN principals found for workspace %s; downstream grants will be skipped.",
            workspace_id,
        )

    return deduped


def _rows_from_result(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = [col.get("name", f"col_{idx}") for idx, col in enumerate(result.get("schema", {}).get("columns", []))]
    data_array = result.get("data_array")
    if data_array is None and "data" in result:
        data_array = result["data"].get("rows") if isinstance(result["data"], dict) else result["data"]
    if data_array is None:
        return []

    rows: List[Dict[str, Any]] = []
    for entry in data_array:
        if isinstance(entry, dict) and "row" in entry:
            row_values = entry.get("row", [])
        else:
            row_values = entry
        if not isinstance(row_values, (list, tuple)):
            row_values = [row_values]
        row_dict = {columns[idx] if idx < len(columns) else f"col_{idx}": value for idx, value in enumerate(row_values)}
        rows.append(row_dict)
    return rows


def execute_sql_statement(
    workspace_url: str,
    warehouse_id: str,
    token: str,
    statement: str,
    catalog: str | None = None,
    schema: str | None = None,
    expect_result: bool = False,
) -> List[Dict[str, Any]]:
    """Execute a SQL statement via the Databricks statement execution API."""

    endpoint = f"{workspace_url.rstrip('/')}/api/2.0/sql/statements"
    payload: Dict[str, Any] = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
        "disposition": "INLINE",
    }
    if catalog:
        payload["catalog"] = catalog
    if schema:
        payload["schema"] = schema

    response = requests.post(endpoint, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=30)
    response.raise_for_status()
    statement_payload = response.json()
    statement_id = statement_payload.get("statement_id")
    status = statement_payload.get("status", {}).get("state")

    while status in {"PENDING", "RUNNING"} and statement_id:
        time.sleep(_SQL_POLL_INTERVAL_SECONDS)
        poll_response = requests.get(
            f"{endpoint}/{statement_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        poll_response.raise_for_status()
        statement_payload = poll_response.json()
        status = statement_payload.get("status", {}).get("state")

    if status != "SUCCEEDED":
        error = statement_payload.get("status", {}).get("error", {})
        message = error.get("message") or error or f"SQL statement failed with state {status}"
        raise RuntimeError(f"SQL execution failed: {message}")

    result = statement_payload.get("result") if expect_result else None
    return _rows_from_result(result) if result else []


def fetch_system_access_tables(
    workspace_url: str,
    warehouse_id: str,
    token: str,
) -> List[str]:
    # Prefer Unity Catalog REST API for reliable discovery of system.access tables.
    uc_endpoint = f"{workspace_url.rstrip('/')}/api/2.1/unity-catalog/tables"
    params = {"catalog_name": "system", "schema_name": "access", "max_results": 1000}
    try:
        uc_response = requests.get(
            uc_endpoint,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        uc_response.raise_for_status()

        uc_payload = uc_response.json()
        uc_tables = [
            str(table.get("name"))
            for table in uc_payload.get("tables", [])
            if table.get("name")
        ]

        next_page_token = uc_payload.get("next_page_token")
        while next_page_token:
            params["page_token"] = next_page_token
            paged_response = requests.get(
                uc_endpoint,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=30,
            )
            paged_response.raise_for_status()
            paged_payload = paged_response.json()
            uc_tables.extend(
                str(table.get("name"))
                for table in paged_payload.get("tables", [])
                if table.get("name")
            )
            next_page_token = paged_payload.get("next_page_token")

        if uc_tables:
            unique_tables = sorted(set(uc_tables))
            logging.info(
                "Discovered %d tables in system.access using Unity Catalog API",
                len(unique_tables),
            )
            return unique_tables
    except requests.RequestException as err:
        logging.debug("Unity Catalog API table listing error: %s", err)

    queries = [
        (
            "information_schema",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'access'",
            "table_name",
        ),
        (
            "show_tables",
            "SHOW TABLES",
            "tableName",
        ),
    ]

    last_error: RuntimeError | None = None
    for label, statement, column_name in queries:
        try:
            rows = execute_sql_statement(
                workspace_url=workspace_url,
                warehouse_id=warehouse_id,
                token=token,
                statement=statement,
                catalog="system",
                schema="access",
                expect_result=True,
            )
        except RuntimeError as err:
            logging.debug(
                "Failed to enumerate system.access tables via %s query (%s)",
                label,
                err,
            )
            last_error = err
            continue

        table_names = []
        for row in rows:
            value = row.get(column_name)
            if value is None:
                # attempt alternate keys when column label differs (e.g., SHOW TABLES)
                value = (
                    row.get("table_name")
                    or row.get("name")
                    or row.get("tablename")
                    or row.get("table")
                )
            if value:
                table_names.append(str(value))

        if table_names:
            unique_tables = sorted(set(table_names))
            logging.info("Discovered %d tables in system.access using %s query", len(unique_tables), label)
            return unique_tables

    if last_error:
        logging.warning(
            "Unable to enumerate system.access tables (last error: %s). "
            "Ensure the service principal has USE CATALOG/SCHEMA on system.",
            last_error,
        )
    else:
        logging.warning(
            "No tables discovered in system.access. Confirm system tables are enabled and accessible to the service principal.",
        )
    return []


def ensure_target_schema(
    workspace_url: str,
    warehouse_id: str,
    token: str,
    catalog: str,
    schema: str,
) -> None:
    statement = f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
    execute_sql_statement(
        workspace_url=workspace_url,
        warehouse_id=warehouse_id,
        token=token,
        statement=statement,
        catalog=catalog,
        schema=schema,
    )


def create_system_access_view(
    workspace_url: str,
    warehouse_id: str,
    token: str,
    catalog: str,
    schema: str,
    view_name: str,
    source_table: str,
    workspace_id_filter: str | None = None,
) -> None:
    base_query = f"SELECT * FROM system.access.`{source_table}`"
    if workspace_id_filter:
        base_query = (
            f"SELECT * FROM system.access.`{source_table}` "
            f"WHERE workspace_id = '{workspace_id_filter}'"
        )

    statement = (
        f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{view_name}` "
        f"AS {base_query}"
    )
    execute_sql_statement(
        workspace_url=workspace_url,
        warehouse_id=warehouse_id,
        token=token,
        statement=statement,
        catalog=catalog,
        schema=schema,
    )


def _quote_principal_identifier(name: str) -> str:
    """Return a Databricks identifier literal that tolerates special characters."""
    return f"`{name.replace('`', '``')}`"


def _build_grant_target(principal: PrincipalGrantTarget) -> Optional[str]:
    # Databricks principal specifications accept quoted identifiers that infer the type.
    if not principal.principal_name:
        logging.debug("Skipping principal with empty name: %s", principal)
        return None
    return _quote_principal_identifier(principal.principal_name)


def grant_view_to_principals(
    workspace_url: str,
    warehouse_id: str,
    token: str,
    catalog: str,
    schema: str,
    view_name: str,
    principals: Sequence[PrincipalGrantTarget],
) -> None:
    for principal in principals:
        target = _build_grant_target(principal)
        if not target:
            continue

        statements = [
            f"GRANT USAGE ON CATALOG `{catalog}` TO {target}",
            f"GRANT USAGE ON SCHEMA `{catalog}`.`{schema}` TO {target}",
            f"GRANT SELECT ON VIEW `{catalog}`.`{schema}`.`{view_name}` TO {target}",
        ]

        for stmt in statements:
            try:
                execute_sql_statement(
                    workspace_url=workspace_url,
                    warehouse_id=warehouse_id,
                    token=token,
                    statement=stmt,
                    catalog=catalog,
                    schema=schema,
                )
            except RuntimeError as err:
                logging.debug(
                    "Grant statement skipped for principal %s %s (%s): %s",
                    principal.principal_type,
                    principal.principal_name,
                    stmt,
                    err,
                )
def obtain_service_principal_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> str:
    """Acquire an AAD access token using the client credentials grant."""
    token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }

    response = requests.post(token_endpoint, data=payload, timeout=30)
    if response.status_code != 200:
        try:
            error_payload = response.json()
            error_description = error_payload.get("error_description") or error_payload.get("error")
        except ValueError:
            error_description = response.text
        raise RuntimeError(
            f"Azure AD token request failed ({response.status_code}): {error_description}"
        )

    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("AAD token response did not include access_token")
    return token


def main() -> None:
    _load_env_file(_ENV_FILE)

    parser = argparse.ArgumentParser(
        description="Ensure a Databricks SQL warehouse is running and list all workspaces in the account.",
    )
    parser.add_argument(
        "--workspace-url",
        default=None,
        help="Base URL of the specific Databricks workspace that hosts the SQL warehouse (for example, https://adb-<id>.azuredatabricks.net).",
    )
    parser.add_argument("--warehouse-id", default=None, help="Identifier of the SQL warehouse to check/start.")
    parser.add_argument("--account-id", default=None, help="Databricks account identifier (UUID).")
    parser.add_argument(
        "--account-url",
        default="https://accounts.azuredatabricks.net",
        help="Databricks account console URL (default: https://accounts.azuredatabricks.net).",
    )
    parser.add_argument(
        "--client-id",
        default=None,
        help="Azure AD application (service principal) client ID.",
    )
    parser.add_argument(
        "--client-secret",
        default=None,
        help="Azure AD application (service principal) client secret.",
    )
    parser.add_argument(
        "--tenant-id",
        default=None,
        help="Azure AD tenant ID for the service principal.",
    )
    parser.add_argument(
        "--aad-scope",
        default=_DEFAULT_AAD_SCOPE,
        help=(
            "AAD scope used when requesting an access token (default: Databricks first-party app scope). "
            "Override only if instructed by Databricks."
        ),
    )
    parser.add_argument(
        "--view-catalog",
        default=None,
        help="Catalog where system.access views should be created.",
    )
    parser.add_argument(
        "--view-schema",
        default=None,
        help="Schema where system.access views should be created.",
    )
    parser.add_argument(
        "--view-prefix",
        default="system_access_",
        help="Prefix for generated view names (default: system_access_).",
    )
    parser.add_argument(
        "--sync-system-access-views",
        action="store_true",
        help="Create views for every table in system.access and grant access to each workspace.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between warehouse status checks (default: 30).",
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        default=900,
        help="Maximum seconds to wait for the warehouse to start (default: 900).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Log verbosity (default: INFO).",
    )
    parser.add_argument(
        "--env-prefix",
        default="ADB_",
        help="Prefix for environment variables loaded from abd.env (default: ADB_).",
    )

    args = parser.parse_args()

    env_prefix = args.env_prefix

    # Allow environment overrides for key parameters.
    workspace_url = args.workspace_url or _env_get(f"{env_prefix}WORKSPACE_URL")
    warehouse_id = args.warehouse_id or _env_get(f"{env_prefix}WAREHOUSE_ID")
    account_id = args.account_id or _env_get(f"{env_prefix}ACCOUNT_ID")
    account_url = args.account_url or _env_get(f"{env_prefix}ACCOUNT_URL")
    client_id = args.client_id or _env_get(f"{env_prefix}CLIENT_ID")
    client_secret = args.client_secret or _env_get(f"{env_prefix}CLIENT_SECRET")
    tenant_id = args.tenant_id or _env_get(f"{env_prefix}TENANT_ID")
    aad_scope = args.aad_scope or _DEFAULT_AAD_SCOPE
    view_catalog = args.view_catalog or _env_get(f"{env_prefix}VIEW_CATALOG")
    view_schema = args.view_schema or _env_get(f"{env_prefix}VIEW_SCHEMA")
    view_prefix = args.view_prefix or _env_get(f"{env_prefix}VIEW_PREFIX") or "system_access_"
    sync_system_access_views = args.sync_system_access_views
    env_sync_flag = _env_get_bool(f"{env_prefix}SYNC_SYSTEM_ACCESS_VIEWS")
    if env_sync_flag is not None:
        sync_system_access_views = env_sync_flag
    poll_interval = args.poll_interval
    max_wait = args.max_wait
    log_level = args.log_level

    missing = [
        name
        for name, value in (
            ("workspace_url", workspace_url),
            ("warehouse_id", warehouse_id),
            ("account_id", account_id),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("tenant_id", tenant_id),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"Missing required arguments: {', '.join(missing)}. Provide them via CLI or abd.env")

    if sync_system_access_views:
        missing_sync = [
            name
            for name, value in (("view_catalog", view_catalog), ("view_schema", view_schema))
            if not value
        ]
        if missing_sync:
            raise SystemExit(
                "Missing required arguments for system.access view sync: "
                + ", ".join(missing_sync)
            )

    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(levelname)s: %(message)s")
    try:
        access_token = obtain_service_principal_token(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            scope=aad_scope,
        )
    except (RuntimeError, requests.HTTPError) as err:
        logging.error("Failed to acquire Azure AD access token: %s", err)
        return

    try:
        ensure_warehouse_running(
            workspace_url=workspace_url,
            warehouse_id=warehouse_id,
            token=access_token,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )
    except (RuntimeError, TimeoutError, requests.HTTPError) as err:
        logging.error("Warehouse readiness check failed: %s", err)
        return
    try:
        workspace_entries = list_account_workspaces(account_url, account_id, access_token)
    except requests.HTTPError as err:
        logging.error("Failed to retrieve account workspace inventory: %s", err)
        return

    if not workspace_entries:
        print("No Databricks workspaces found for the provided service principal token.")
        return

    if sync_system_access_views:
        try:
            logging.info(
                "Synchronising system.access views into %s.%s for %d workspaces",
                view_catalog,
                view_schema,
                len(workspace_entries),
            )
            ensure_target_schema(
                workspace_url=workspace_url,
                warehouse_id=warehouse_id,
                token=access_token,
                catalog=view_catalog,
                schema=view_schema,
            )
            system_access_tables = fetch_system_access_tables(
                workspace_url=workspace_url,
                warehouse_id=warehouse_id,
                token=access_token,
            )
            if not system_access_tables:
                logging.warning("No tables discovered in system.access; skipping view creation.")
            else:
                total_tables = len(system_access_tables)
                total_workspaces = len(workspace_entries)
                logging.info(
                    "Preparing %d system.access tables for %d workspaces (%d views total)",
                    total_tables,
                    total_workspaces,
                    total_tables * total_workspaces,
                )
                workspace_admin_cache: Dict[str, List[PrincipalGrantTarget]] = {}
                for table_idx, table_name in enumerate(system_access_tables, start=1):
                    if table_name == "clean_room_events":
                        logging.info("[%d/%d] Skipping system.access.%s per configuration", table_idx, total_tables, table_name)
                        continue
                    logging.info(
                        "[%d/%d] Processing system.access.%s",
                        table_idx,
                        total_tables,
                        table_name,
                    )
                    for workspace_idx, workspace in enumerate(workspace_entries, start=1):
                        workspace_id = workspace.get("workspace_id")
                        if not workspace_id:
                            logging.debug("Skipping workspace with missing workspace_id: %s", workspace)
                            continue
                        workspace_id_str = str(workspace_id)
                        admin_principals = workspace_admin_cache.get(workspace_id_str)
                        if admin_principals is None:
                            admin_principals = fetch_workspace_admin_principals(
                                account_url=account_url,
                                account_id=account_id,
                                workspace_id=workspace_id_str,
                                token=access_token,
                            )
                            workspace_admin_cache[workspace_id_str] = admin_principals
                        view_name = f"{view_prefix}{table_name}_ws_{workspace_id}"
                        logging.info(
                            "    [%d/%d] Creating view %s.%s.%s for workspace %s",
                            workspace_idx,
                            total_workspaces,
                            view_catalog,
                            view_schema,
                            view_name,
                            workspace_id,
                        )
                        try:
                            create_system_access_view(
                                workspace_url=workspace_url,
                                warehouse_id=warehouse_id,
                                token=access_token,
                                catalog=view_catalog,
                                schema=view_schema,
                                view_name=view_name,
                                source_table=table_name,
                                workspace_id_filter=str(workspace_id),
                            )
                        except RuntimeError as err:
                            if "workspace_id" in str(err).lower():
                                logging.warning(
                                    "    Workspace filter failed for %s (workspace %s); creating unfiltered view instead (%s)",
                                    table_name,
                                    workspace_id,
                                    err,
                                )
                                create_system_access_view(
                                    workspace_url=workspace_url,
                                    warehouse_id=warehouse_id,
                                    token=access_token,
                                    catalog=view_catalog,
                                    schema=view_schema,
                                    view_name=view_name,
                                    source_table=table_name,
                                    workspace_id_filter=None,
                                )
                            else:
                                raise
                        logging.info(
                            "    View %s.%s.%s created; evaluating grants for workspace %s",
                            view_catalog,
                            view_schema,
                            view_name,
                            workspace_id,
                        )
                        if admin_principals:
                            grant_view_to_principals(
                                workspace_url=workspace_url,
                                warehouse_id=warehouse_id,
                                token=access_token,
                                catalog=view_catalog,
                                schema=view_schema,
                                view_name=view_name,
                                principals=admin_principals,
                            )
                        else:
                            logging.warning(
                                "    Skipping grants for workspace %s because no WORKSPACE_ADMIN principals were resolved",
                                workspace_id,
                            )
                        logging.info(
                            "    Finished view %s.%s.%s for workspace %s",
                            view_catalog,
                            view_schema,
                            view_name,
                            workspace_id,
                        )
        except RuntimeError as err:
            logging.error("System.access view sync failed: %s", err)
            return

    for entry in workspace_entries:
        print(
            f"workspace_id={entry['workspace_id']}\tworkspace_name={entry['workspace_name']}"
            f"\tworkspace_url={entry['workspace_url']}\tstatus={entry['workspace_status']}"
            f"\tdeployment_name={entry['deployment_name']}\tlocation={entry['location']}"
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.error("Execution cancelled by user.")
