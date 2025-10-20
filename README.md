# Databricks Workspace Audit Exporter

This workspace contains a CLI helper for exporting Azure Databricks audit log events for specific
workspace-level service categories. The script follows the Microsoft documentation for audit log
coverage: <https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/audit-logs>.

## Prerequisites

- Python 3.10+
- Access to the Azure Databricks workspace and a SQL warehouse endpoint that can query
  `system.access.audit`.
- Azure Active Directory permissions to request tokens for the Databricks resource using
  `DefaultAzureCredential`, or a Databricks personal access token.

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Exporting workspace audit categories

`scripts/fetch_workspace_audit_categories.py` fetches audit events for the key workspace-level
service categories (accounts, dashboards, clusters, jobs, and more) filtered by workspace ID and
date range. Results are streamed to JSON Lines, one event per line.

```bash
python scripts/fetch_workspace_audit_categories.py \
  --server-hostname adb-1234567890123456.3.azuredatabricks.net \
  --http-path /sql/1.0/endpoints/1234567890abcdef \
  --workspace-id 1234567890123456 \
  --start 2024-09-01T00:00:00Z \
  --end 2024-09-02T00:00:00Z \
  --output workspace_categories.jsonl
```

Key options:

- `--services` – optional comma-separated list of `service_name` values when you want a subset or
  superset beyond the default list documented in the link above.
- `--access-token` – supply a pre-issued Databricks personal access token instead of using Azure AD.
- `--managed-identity-client-id` – force the Azure credential chain to use a specific user-assigned
  managed identity.

The JSONL output includes `event_time`, `service_name`, `action_name`, `user_identity.email`,
`request_params`, and more, making it easy to ingest into downstream analytics (Power BI, Sentinel,
SIEMs).

## Validation

After installing the requirements you can perform a syntax check:

```bash
python -m compileall scripts
```

## Validate prerequisites for admins

Workspace admins can confirm their environment is ready before attempting an export by running `scripts/check_prerequisites.py`. The scanner verifies Azure authentication, SQL warehouse connectivity, and permissions on `system.access.audit`.

### 1. Gather authentication

- Preferred: sign in with Azure CLI (`az login`) using an identity in the same tenant as the workspace. If interactive login fails, provide `--access-token <Databricks PAT>` instead.
- Optional: pass `--scope` if your workspace uses a sovereign cloud endpoint.

### 2. Run the prerequisite scanner

```bash
python scripts/check_prerequisites.py \
  --server-hostname adb-<workspace>.azuredatabricks.net \
  --http-path /sql/1.0/warehouses/<warehouse-id> \
  --workspace-id <workspace-id>
```

Use `--access-token` when relying on a PAT, or `--workspace-check-lookback-hours` to adjust the audit lookback window.

### 3. If you see `INSUFFICIENT_PERMISSIONS`

- Confirm the workspace has **System tables** enabled in the Databricks account console.
- Ask an account admin to grant your identity the **System tables** entitlement or a role with `USE SCHEMA` on `system.access`.
  - Account console: **Workspaces → [Workspace] → System tables → Grant access**.
  - Or, via SCIM: assign the `system_table_access` entitlement to your user or group.
- After the entitlement propagates (usually within minutes), rerun the scanner; it should now report `[PASS] system_table_access` and `[PASS] workspace_filter`.

Once all checks pass, you can safely run the exporters in this repository.
