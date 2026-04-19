# RBAC and Access Control

This slice introduces the first enforceable authentication and authorization
layer for the internal API.

## Identity Contract

- Authentication is header-based for now so the application can accept
  principal context from an upstream enterprise identity gateway.
- The API expects:
  - `X-DDCL-Subject`
  - `X-DDCL-Name`
  - `X-DDCL-Roles`
- Missing subject or role headers return `401 Unauthorized`.
- Unrecognized or insufficient roles return `403 Forbidden`.

## Role Model

- `admin`
  - can access all secured internal API routes, including system metadata
    endpoints.
- `operator`
  - can access monitoring, ingestion, evaluation, scoring, and orchestration
    surfaces used for operational workflow.
- `reader`
  - is recognized by the identity layer but cannot call operator/admin routes in
    the current repository state.

## Protected Surfaces

- `/admin/sources/*`
- `/admin/runs/*/evaluation*`
- `/admin/runs/*/scoring*`
- `/admin/monitoring/overview`
- `/orchestration/*`
- `/foundation/tables`

## Audit and Attribution

- RBAC denials are written to the application log with subject, path, and role
  context.
- Orchestration retry, cancel, and rerun actions default their actor identity
  from the authenticated principal when the caller does not provide an explicit
  actor name.

## Validation Coverage

- missing-auth request returns `401`,
- reader-role request returns `403`,
- operator request succeeds on operator surfaces,
- admin request succeeds on admin-only system metadata routes, and
- denied attempts emit a security log entry.
