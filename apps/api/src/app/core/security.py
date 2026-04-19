from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from app.core.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class AppRole(StrEnum):
    ADMIN = "admin"
    OPERATOR = "operator"
    READER = "reader"


@dataclass(slots=True, frozen=True)
class AuthenticatedPrincipal:
    subject: str
    display_name: str | None
    roles: frozenset[AppRole]
    auth_source: str


SettingsDependency = Annotated[Settings, Depends(get_settings)]


def get_current_principal(
    request: Request,
    settings: SettingsDependency,
) -> AuthenticatedPrincipal:
    if not settings.auth_enabled:
        return AuthenticatedPrincipal(
            subject="local-dev",
            display_name="Local Development",
            roles=frozenset({AppRole.ADMIN, AppRole.OPERATOR, AppRole.READER}),
            auth_source="auth-disabled",
        )

    subject = request.headers.get(settings.auth_subject_header)
    raw_roles = request.headers.get(settings.auth_roles_header)
    display_name = request.headers.get(settings.auth_name_header)

    if not subject or not raw_roles:
        logger.warning(
            "Authentication failed for %s %s: missing subject or role headers.",
            request.method,
            request.url.path,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication headers are required.",
        )

    roles = frozenset(
        AppRole(normalized_role)
        for normalized_role in {
            role.strip().lower()
            for role in raw_roles.split(",")
            if role.strip()
        }
        if normalized_role in {role.value for role in AppRole}
    )

    if not roles:
        logger.warning(
            "Authentication failed for %s %s: no recognized application roles for `%s`.",
            request.method,
            request.url.path,
            subject,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated principal does not have a recognized application role.",
        )

    return AuthenticatedPrincipal(
        subject=subject,
        display_name=display_name,
        roles=roles,
        auth_source="header",
    )


def require_roles(*allowed_roles: AppRole):
    allowed_role_set = frozenset(allowed_roles)

    def dependency(
        request: Request,
        principal: Annotated[AuthenticatedPrincipal, Depends(get_current_principal)],
    ) -> AuthenticatedPrincipal:
        if principal.roles.isdisjoint(allowed_role_set):
            allowed = ", ".join(sorted(role.value for role in allowed_role_set))
            presented = ", ".join(sorted(role.value for role in principal.roles))
            logger.warning(
                "RBAC access denied for %s %s: subject=%s roles=%s required=%s",
                request.method,
                request.url.path,
                principal.subject,
                presented,
                allowed,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This operation requires one of the following roles: {allowed}.",
            )

        return principal

    return dependency


require_operator_access = require_roles(AppRole.ADMIN, AppRole.OPERATOR)
require_admin_access = require_roles(AppRole.ADMIN)

CurrentPrincipal = Annotated[AuthenticatedPrincipal, Depends(get_current_principal)]
