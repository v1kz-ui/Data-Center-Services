from __future__ import annotations

import json
import logging
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from uuid import uuid4

from app.core.settings import Settings

_request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)
_trace_id_context: ContextVar[str | None] = ContextVar("trace_id", default=None)


def configure_application_logging(settings: Settings) -> None:
    logging.getLogger("app").setLevel(settings.log_level.upper())


def generate_correlation_id() -> str:
    return uuid4().hex


def bind_observability_context(request_id: str, trace_id: str) -> tuple[Token, Token]:
    request_token = _request_id_context.set(request_id)
    trace_token = _trace_id_context.set(trace_id)
    return request_token, trace_token


def reset_observability_context(tokens: tuple[Token, Token]) -> None:
    request_token, trace_token = tokens
    _request_id_context.reset(request_token)
    _trace_id_context.reset(trace_token)


def log_structured_event(
    logger_name: str,
    *,
    event: str,
    level: int = logging.INFO,
    **fields: object,
) -> None:
    payload: dict[str, object] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
    }
    request_id = _request_id_context.get()
    trace_id = _trace_id_context.get()
    if request_id is not None:
        payload["request_id"] = request_id
    if trace_id is not None:
        payload["trace_id"] = trace_id
    payload.update(fields)
    logging.getLogger(logger_name).log(
        level,
        json.dumps(payload, sort_keys=True, default=str),
    )
