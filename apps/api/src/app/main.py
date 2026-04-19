from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI, Request

from app.api.router import api_router
from app.core.observability import (
    bind_observability_context,
    configure_application_logging,
    generate_correlation_id,
    log_structured_event,
    reset_observability_context,
)
from app.core.settings import get_settings


@asynccontextmanager
async def lifespan(application: FastAPI):
    settings = application.state.settings
    log_structured_event(
        "app.lifecycle",
        event="app_started",
        app_name=settings.app_name,
        environment=settings.app_env,
    )
    yield
    log_structured_event(
        "app.lifecycle",
        event="app_stopped",
        app_name=settings.app_name,
        environment=settings.app_env,
    )


def create_app() -> FastAPI:
    settings = get_settings()
    configure_application_logging(settings)
    application = FastAPI(
        title="Dense Data Center Locator API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(api_router)
    application.state.settings = settings

    @application.middleware("http")
    async def request_observability_middleware(request: Request, call_next):
        request_id = request.headers.get(settings.request_id_header) or generate_correlation_id()
        trace_id = request.headers.get(settings.trace_id_header) or request_id
        principal_subject = request.headers.get(settings.auth_subject_header)
        principal_roles = request.headers.get(settings.auth_roles_header)
        request.state.request_id = request_id
        request.state.trace_id = trace_id
        context_tokens = bind_observability_context(request_id, trace_id)
        started_at = perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = round((perf_counter() - started_at) * 1000, 2)
            log_structured_event(
                "app.request",
                event="request_failed",
                level=40,
                method=request.method,
                path=request.url.path,
                duration_ms=duration_ms,
                principal_subject=principal_subject,
                principal_roles=principal_roles,
                exception_type=exc.__class__.__name__,
            )
            reset_observability_context(context_tokens)
            raise

        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        response.headers[settings.request_id_header] = request_id
        response.headers[settings.trace_id_header] = trace_id
        log_structured_event(
            "app.request",
            event="request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
            principal_subject=principal_subject,
            principal_roles=principal_roles,
        )
        reset_observability_context(context_tokens)
        return response

    return application


app = create_app()
