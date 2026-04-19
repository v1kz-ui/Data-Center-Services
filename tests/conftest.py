from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.session import get_db
from app.main import create_app


@pytest.fixture
def admin_headers() -> dict[str, str]:
    return {
        "X-DDCL-Subject": "test-admin",
        "X-DDCL-Name": "Test Admin",
        "X-DDCL-Roles": "admin",
    }


@pytest.fixture
def operator_headers() -> dict[str, str]:
    return {
        "X-DDCL-Subject": "test-operator",
        "X-DDCL-Name": "Test Operator",
        "X-DDCL-Roles": "operator",
    }


@pytest.fixture
def reader_headers() -> dict[str, str]:
    return {
        "X-DDCL-Subject": "test-reader",
        "X-DDCL-Name": "Test Reader",
        "X-DDCL-Roles": "reader",
    }


@pytest.fixture
def session_factory() -> Iterator[sessionmaker[Session]]:
    engine = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        future=True,
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)

    factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )
    try:
        yield factory
    finally:
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture
def db_session(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(
    session_factory: sessionmaker[Session],
    admin_headers: dict[str, str],
) -> Iterator[TestClient]:
    app = create_app()

    def override_get_db() -> Iterator[Session]:
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        test_client.headers.update(admin_headers)
        yield test_client

    app.dependency_overrides.clear()
