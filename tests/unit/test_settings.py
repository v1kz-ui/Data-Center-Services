from app.core.settings import Settings, get_settings


def test_settings_default_batch_strategy() -> None:
    settings = get_settings()
    assert settings.active_batch_strategy == "activated_only"


def test_database_url_is_psycopg() -> None:
    settings = get_settings()
    assert settings.database_url.startswith("postgresql+psycopg://")


def test_database_url_override_is_respected() -> None:
    settings = Settings(DATABASE_URL="sqlite+pysqlite:///phase2-smoke.db")
    assert settings.database_url == "sqlite+pysqlite:///phase2-smoke.db"


def test_security_headers_default_to_header_auth_contract() -> None:
    settings = get_settings()
    assert settings.auth_enabled is True
    assert settings.auth_subject_header == "X-DDCL-Subject"
    assert settings.auth_name_header == "X-DDCL-Name"
    assert settings.auth_roles_header == "X-DDCL-Roles"


def test_observability_defaults_are_configured() -> None:
    settings = get_settings()
    assert settings.source_connector_config_path == "configs/source_connectors.json"
    assert (
        settings.authoritative_source_inventory_path
        == "configs/authoritative_source_inventory.json"
    )
    assert settings.request_id_header == "X-Request-ID"
    assert settings.trace_id_header == "X-Trace-ID"
    assert settings.monitoring_failed_run_threshold == 1
    assert settings.monitoring_failed_snapshot_threshold == 1
    assert settings.monitoring_quarantined_snapshot_threshold == 1
    assert settings.monitoring_freshness_failure_threshold == 1
    assert settings.monitoring_latest_batch_failed_threshold == 1
