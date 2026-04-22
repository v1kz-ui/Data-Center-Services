from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "local"
    app_name: str = "dense-data-center-locator"
    app_port: int = 8000
    log_level: str = "INFO"
    reference_seed_dir: str = "db/seeds"
    source_connector_config_path: str = "configs/source_connectors.json"
    authoritative_source_inventory_path: str = "configs/authoritative_source_inventory.json"
    uat_environment_name: str = "uat"
    uat_scenario_pack_path: str = "infra/uat/phase7_uat_scenarios.json"
    request_id_header: str = "X-Request-ID"
    trace_id_header: str = "X-Trace-ID"
    monitoring_failed_run_threshold: int = 1
    monitoring_failed_snapshot_threshold: int = 1
    monitoring_quarantined_snapshot_threshold: int = 1
    monitoring_freshness_failure_threshold: int = 1
    monitoring_latest_batch_failed_threshold: int = 1
    auth_enabled: bool = True
    auth_subject_header: str = "X-DDCL-Subject"
    auth_name_header: str = "X-DDCL-Name"
    auth_roles_header: str = "X-DDCL-Roles"
    dashboard_password: str | None = Field(default=None, alias="DASHBOARD_PASSWORD")

    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "dense_data_center_locator"
    db_user: str = "postgres"
    db_password: str = "postgres"
    database_url_override: str | None = Field(default=None, alias="DATABASE_URL")

    active_batch_strategy: str = "activated_only"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    @property
    def database_url(self) -> str:
        if self.database_url_override:
            return self.database_url_override

        return (
            f"postgresql+psycopg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
