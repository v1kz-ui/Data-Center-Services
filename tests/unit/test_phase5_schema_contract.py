from app.db.base import Base


def test_phase5_score_run_profile_tracking_column_exists() -> None:
    score_run = Base.metadata.tables["score_run"]

    assert "profile_name" in score_run.columns
