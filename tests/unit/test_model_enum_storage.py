from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import ScoringProfile
from app.db.models.evaluation import ParcelEvaluation
from app.db.models.ingestion import SourceSnapshot


def test_native_enum_columns_persist_lowercase_values() -> None:
    assert ScoreBatch.__table__.c.status.type.enums == ["building", "failed", "completed", "active"]
    assert ScoreRun.__table__.c.status.type.enums == ["running", "failed", "completed"]
    assert ScoringProfile.__table__.c.status.type.enums == ["draft", "active", "retired"]
    assert SourceSnapshot.__table__.c.status.type.enums == ["success", "failed", "quarantined"]
    assert ParcelEvaluation.__table__.c.status.type.enums == [
        "prefiltered_band",
        "prefiltered_size",
        "pending_exclusion_check",
        "pending_scoring",
        "scored",
        "excluded",
    ]
