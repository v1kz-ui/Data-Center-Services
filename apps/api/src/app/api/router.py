from fastapi import APIRouter

from app.api.routes import (
    audit,
    connectors,
    evaluation,
    ingestion,
    market_listings,
    monitoring,
    orchestration,
    parcels,
    scoring,
    source_inventory,
    system,
    uat,
)

api_router = APIRouter()
api_router.include_router(system.router, tags=["system"])
api_router.include_router(audit.router, tags=["audit"])
api_router.include_router(connectors.router, tags=["connectors"])
api_router.include_router(evaluation.router, tags=["evaluation"])
api_router.include_router(ingestion.router, tags=["ingestion"])
api_router.include_router(market_listings.router, tags=["market-listings"])
api_router.include_router(monitoring.router, tags=["monitoring"])
api_router.include_router(orchestration.router, tags=["orchestration"])
api_router.include_router(parcels.router, tags=["parcels"])
api_router.include_router(scoring.router, tags=["scoring"])
api_router.include_router(source_inventory.router, tags=["source-inventory"])
api_router.include_router(uat.router, tags=["uat"])
