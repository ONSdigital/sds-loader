from fastapi import APIRouter

from app import get_logger
from app.util.get_version import get_app_version

logger = get_logger()
router = APIRouter()


@router.get("/")
async def root():
    logger.info("Example Log", {"extra": "values"})
    return {"message": "Welcome to the sdx-fastapi-boilerplate"}


@router.get("/health")
async def health():
    return {"message": "OK"}


@router.get("/version")
async def version():
    return {"version": get_app_version()}
