from fastapi import APIRouter

from app import get_logger
from app.settings import get_instance

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
    return {"version": get_instance().app_version}
