from fastapi import APIRouter, Request, Response
from sdx_base.models.pubsub import get_message, Message

from app import get_logger
from app.settings import get_instance

logger = get_logger()
router = APIRouter()


@router.get("/")
async def root():
    logger.info("Example Log", {"extra": "values"})
    return {"message": "Welcome to sds-loader!"}


@router.get("/health")
async def health():
    return {"message": "OK"}


@router.get("/version")
async def version():
    return {"version": get_instance().app_version}


@router.post("/publish-schemas") # TODO: make a good name pls
async def handle(request: Request) -> Response:
    message: Message = await get_message(request)
