from fastapi import APIRouter, Request, Response
from lagom.integrations.fast_api import FastApiIntegration
from sdx_base.models.pubsub import get_message, Message, get_data

from app import get_logger
from app.dependencies import build_container
from app.services.schema_service import SchemaService
from app.settings import get_instance

logger = get_logger()
router = APIRouter()
DEPS = FastApiIntegration(build_container())


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
async def handle(
    request: Request,
    schema_service: SchemaService = DEPS.depends(SchemaService)
):
    # Fetch the message from pubsub
    message: Message = await get_message(request)

    # Publish the new schemas
    schema_service.publish_new_schemas(
        get_data(message).split("\n")
    )


