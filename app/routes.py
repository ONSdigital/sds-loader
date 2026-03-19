from fastapi import APIRouter, Request
from lagom.integrations.fast_api import FastApiIntegration
from sdx_base.models.pubsub import get_message, Message, get_data

from app import get_logger
from app.dependencies import build_container
from app.services.schema_service import SchemaService
from app.settings import get_instance
from app.utils.schema_file_parser import SchemaFileParser

logger = get_logger()
router = APIRouter()
DEPS = FastApiIntegration(build_container())

# ------------------------
# core routes
# ------------------------


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

# ------------------------
# event driven endpoints
# ------------------------


@router.post("/events/publish-schemas")
async def publish_schemas(
    request: Request,
    schema_service: SchemaService = DEPS.depends(SchemaService)
):
    """
    This endpoint handles a pubsub request. The payload should contain
    a newline delimited list of filenames to publish
    """
    # Fetch the message from pubsub
    message: Message = await get_message(request)

    # Publish the new schemas
    schema_service.publish_new_schemas(

        # Extract the newline separated files into a list
        SchemaFileParser().parse(get_data(message))
    )

    # Return a status message
    return {"message": "OK"}



