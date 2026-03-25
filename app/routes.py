from fastapi import APIRouter, Request
from fastapi.params import Query
from lagom.integrations.fast_api import FastApiIntegration
from sdx_base.models.pubsub import get_message, Message, get_data
from starlette.responses import JSONResponse

from app import get_logger
from app.dependencies import build_container
from app.exceptions import NonCriticalException, DatasetException
from app.services.dataset_service import DatasetService
from app.services.schema_service import SchemaService
from app.settings import get_instance

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


@router.post("/events/schema/publish")
async def publish_schemas(
    request: Request,
    source: str = Query(
        "github",
        description="The source of the files specified in this request, are they from github or bucket? ",
        min_length=3,
        max_length=10,
    ),
    schema_service: SchemaService = DEPS.depends(SchemaService)
):
    """
    This endpoint handles a publishing schemas from a given
    location.
    """

    # Fetch the message from pubsub
    message: Message = await get_message(request)

    # Publish the new schemas
    schema_service.publish_new_schemas(
        source=source,
        file_list=get_data(message).split("\n")
    )

    # Return a status
    return 200


@router.get("/events/dataset/create")
async def create_dataset(
    dataset_service: DatasetService = DEPS.depends(DatasetService)
):
    """
    This endpoint handles creating a dataset
    """

    try:
        dataset_service.create_dataset()
    except NonCriticalException as e:

        # Return a status 200 (non critical exception)
        return JSONResponse(
            status_code=200,
            content={"success": True, "message": str(e)},
        )

    except DatasetException as e:

        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "Exception creating dataset: " + str(e)},
        )

    return JSONResponse(
        status_code=200,
        content={"success": True, "message": "Dataset created successfully"},
    )


@router.delete("/events/dataset/{dataset_id}")
async def delete_dataset(
    request: Request,
):
    """
    This endpoint deletes a dataset
    """
    pass

