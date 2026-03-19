from fastapi import FastAPI
from requests import Request
from sdx_base.run import run, initialise
from sdx_base.server.server import RouterConfig
from sdx_base.server.servers import default_server
from sdx_base.server.tx_id import txid_not_applicable

from app.routes import router
from app.settings import Settings, ROOT, get_instance

# Description for the OpenAPI documentation, which will be displayed in the API docs /docs or /redoc
description = """

## Documentation

 TODO - put link to documentation

"""


async def txid_from_goon(request: Request) -> str:
    print("GOOON")
    return "123"


# Basic router configuration
router_1 = RouterConfig(
    router, tx_id_getter=txid_not_applicable
)


# Initialize the FastAPI app with the specified settings, routers, and project root.
app: FastAPI = initialise(
    settings=Settings,
    routers=[router_1],
    proj_root=ROOT
)

# Fetch the populated settings
settings = get_instance()


# Add the description to the OpenAPI schema for better documentation.
app.description = description

if __name__ == "__main__":

    default_server(
        app,
        port=settings.port
    )
