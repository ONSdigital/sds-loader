from fastapi import FastAPI
from pathlib import Path
from requests import Request
from sdx_base.models.pubsub import Envelope
from sdx_base.run import initialise
from sdx_base.server.server import RouterConfig
from sdx_base.server.servers import default_server
from sdx_base.server.tx_id import txid_not_applicable, txid_from_request

from app.middleware.timing import TimingMiddleware
from app.routes import router
from app.settings import Settings, ROOT, get_instance

# Description for the OpenAPI documentation, which will be displayed in the API docs /docs or /redoc
description = """

## Documentation

 TODO - put link to documentation

"""


def load_startup_banner() -> str:
    """
    Load the ascii banner
    """
    banner_path = Path(ROOT) / "banner.txt"
    try:
        return banner_path.read_text()
    except (OSError, UnicodeDecodeError):
        return "sds-loader"


async def smart_txid(request: Request) -> str:
    """
    Extract the tx_id from the request
    """
    if request.method == "GET":
        return await txid_not_applicable(request)
    envelope: Envelope = await request.json()
    if "message" in envelope:
        return await txid_from_request(request)
    return await txid_not_applicable(request)


# Basic router configuration
router_1 = RouterConfig(
    router, tx_id_getter=smart_txid
)


# Initialize the FastAPI app with the specified settings, routers, and project root.
app: FastAPI = initialise(
    settings=Settings,
    routers=[router_1],
    middleware=[TimingMiddleware],
    proj_root=ROOT
)

# Fetch the populated settings
settings = get_instance()


# Add the description to the OpenAPI schema for better documentation.
app.description = description

if __name__ == "__main__":
    print(load_startup_banner())

    default_server(
        app,
        port=settings.port
    )
