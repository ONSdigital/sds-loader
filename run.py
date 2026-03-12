from pathlib import Path

from sdx_base.server.server import RouterConfig
from sdx_base.server.tx_id import txid_not_applicable

from app.routes import router
from app.settings import Settings
from sdx_base.run import run


if __name__ == "__main__":
    proj_root = Path(__file__).parent

    routers = [RouterConfig(router=router, tx_id_getter=txid_not_applicable)]

    run(Settings, proj_root=proj_root, routers=routers)
