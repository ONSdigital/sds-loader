import os
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import FastAPI
from sdx_base.run import initialise
from sdx_base.server.server import RouterConfig
from sdx_base.server.tx_id import txid_not_applicable
from sdx_base.settings.app import AppSettings

from app.interfaces.schema_repository_interface import SchemaRepositoryInterface
from app.routes import router
from app.services.schema_service import SchemaService
from app.settings import ROOT


# ------------------------
# Fixtures
# ------------------------

@pytest.fixture
def schema_repo() -> MagicMock:
    return create_autospec(SchemaRepositoryInterface)


@pytest.fixture
def schema_service(schema_repo: MagicMock) -> SchemaService:
    return SchemaService(schema_repo)


@pytest.fixture
def test_app() -> FastAPI:
    """
    This fixture will create a FastAPI app using SDX base
    """

    # Set environment variable for testing
    os.environ["PROJECT_ID"] = "ons-sdx-sandbox"

    # Fake Settings
    class FakeSettings(AppSettings):
        project_id: str

    class MockSecretReader:
        def get_secret(self, _project_id: str, secret_id: str) -> str:
            return "secret"

    return initialise(
        settings=FakeSettings,
        routers=[
            RouterConfig(
                router, tx_id_getter=txid_not_applicable
            )
        ],
        proj_root=ROOT,
        secret_reader=MockSecretReader,
    )
