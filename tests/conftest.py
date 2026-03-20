import os
from typing import Callable

import pytest
from fastapi import FastAPI
from sdx_base.run import initialise
from sdx_base.server.server import RouterConfig
from sdx_base.server.tx_id import txid_not_applicable
from sdx_base.settings.app import AppSettings

from app.routes import router
from app.settings import ROOT

# ------------------------
# Testing classes
# ------------------------


class MockPublisher:
    """
    Mock Publisher class, allows
    tracking of published schemas and side effects for testing purposes.
    """
    def __init__(self, label: str):
        self.label = label
        self.side_effects = {}
        self.published_schemas = []

    def add_side_effect(self, file_name: str, side_effect: Callable):
        self.side_effects[file_name] = side_effect

    def publish_schema(self, file_name: str):
        if file_name in self.side_effects:
            try:
                self.side_effects[file_name]()
            except:
                return

        self.published_schemas.append(file_name)

# ------------------------
# Fixtures
# ------------------------


@pytest.fixture
def mock_repo_publisher() -> MockPublisher:
    return MockPublisher(
        label="repo publisher"
    )


@pytest.fixture
def mock_bucket_publisher() -> MockPublisher:
    return MockPublisher(
        label="bucket publisher",
    )


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
