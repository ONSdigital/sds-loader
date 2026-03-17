import base64
from unittest.mock import MagicMock, Mock

from fastapi import FastAPI
from sdx_base.models.pubsub import Message, Envelope
from starlette.testclient import TestClient

from app.interfaces.schema_repository_interface import SchemaRepositoryInterface
from app.models.schema_metadata import SchemaMetadata
from app.repositories.sds_schema_repository import SdsSchemaRepository
from app.routes import DEPS
from app.services.schema_service import SchemaService

import requests
from sds_common.schema.schema import Schema as CommonSchema


class MockSchemaRequest:

    def __init__(
        self,
        schema_metadata: list[SchemaMetadata],
    ):
        self.schema_metadata = schema_metadata
        self.posted_schemas = []

    def get_all_schema_metadata(self) -> requests.Response:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = self.schema_metadata
        return mock_response

    def post_schema(self, schema: CommonSchema) -> requests.Response:
        self.posted_schemas.append(schema)
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        return mock_response


class TestPublishSchemasEndpoint:

    def test_using_sds_schema_repository(
        self,
        test_app: FastAPI,
        schema_repo: MagicMock,
        schema_service: SchemaService,
    ):
        """
        Test our publish schemas endpoint
        """

        existing_schemas = [
            SchemaMetadata(
                guid="abc",
                survey_id="123",
                schema_location="abc",
                sds_schema_version=1,
                sds_published_at="2001",
                schema_version="v1",
                title="Hello World",
            )
        ]

        with DEPS.override_for_test() as test_container:
            # We use the SdsSchema implementation
            test_container[SchemaRepositoryInterface] = SdsSchemaRepository(
                MockSchemaRequest(existing_schemas)
            )

            # Create fake files to simulate new added schemas sent to loader
            fake_filenames = "schemas/abc/v1.json\nschemas/def/v3.json\nschemas/ghi/v2.json"

            # Encode the message
            fake_data = base64.b64encode(
                fake_filenames.encode("utf-8")
            ).decode("utf-8")

            # Create a fake Message object to simulate a pubsub object
            self.message: Message = {
                "attributes": {},
                "data": fake_data,
                "message_id": "",
                "publish_time": "",
            }

            # Wrap in an envelope object
            self.envelope: Envelope = {"message": self.message, "subscription": ""}

            # Create a TestClient instance
            client = TestClient(test_app)

            # Make a POST request to the /publish-schemas endpoint as pubsub would
            response = client.post("/publish-schemas", json=self.envelope)

            # Assert a 200 status code
            assert response.status_code == 200

            # Check our MockSchemaRequest and check the posted schemas match the expected schemas

