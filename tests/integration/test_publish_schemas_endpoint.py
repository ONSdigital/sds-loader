import base64
from typing import Callable

from fastapi import FastAPI
from sdx_base.models.pubsub import Message, Envelope
from starlette.testclient import TestClient

from app.routes import DEPS
from app.services.schema_service import SchemaService, SchemaPublisher


class FakePublisher:
    def __init__(self, side_effects: dict[str, Callable]):
        self.side_effects = side_effects
        self.published_schemas = []

    def publish_schema(self, file_name: str):
        self.published_schemas.append(file_name)
        if file_name in self.side_effects:
            self.side_effects[file_name]()


class TestPublishSchemasEndpoint:

    def test_publish_schemas_with_2_valid_schemas_and_others_are_invalid(
        self,
        test_app: FastAPI,
        schema_service: SchemaService,
    ):
        """
        Test our publish schemas endpoint
        """

        fake_publisher = FakePublisher(
            {}
        )

        with DEPS.override_for_test() as test_container:

            # Use a fake publisher for the test and add a side effect for v1.json to raise an error
            test_container[SchemaPublisher] = fake_publisher

            # Create fake files to simulate new added schemas sent to loader
            received_filenames = "schemas/abc/v1.json\nscripts/doSomething.js\nschemas/v1_template.json\nschemas/abc/v3.json"

            # Encode the message
            encoded_data = base64.b64encode(
                received_filenames.encode("utf-8")
            ).decode("utf-8")

            # Create a fake Message object to simulate a pubsub object
            self.message: Message = {
                "attributes": {},
                "data": encoded_data,
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

            # Assert the two valid schemas are in published_schemas on our FakePublisher
            assert "schemas/abc/v1.json" in fake_publisher.published_schemas
            assert "schemas/abc/v3.json" in fake_publisher.published_schemas

            # Assert only two items in the published_schemas
            assert len(fake_publisher.published_schemas) == 2

