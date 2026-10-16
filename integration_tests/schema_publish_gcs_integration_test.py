from unittest import TestCase

import pytest
from sds_common import (
    CONFIG,
    SdsCommon,
    cleanup,
    inject_wait_time,
    poll_subscription,
    pubsub_purge_messages,
    pubsub_setup,
    pubsub_teardown,
)
from sds_common.test_helpers.common_test_data import test_schema_subscriber_id_success

from app.routes import logger


class SchemaPublishGcsIntegrationTest(TestCase):
    @classmethod
    def setup_class(cls):
        cls.common = SdsCommon()
        cleanup()
        cls.bucket_service = cls.common.gcs_publisher.file_service
        pubsub_setup(test_schema_subscriber_id_success, CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID)
        inject_wait_time(10)  # Inject wait time to allow resources to complete setting up

    @classmethod
    def teardown_class(cls) -> None:
        cleanup()
        inject_wait_time(3)  # Inject wait time to allow all messages to be processed
        pubsub_purge_messages(test_schema_subscriber_id_success, CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID)
        pubsub_teardown(test_schema_subscriber_id_success, CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID)

    @pytest.mark.order(1)
    def test_publish_schema_to_gcs(self):
        """
        Test publishing a schema via GCS happy path.

        *We drop a valid schema file into the GCS schema publish bucket
        *We poll the schema_success_topic to check if the schema was published.
        *We assert that the schema was published successfully.

        """

        logger.info(f"Success topic: {CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID}")

        self.bucket_service.upload("integration_tests/test_data/test_schema_success.json")

        messages = poll_subscription(test_schema_subscriber_id_success, CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID)

        logger.info(f"Received messages: {messages}")

        assert messages is not None
        for message in messages:
            assert "guid" in message
