from unittest import TestCase

import pytest
from sds_common.config.config import CONFIG
from sds_common.test_helpers.integration_helpers import (
    cleanup,
    inject_wait_time,
    poll_subscription,
    pubsub_purge_messages,
    pubsub_setup,
    pubsub_teardown,
)
from sds_common.test_helpers.pub_sub_helper import PubSubHelper
from sds_common.test_helpers.common_test_data import (
    test_schema_subscriber_id_success,
)


class SchemaPublishIntegrationTest(TestCase):
    @classmethod
    def setup_class(cls):
        cleanup()
        cls.schema_queue_pubsub_helper = PubSubHelper(
            CONFIG.PUBLISH_SCHEMA_QUEUE_TOPIC_ID
        )
        cls.schema_success_pubsub_helper = PubSubHelper(
            CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID
        )
        pubsub_setup(
            cls.schema_success_pubsub_helper, test_schema_subscriber_id_success
        )
        inject_wait_time(5)  # Inject wait time to allow resources properly set up

    @classmethod
    def teardown_class(cls) -> None:
        cleanup()
        inject_wait_time(3)  # Inject wait time to allow all message to be processed
        pubsub_purge_messages(
            cls.schema_success_pubsub_helper, test_schema_subscriber_id_success
        )
        pubsub_teardown(
            cls.schema_success_pubsub_helper, test_schema_subscriber_id_success
        )

    @pytest.mark.order(1)
    def test_publish_schema_success(self):
        """
        Test the publish-schema Cloud Function happy path.

        * We drop a message containing the filepath to a valid schema onto the queue.
        * We poll the schema_success_topic to check if the schema was published.
        * We assert that the schema was published successfully.
        """

        self.schema_queue_pubsub_helper.publish_message("schemas/test_schemas/test_schema_success.json")

        messages = poll_subscription(
            self.schema_success_pubsub_helper, test_schema_subscriber_id_success
        )

        assert messages is not None
        for message in messages:
            assert "guid" in message

