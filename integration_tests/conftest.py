import pytest
from sds_common.config.config import CONFIG
from sds_common.enums.buckets import Bucket
from sds_common.repositories.bucket_loader import BucketLoader
from sds_common.services.bucket_service import BucketService
from sds_common.test_helpers.common_test_data import (
    test_schema_subscriber_id_fail,
    test_schema_subscriber_id_success,
)
from sds_common.test_helpers.integration_helpers import (
    cleanup,
    inject_wait_time,
    pubsub_purge_messages,
    pubsub_setup,
    pubsub_teardown,
)
from sds_common.test_helpers.pub_sub_helper import PubSubHelper


@pytest.fixture(scope="module")
def schema_pubsub_helpers() -> dict[str, PubSubHelper]:
    cleanup()

    schema_queue_pubsub_helper = PubSubHelper(CONFIG.PUBLISH_SCHEMA_QUEUE_TOPIC_ID)
    schema_error_pubsub_helper = PubSubHelper(CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID)
    schema_success_pubsub_helper = PubSubHelper(CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID)

    pubsub_setup(schema_error_pubsub_helper, test_schema_subscriber_id_fail)
    pubsub_setup(schema_success_pubsub_helper, test_schema_subscriber_id_success)
    inject_wait_time(3)  # Allow pubsub resources to finish provisioning.

    yield {
        "queue": schema_queue_pubsub_helper,
        "error": schema_error_pubsub_helper,
        "success": schema_success_pubsub_helper,
    }

    cleanup()
    inject_wait_time(3)  # Allow in-flight messages to settle before purge/teardown.
    pubsub_purge_messages(schema_error_pubsub_helper, test_schema_subscriber_id_fail)
    pubsub_purge_messages(schema_success_pubsub_helper, test_schema_subscriber_id_success)
    pubsub_teardown(schema_error_pubsub_helper, test_schema_subscriber_id_fail)
    pubsub_teardown(schema_success_pubsub_helper, test_schema_subscriber_id_success)


@pytest.fixture(scope="module")
def bucket_service() -> BucketService:
    return BucketService(Bucket.SCHEMA_PUBLISH_BUCKET, BucketLoader())

