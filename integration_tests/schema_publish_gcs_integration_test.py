from sds_common.config.config import CONFIG
from sds_common.enums.buckets import Bucket
from sds_common.services.bucket_service import BucketService
from sds_common.test_helpers.bucket_loader import BucketLoader
from sds_common.test_helpers.common_test_data import test_schema_subscriber_id_success, test_schema_subscriber_id_fail
from sds_common.test_helpers.integration_helpers import poll_subscription, cleanup, pubsub_setup, inject_wait_time, \
    pubsub_purge_messages, pubsub_teardown
from sds_common.test_helpers.pub_sub_helper import PubSubHelper


def test_publish_schema_to_gcs():
    """
    Test publishing a schema via GCS happy path.

    - We drop a valid schema file into the GCS schema publish bucket.
    - We poll the schema_success_topic to check if the schema was published.
    - We assert that the schema was published successfully.
    """

    # Run cleanup
    cleanup()

    # Create a bucket service
    bucket_service = BucketService(
        Bucket.SCHEMA_PUBLISH_BUCKET, BucketLoader()
    )

    # Create a pubsub helper
    schema_error_pubsub_helper = PubSubHelper(
        CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID
    )
    schema_success_pubsub_helper = PubSubHelper(
        CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID
    )
    pubsub_setup(
        schema_error_pubsub_helper, test_schema_subscriber_id_fail
    )
    pubsub_setup(
        schema_success_pubsub_helper, test_schema_subscriber_id_success
    )
    inject_wait_time(3)  # Inject wait time to allow resources to complete setting up

    # Upload the file to the bucket
    bucket_service.upload_file_to_bucket("src/tests/test_data/test_schema_success.json")

    # Poll pubsub for a response
    messages = poll_subscription(
        schema_success_pubsub_helper, test_schema_subscriber_id_success
    )

    # Assertions
    assert messages is not None
    for message in messages:
        assert "guid" in message

    # Cleanup
    cleanup()
    inject_wait_time(3)  # Inject wait time to allow all messages to be processed
    pubsub_purge_messages(
        schema_success_pubsub_helper, test_schema_subscriber_id_success
    )
    pubsub_teardown(
        schema_success_pubsub_helper, test_schema_subscriber_id_success
    )

