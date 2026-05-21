import unittest
from sds_common.repositories.bucket_loader import BucketLoader
from sds_common.services.bucket_service import BucketService
from sds_common.services.http_service import HttpService
from sds_common.services.sds_dataset_request_service import SdsDatasetRequestService
from sds_common.test_helpers.integration_helpers import cleanup, inject_wait_time
from sds_common.enums.buckets import Bucket
from sds_common.config.config import CONFIG
from unittest import TestCase
from google.cloud import storage
from sds_common.test_helpers.pub_sub_helper import PubSubHelper

from app.settings import Settings


class DatasetPublishIntegrationTest(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cleanup()
        cls.schema_queue_pubsub_helper = PubSubHelper(
            CONFIG.PUBLISH_DATASET_TOPIC_ID
        )
        cls.bucket_service = BucketService(
            Bucket.DATASET_BUCKET, BucketLoader()
        )
        cls.sds_dataset_request_service = SdsDatasetRequestService()
        cls.dataset_create_endpoint = f"{CONFIG.SDS_URL}{CONFIG.DATASET_CREATE_PATH}"

    @classmethod
    def teardown_class(cls) -> None:
        cleanup()

    def test_dataset_publish_success(self):
        """
        Test whether the dataset is published successfully.

        * We drop a dataset into the dataset bucket.
        * We call the /events/dataset/create endpoint to trigger the dataset publish process.
        * We assert that the endpoint returns 200.
        * We check to see if the file has been deleted from the bucket.
        * We call SDS metadata endpoint to check if the dataset is published with the correct survey ID.
        """

        self.bucket_service.upload_file_to_bucket("integration_tests/test_data/dummy_dataset_40_encrypted.json")

        response = self.sds_dataset_request_service.get_dataset_create()

        self.assertEqual(200, response.status_code)

        if Settings.autodelete_dataset:
            self.assertFalse(
                self.bucket_service.check_file_exists("dummy_dataset_40_encrypted.json"),
                "Expected dataset file to be deleted from the bucket when autodelete is enabled.",
            )

        survey_id = "test_survey_id_dataset_create_test"
        period_id = "test_period_id"
        response = self.sds_dataset_request_service.get_dataset_metadata(survey_id, period_id)
        self.assertEqual(response, 1, "Expected one dataset metadata entry in the response.")
