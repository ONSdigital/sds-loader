from fastapi import FastAPI
from starlette.testclient import TestClient

from app.factories.dataset_factories import RawDatasetFactory
from app.routes import DEPS
from app.services.dataset_service import DatasetService
from tests.conftest import MockBroadcaster


class TestCreateDatasetEndpoint:

    def test_create_dataset_first_version(
        self,
        test_app: FastAPI,
        mock_dataset_source_repo,
        mock_dataset_storage_repo,
        mock_broadcaster: MockBroadcaster,
        raw_dataset_factory: RawDatasetFactory
    ):
        """
        Test a happy path for creating a dataset endpoint.

        - valid file in source repository
        - No other versions of this dataset exist (it will be v1)
        """

        # Use predictable survey_id and period_id
        survey_id = "123"
        period_id = "456"

        # ------------------------
        # Source repository mocks
        # ------------------------

        # Mock the source repo to return a valid JSON filename
        mock_dataset_source_repo.get_oldest_file.return_value = "valid-filename.json"

        # Mock the source repo to return valid RawDataset
        mock_dataset_source_repo.get_raw_data.return_value = raw_dataset_factory.build(
            survey_id=survey_id,
            period_id=period_id,
        )

        # ------------------------
        # Storage repository mocks
        # ------------------------

        # Mock the current storage repository (firestore) to force version 1
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = None

        with DEPS.override_for_test() as test_container:

            # For this test we set autodelete to True
            class MockSettings:
                autodelete_dataset: bool = True

            # Override the DatasetService dependencies with our mocks
            test_container[DatasetService] = DatasetService(
                dataset_source_repo=mock_dataset_source_repo,
                dataset_storage_repo=mock_dataset_storage_repo,
                broadcaster=mock_broadcaster,
                settings=MockSettings()
            )

            # Create a TestClient instance
            client = TestClient(test_app)

            # Make the get request to the endpoint
            response = client.get(
                "/events/dataset/create",
            )

            # Assert a 200
            assert response.status_code == 200






