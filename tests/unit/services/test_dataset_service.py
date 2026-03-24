
import pytest

from app.exceptions.dataset_invalid_filename_exception import DatasetInvalidFilenameException
from app.exceptions.dataset_source_empty_exception import DatasetSourceEmptyException
from app.factories.dataset_factories import RawDatasetFactory, DatasetMetadataWithoutIdFactory
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId
from app.services.dataset_service import DatasetService


class TestCreateDataset:

    def test_raises_exception_if_filename_invalid(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster
    ):
        """
        Test that the filename in the dataset source repository is invalid
        and an exception is raised
        """

        # Mock the source repo to return a filename that is invalid
        mock_dataset_source_repo.get_oldest_file.return_value = "invalid-filename"

        # Create mock settings
        class MockSettings:
            autodelete_dataset = False

        # Create a DatasetService
        service = DatasetService(
            dataset_source_repo=mock_dataset_source_repo,
            dataset_storage_repo=mock_dataset_storage_repo,
            broadcaster=mock_broadcaster,
            settings=MockSettings(),
        )

        # Call create_dataset and assert that it raises the expected exception
        with pytest.raises(DatasetInvalidFilenameException):
            service.create_dataset()

    def test_raises_exception_if_source_is_empty(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster
    ):
        """
        Test that when the dataset source repository where datasets are picked up from is empty
        an exception is raised
        """

        # Mock the source repo to always return None (no files)
        mock_dataset_source_repo.get_oldest_file.return_value = None

        # Create mock settings
        class MockSettings:
            autodelete_dataset = False

        # Create a DatasetService
        service = DatasetService(
            dataset_source_repo=mock_dataset_source_repo,
            dataset_storage_repo=mock_dataset_storage_repo,
            broadcaster=mock_broadcaster,
            settings=MockSettings(),
        )

        # Call create_dataset and assert that it raises the expected exception
        with pytest.raises(DatasetSourceEmptyException):

            service.create_dataset()

    def test_increments_dataset_version(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster,
        raw_dataset_factory: RawDatasetFactory,
        dataset_metadata_without_id_factory: DatasetMetadataWithoutIdFactory
    ):
        """
        Test that if a dataset exists for the current survey_id and period
        that the service correctly increments the dataset version
        """

        # Use predictable survey_id and period
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

        # Mock the current storage repository (firestore)
        # Having a sds_dataset_version of 3
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = dataset_metadata_without_id_factory.build(
            survey_id=survey_id,
            period_id=period_id,
            sds_dataset_version=3,
        )

        # Create mock settings
        class MockSettings:
            autodelete_dataset = False

        # Create a DatasetService
        service = DatasetService(
            dataset_source_repo=mock_dataset_source_repo,
            dataset_storage_repo=mock_dataset_storage_repo,
            broadcaster=mock_broadcaster,
            settings=MockSettings(),
        )

        # Call create_dataset method
        service.create_dataset()

        # Assert we only called store_dataset once
        mock_dataset_storage_repo.store_dataset.assert_called_once()

        # Get the arguments the repository was called with
        args, kwargs = mock_dataset_storage_repo.store_dataset.call_args

        # Extract just the dataset_metadata argument
        dataset_metadata: DatasetMetadataWithoutId = kwargs['dataset_metadata']

        # Assert the metadata contains correct data
        assert dataset_metadata.survey_id == survey_id
        assert dataset_metadata.period_id == period_id
        assert dataset_metadata.sds_dataset_version == 4




