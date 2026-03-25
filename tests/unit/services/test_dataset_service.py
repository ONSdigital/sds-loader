
import pytest

from app.exceptions.dataset_invalid_filename_exception import DatasetInvalidFilenameException
from app.exceptions.dataset_not_found_exception import DatasetNotFoundException
from app.exceptions.dataset_source_empty_exception import DatasetSourceEmptyException
from app.exceptions.dataset_validation_exception import DatasetValidationException
from app.factories.dataset_factories import RawDatasetFactory, DatasetMetadataWithoutIdFactory
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId, DatasetMetadata
from app.services.dataset_service import DatasetService


class TestCreateDataset:

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

        # Assert that because autodelete_dataset is False, the delete method is not called on the repo
        mock_dataset_source_repo.delete_raw_data.assert_not_called()

    def test_raises_exception_and_autodeletes_dataset_if_filename_invalid(
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
            autodelete_dataset = True  # Set autodelete to True

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

        # Assert the mock_dataset_source_repo.delete_raw_data method was called with the invalid filename
        mock_dataset_source_repo.delete_raw_data.assert_called_once_with("invalid-filename")

    def test_raises_exception_when_file_contents_are_invalid(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster
    ):
        """
        Test that when the dataset is read in from the source repository
        and its content does not conform to the expected format an exception is raised
        """

        # Mock the source repo to return a valid filename
        mock_dataset_source_repo.get_oldest_file.return_value = "valid-filename.json"

        # Mock the get_raw_data method to raise DatasetValidationException
        mock_dataset_source_repo.get_raw_data.side_effect = DatasetValidationException("Invalid dataset content")

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
        with pytest.raises(DatasetValidationException):
            service.create_dataset()

        # Assert that because autodelete_dataset is False, the delete method is not called on the repo
        mock_dataset_source_repo.delete_raw_data.assert_not_called()

    def test_raises_exception_and_autodeletes_when_file_contents_are_invalid(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster
    ):
        """
        Test that when the dataset is read in from the source repository
        and its content does not conform to the expected format an exception is raised
        """

        # Mock the source repo to return a valid filename
        mock_dataset_source_repo.get_oldest_file.return_value = "valid-filename.json"

        # Mock the get_raw_data method to raise DatasetValidationException
        mock_dataset_source_repo.get_raw_data.side_effect = DatasetValidationException("Invalid dataset content")

        # Create mock settings
        class MockSettings:
            autodelete_dataset = True

        # Create a DatasetService
        service = DatasetService(
            dataset_source_repo=mock_dataset_source_repo,
            dataset_storage_repo=mock_dataset_storage_repo,
            broadcaster=mock_broadcaster,
            settings=MockSettings(),
        )

        # Call create_dataset and assert that it raises the expected exception
        with pytest.raises(DatasetValidationException):
            service.create_dataset()

        # Assert the mock_dataset_source_repo.delete_raw_data method was called
        mock_dataset_source_repo.delete_raw_data.assert_called_once_with("valid-filename.json")

    def test_raises_exception_if_cannot_find_dataset_in_source_repo(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster
    ):
        """
        Test that if the specified filename for a dataset cannot be found in the source repository
        an exception is raised
        """

        # Mock the source repo to return a valid filename
        mock_dataset_source_repo.get_oldest_file.return_value = "valid-filename.json"

        # Mock the get_raw_data to return None, to simulate not found
        mock_dataset_source_repo.get_raw_data.return_value = None

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
        with pytest.raises(DatasetNotFoundException):
            service.create_dataset()

    def test_autodelete_after_successfully_reading_dataset(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster,
        raw_dataset_factory: RawDatasetFactory,
        dataset_metadata_without_id_factory: DatasetMetadataWithoutIdFactory
    ):
        """
        Test that when autodelete is set to True the dataset is automatically deleted
        after reading from the source repository
        """

        # ------------------------
        # Source repository mocks
        # ------------------------

        # Mock the source repo to return a valid JSON filename
        mock_dataset_source_repo.get_oldest_file.return_value = "valid-filename.json"

        # Mock the source repo to return valid RawDataset
        mock_dataset_source_repo.get_raw_data.return_value = raw_dataset_factory.build()

        # ------------------------
        # Storage repository mocks
        # ------------------------

        # Mock the storage repository (firestore)
        # to return a valid dataset
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = dataset_metadata_without_id_factory.build()

        # Create mock settings
        class MockSettings:
            autodelete_dataset = True

        # Create a DatasetService
        service = DatasetService(
            dataset_source_repo=mock_dataset_source_repo,
            dataset_storage_repo=mock_dataset_storage_repo,
            broadcaster=mock_broadcaster,
            settings=MockSettings(),
        )

        # Call create_dataset method
        service.create_dataset()

        # Assert the mock_dataset_source_repo.delete_raw_data method was called
        mock_dataset_source_repo.delete_raw_data.assert_called_once_with("valid-filename.json")

    def test_does_not_autodelete_after_successfully_reading_dataset_if_autodelete_false(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster,
        raw_dataset_factory: RawDatasetFactory,
        dataset_metadata_without_id_factory: DatasetMetadataWithoutIdFactory
    ):
        """
        Test that when autodelete is set to False the dataset should not be autodeleted
        """

        # ------------------------
        # Source repository mocks
        # ------------------------

        # Mock the source repo to return a valid JSON filename
        mock_dataset_source_repo.get_oldest_file.return_value = "valid-filename.json"

        # Mock the source repo to return valid RawDataset
        mock_dataset_source_repo.get_raw_data.return_value = raw_dataset_factory.build()

        # ------------------------
        # Storage repository mocks
        # ------------------------

        # Mock the storage repository (firestore)
        # to return a valid dataset
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = dataset_metadata_without_id_factory.build()

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

        # Assert the mock_dataset_source_repo.delete_raw_data method was not called
        mock_dataset_source_repo.delete_raw_data.assert_not_called()

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

    def test_increments_dataset_version_with_no_older_versions(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster,
        raw_dataset_factory: RawDatasetFactory,
    ):
        """
        Test that if a dataset does NOT exist for the current survey_id and period
        that the service correctly sets the version to 1 (since there are no older versions)
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
        # to have no previous versions (return None)
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = None

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
        assert dataset_metadata.sds_dataset_version == 1

    def test_stored_successfully_when_created(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster,
        raw_dataset_factory: RawDatasetFactory
    ):
        """
        Test the dataset_storage_repo is called correctly when a valid
        dataset is created
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
        # with some fake unit_data to test if this is correctly formatted on save
        mock_dataset_source_repo.get_raw_data.return_value = raw_dataset_factory.build(
            survey_id=survey_id,
            period_id=period_id,
            data=[
                {
                    "identifier": "abc",
                    "unit_data": ["hello", "world"],
                },
                {
                    "identifier": "def",
                    "unit_data": []
                },
                {
                    "identifier": "ghi",
                    "unit_data": ["test"]
                }
            ]
        )

        # ------------------------
        # Storage repository mocks
        # ------------------------

        # Mock the current storage repository (firestore) to force version 1
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = None

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

        # Assert the mock_dataset_storage_repo is called
        mock_dataset_storage_repo.store_dataset.assert_called_once()

        args, kwargs = mock_dataset_storage_repo.store_dataset.call_args

        # ------------------------
        # Check unit_data_identifiers
        # ------------------------

        unit_data_identifiers = kwargs['unit_data_identifiers']

        expected_identifiers = [
            "abc", "def", "ghi"
        ]

        assert unit_data_identifiers == expected_identifiers

        # ------------------------
        # Check unit_data_collection_with_metadata
        # ------------------------

        unit_data_collection_with_metadata = kwargs['unit_data_collection_with_metadata']

        # There should be 3 sets of unit_data
        assert len(unit_data_collection_with_metadata) == 3

        # Each one should have survey_id and period_id that match
        # unit_data should match what we mocked

        assert unit_data_collection_with_metadata[0].survey_id == survey_id
        assert unit_data_collection_with_metadata[0].period_id == period_id
        assert unit_data_collection_with_metadata[0].data == ["hello", "world"]

        assert unit_data_collection_with_metadata[1].survey_id == survey_id
        assert unit_data_collection_with_metadata[1].period_id == period_id
        assert unit_data_collection_with_metadata[1].data == []

        assert unit_data_collection_with_metadata[2].survey_id == survey_id
        assert unit_data_collection_with_metadata[2].period_id == period_id
        assert unit_data_collection_with_metadata[2].data == ["test"]

    def test_broadcasts_dataset_when_created_successfully(
        self,
        mock_dataset_source_repo: DatasetSourceRepositoryInterface,
        mock_dataset_storage_repo: DatasetStorageRepositoryInterface,
        mock_broadcaster,
        raw_dataset_factory: RawDatasetFactory,
    ):
        """
        Test that if a dataset is created successfully its metadata
        is broadcasted
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

        # Mock the current storage repository (firestore) to force version 1
        mock_dataset_storage_repo.get_latest_dataset_metadata.return_value = None

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

        # Get all the datasets broadcast to the mock_broadcaster
        broadcast_datasets = mock_broadcaster.broadcasted_datasets

        # Assert it was just the once
        assert len(broadcast_datasets) == 1

        # Fetch the broadcast dataset
        broadcast_dataset: DatasetMetadata = broadcast_datasets[0]

        assert broadcast_dataset.survey_id == survey_id
        assert broadcast_dataset.period_id == period_id




