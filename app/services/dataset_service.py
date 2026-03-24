from typing import Protocol

from app import get_logger
from app.exceptions.dataset_validation_exception import DatasetValidationException
from app.exceptions.empty_dataset_source_exception import EmptyDatasetSourceException
from app.exceptions.invalid_dataset_filename_exception import InvalidDatasetFilenameException
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import RawDataset

logger = get_logger()


class DatasetSettings(Protocol):
    autodelete_dataset: bool


class DatasetService:
    """
    DatasetService provides a way to manage
    datasets
    """

    def __init__(
        self,
        dataset_source_repo: DatasetSourceRepositoryInterface,
        dataset_storage_repo: DatasetStorageRepositoryInterface,
        settings: DatasetSettings
    ):
        self.dataset_source_repo = dataset_source_repo
        self.dataset_storage_repo = dataset_storage_repo
        self.settings = settings

    def _validate_filename(self, filename: str) -> bool:
        """
        Check if the filename given to the dataset is valid

        TODO maybe put into service?
        """
        if filename.endswith(".json"):
            return True
        return False

    def create_dataset(self):
        """
        Create a new dataset (only one)

        :raises EmptyDatasetSourceException if there are no files in the dataset source repository
        :raises InvalidDatasetFilenameException if the filename of the dataset to be created is not valid
        :raises DatasetValidationException if the contents of the dataset are invalid
        """

        # Get the oldest filename in the bucket
        oldest_filename: str | None = self.dataset_source_repo.get_oldest_file()

        # If the source repository is empty, there are no datasets to create
        if not oldest_filename:
            logger.warning(f"No dataset found to create, skipping process")
            raise EmptyDatasetSourceException("No datasets found in the dataset source repository")

        # Validate the filename
        if not self._validate_filename(oldest_filename):
            logger.warning(f"Filename: {oldest_filename} is not valid")

            # If autodelete_dataset is true, delete this from the bucket
            if self.settings.autodelete_dataset:
                self.dataset_source_repo.delete_raw_data(oldest_filename)
                logger.warning(f"Filename: {oldest_filename} has been deleted")

            raise InvalidDatasetFilenameException(f"Filename: {oldest_filename} is not valid")

        try:
            # Fetch the raw data for given filename from bucket
            raw_data: RawDataset = self.dataset_source_repo.get_raw_data(oldest_filename)
        except DatasetValidationException as e:
            logger.warning(f"Dataset with filename: {oldest_filename} failed validation with error: {e}")

            # If autodelete_dataset is true, delete this from the bucket
            if self.settings.autodelete_dataset:
                self.dataset_source_repo.delete_raw_data(oldest_filename)
                logger.warning(f"Filename: {oldest_filename} has been deleted")

            # Raise the error to skip processing this dataset and move on to the next one
            raise e


        # Process the new dataset

            # Generate Guid

            # Remove the data

            # Write to firestore

            # Publish to topic

        pass

    def delete_dataset(self):
        """
        Delete a dataset marked for deletion (only one)
        """

        # Fetch a single dataset guid from the marked_for_deletion collection in firestore

        # Get the dataset document based on this guid

        # Delete the document

        # Update the status of the record in marked_for_deletion collection to deleted
        pass
