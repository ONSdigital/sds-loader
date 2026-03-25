import datetime
import uuid
from typing import Protocol

from app import get_logger
from app.exceptions.dataset_not_found_exception import DatasetNotFoundException
from app.exceptions.dataset_validation_exception import DatasetValidationException
from app.exceptions.dataset_source_empty_exception import DatasetSourceEmptyException
from app.exceptions.dataset_invalid_filename_exception import DatasetInvalidFilenameException
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import RawDataset, DatasetMetadataWithoutId, UnitDataset, DatasetMetadata

logger = get_logger()


class DatasetSettings(Protocol):
    """
    The settings needed for the dataset service
    """

    autodelete_dataset: bool


class BroadcastProtocol(Protocol):
    """
    Defines the protocol for broadcasting messages
    for the dataset service
    """

    def broadcast(self, dataset_metadata: DatasetMetadata) -> None:
        ...


class DatasetService:
    """
    DatasetService provides a way to manage
    datasets
    """

    def __init__(
        self,
        dataset_source_repo: DatasetSourceRepositoryInterface,
        dataset_storage_repo: DatasetStorageRepositoryInterface,
        broadcaster: BroadcastProtocol,
        settings: DatasetSettings
    ):
        self.dataset_source_repo = dataset_source_repo
        self.dataset_storage_repo = dataset_storage_repo
        self.broadcaster = broadcaster
        self.settings = settings

    def _validate_filename(self, filename: str) -> bool:
        """
        Check if the filename given to the dataset is valid

        TODO maybe put into service?
        """
        if filename.endswith(".json"):
            return True
        return False

    def _autodelete_dataset(self, dataset_filename: str):
        """
        If autodelete_dataset is true, delete
        the given filename from the source repository

        :param dataset_filename: filename of the dataset autodelete

        :raises DatasetDeletionException: if there is an issue deleting the file from the source repository
        """
        if self.settings.autodelete_dataset:
            self.dataset_source_repo.delete_raw_data(dataset_filename)
            logger.warning(f"Filename: {dataset_filename} has been deleted")

    def create_dataset(self):
        """
        Create a new dataset (only one)

        :raises DatasetSourceEmptyException: if there are no files in the dataset source repository
        :raises DatasetInvalidFilenameException: if the filename of the dataset to be created is not valid
        :raises DatasetValidationException: if the contents of the dataset are invalid
        :raises DatasetMetadataRetrivalException: if there is an issue retrieving the latest dataset metadata from the dataset storage repository
        :raises DatasetStoringException: if there is an issue storing the new dataset in the dataset storage repository
        :raises DatasetNotFoundException: if the dataset to be created cannot be found in the dataset source repository
        :raises DatasetDeletionException if there is an issue deleting the dataset from the source repository
        """

        logger.info(f"Starting create dataset process...")

        # Get the filename of the oldest dataset in the bucket
        dataset_filename: str | None = self.dataset_source_repo.get_oldest_filename()

        # If the source repository is empty, there are no datasets to create
        if not dataset_filename:
            logger.warning(f"No dataset found to create, skipping process")
            raise DatasetSourceEmptyException("No datasets found in the dataset source repository")

        # Validate the filename
        if not self._validate_filename(dataset_filename):
            logger.warning(f"Filename: {dataset_filename} is not valid")

            # Delete the dataset
            self._autodelete_dataset(dataset_filename)

            raise DatasetInvalidFilenameException(f"Filename: {dataset_filename} is not valid")

        logger.info(f"Fetching latest dataset from source repository: {dataset_filename}")

        try:
            # Fetch the raw data for given filename from bucket
            raw_dataset: RawDataset = self.dataset_source_repo.get_raw_data(dataset_filename)
        except DatasetValidationException as e:
            logger.error(f"Dataset with filename: {dataset_filename} failed validation with error: {e}")

            # Delete the dataset
            self._autodelete_dataset(dataset_filename)

            raise e

        # If the dataset could not be found
        if not raw_dataset:
            raise DatasetNotFoundException(f"{dataset_filename} could not be found in the dataset source repository")

        # TODO should this be done in cleanup method?
        # Delete the dataset
        self._autodelete_dataset(dataset_filename)

        # Process the new dataset
        logger.info("Creating new dataset ...")

        # Generate Guid
        dataset_id = str(uuid.uuid4())

        # Generate a current timestamp
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get the latest dataset based on survey_id and period_id
        latest_dataset: DatasetMetadataWithoutId = self.dataset_storage_repo.get_latest_dataset_metadata(
            raw_dataset.survey_id, raw_dataset.period_id
        )

        # Determine next dataset version based on the latest dataset version
        if latest_dataset:
            logger.info(
                f"Found previous dataset version: {latest_dataset.sds_dataset_version} for survey {raw_dataset.survey_id}, period {raw_dataset.period_id}, incrementing version for new dataset")
            new_version = latest_dataset.sds_dataset_version + 1
        else:
            logger.info(
                f"Could not find a previous dataset version for survey {raw_dataset.survey_id}, period {raw_dataset.period_id}, setting version to 1")
            new_version = 1

        # Create a new dataset_metadata object to store
        new_dataset_metadata = DatasetMetadataWithoutId(
            survey_id=raw_dataset.survey_id,
            period_id=raw_dataset.period_id,
            form_types=raw_dataset.form_types,
            filename=dataset_filename,
            sds_published_at=now,
            total_reporting_units=len(raw_dataset.data),
            sds_dataset_version=new_version,
        )

        # Add the optional title field
        if "title" in raw_dataset:
            new_dataset_metadata.title = raw_dataset.title

        # Create a list of all the unit data from the dataset and associated each unit with
        # metadata about the new dataset

        unit_data_collection_with_metadata: list[UnitDataset] = [
            UnitDataset(
                dataset_id=dataset_id,
                survey_id=new_dataset_metadata.survey_id,
                period_id=new_dataset_metadata.period_id,
                form_types=new_dataset_metadata.form_types,
                data=item.unit_data,
            )

            for item in raw_dataset.data
        ]

        # Fetch a list of the identifiers for the unit data in the dataset
        unit_data_identifiers = [
            item.identifier for item in raw_dataset.data
        ]

        # Write the new dataset to storage (firestore)

        logger.info(f"Saving new dataset to storage repository: {dataset_id}")
        self.dataset_storage_repo.store_dataset(
            dataset_id=dataset_id,
            dataset_metadata=new_dataset_metadata,
            unit_data_collection_with_metadata=unit_data_collection_with_metadata,
            unit_data_identifiers=unit_data_identifiers,
        )
        logger.info(f"Dataset saved to storage successfully: {dataset_id}")

        # Create a DatasetMetadata object
        dataset_metadata = DatasetMetadata(
            dataset_id=dataset_id,
            **new_dataset_metadata.model_dump()
        )

        # Broadcast the dataset has been created (pubsub)
        self.broadcaster.broadcast(dataset_metadata)

        logger.info(f"Cleaning up for: {dataset_id}")

        # Cleanup any other versions of the dataset if necessary
        self._cleanup(
            survey_id=dataset_metadata.survey_id,
            period_id=dataset_metadata.period_id,
            version=new_dataset_metadata.sds_dataset_version,
        )

        logger.info(f"Dataset creation process completed: {dataset_id}")

    def _cleanup(
        self,
        survey_id: str,
        period_id: str,
        version: int
    ):
        """
        Determine if the other versions of the dataset should be deleted
        """
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
