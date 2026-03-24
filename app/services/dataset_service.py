import datetime
import uuid
from typing import Protocol

from app import get_logger
from app.exceptions.dataset_validation_exception import DatasetValidationException
from app.exceptions.dataset_source_empty_exception import DatasetSourceEmptyException
from app.exceptions.dataset_invalid_filename_exception import DatasetInvalidFilenameException
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import RawDataset, DatasetMetadataWithoutId, UnitDataset, DatasetMetadata

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

        :raises DatasetSourceEmptyException: if there are no files in the dataset source repository
        :raises DatasetInvalidFilenameException: if the filename of the dataset to be created is not valid
        :raises DatasetValidationException: if the contents of the dataset are invalid
        :raises DatasetMetadataRetrivalException: if there is an issue retrieving the latest dataset metadata from the dataset storage repository
        :raises DatasetStoringException: if there is an issue storing the new dataset in the dataset storage repository
        """

        # Get the filename of the oldest dataset in the bucket
        dataset_filename: str | None = self.dataset_source_repo.get_oldest_file()

        # If the source repository is empty, there are no datasets to create
        if not dataset_filename:
            logger.warning(f"No dataset found to create, skipping process")
            raise DatasetSourceEmptyException("No datasets found in the dataset source repository")

        # Validate the filename
        if not self._validate_filename(dataset_filename):
            logger.warning(f"Filename: {dataset_filename} is not valid")

            # If autodelete_dataset is true, delete this from the bucket
            if self.settings.autodelete_dataset:
                self.dataset_source_repo.delete_raw_data(dataset_filename)
                logger.warning(f"Filename: {dataset_filename} has been deleted")

            raise DatasetInvalidFilenameException(f"Filename: {dataset_filename} is not valid")

        try:
            # Fetch the raw data for given filename from bucket
            raw_dataset: RawDataset = self.dataset_source_repo.get_raw_data(dataset_filename)
        except DatasetValidationException as e:
            logger.warning(f"Dataset with filename: {dataset_filename} failed validation with error: {e}")

            # If autodelete_dataset is true, delete this from the bucket
            if self.settings.autodelete_dataset:
                self.dataset_source_repo.delete_raw_data(dataset_filename)
                logger.warning(f"Filename: {dataset_filename} has been deleted")

            # Raise the error to skip processing this dataset
            raise e

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
                f"Found previous dataset version: {latest_dataset.sds_dataset_version} for survey {latest_dataset.survey_id}, period {latest_dataset.period_id}, incrementing version for new dataset")
            new_version = latest_dataset.sds_dataset_version + 1
        else:
            logger.info(
                f"Could not find a previous dataset version for survey {latest_dataset.survey_id}, period {latest_dataset.period_id}, setting version to 1")
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

        # Write to storage (firestore)
        self._save_dataset_to_storage(
            dataset_id=dataset_id,
            dataset_metadata=new_dataset_metadata,
            unit_data_collection_with_metadata=unit_data_collection_with_metadata,
            unit_data_identifiers=unit_data_identifiers,
        )

        # Create a DatasetMetadata object
        dataset_metadata = DatasetMetadata(
            dataset_id=dataset_id,
            **new_dataset_metadata.model_dump()
        )

        # Broadcast the dataset has been created (pubsub)
        self._broadcast_dataset_created(
            dataset_metadata=dataset_metadata
        )

        # TODO cleanup determine which to delete logic

    def _save_dataset_to_storage(
        self,
        dataset_id: str,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str],
    ):
        """
        Will write information about a new dataset to the storage repository

        :param dataset_id: Unique identifier of the dataset (guid)
        :param dataset_metadata: Metadata for this new dataset
        :param unit_data_collection_with_metadata: A list of the units in the dataset associated with this datasets metadata
        :param unit_data_identifiers: A list of each of the identifiers for the unit data in the dataset

        :raises DatasetStoringException: if there is an issue storing the dataset in the dataset storage repository
        """
        logger.info(f"Saving new dataset to storage repository: {dataset_id}")
        self.dataset_storage_repo.store_dataset(
            dataset_id=dataset_id,
            dataset_metadata=dataset_metadata,
            unit_data_collection_with_metadata=unit_data_collection_with_metadata,
            unit_data_identifiers=unit_data_identifiers,
        )
        logger.info(f"Dataset saved successfully: {dataset_id}")

    def _broadcast_dataset_created(self, dataset_metadata: DatasetMetadata):
        """
        Broadcast an event that the dataset has been created
        """
        logger.info(f"Broadcasting creation of new dataset {dataset_metadata.dataset_id}")
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
