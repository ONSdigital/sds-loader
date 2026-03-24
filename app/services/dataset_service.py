import datetime
import uuid
from typing import Protocol

from app import get_logger
from app.exceptions.dataset_validation_exception import DatasetValidationException
from app.exceptions.empty_dataset_source_exception import EmptyDatasetSourceException
from app.exceptions.invalid_dataset_filename_exception import InvalidDatasetFilenameException
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import RawDataset, RawDatasetWithoutData, DatasetMetadataWithoutId

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
            raw_dataset: RawDataset = self.dataset_source_repo.get_raw_data(oldest_filename)
        except DatasetValidationException as e:
            logger.warning(f"Dataset with filename: {oldest_filename} failed validation with error: {e}")

            # If autodelete_dataset is true, delete this from the bucket
            if self.settings.autodelete_dataset:
                self.dataset_source_repo.delete_raw_data(oldest_filename)
                logger.warning(f"Filename: {oldest_filename} has been deleted")

            # Raise the error to skip processing this dataset
            raise e

        # Process the new dataset
        logger.info("Creating new dataset ...")

        """
        ----------------------
        Refactor
        ----------------------
        """

        # Generate Guid
        guid = str(uuid.uuid4())

        # Remove the data from the raw data
        raw_dataset_without_data = RawDatasetWithoutData(
            survey_id=raw_dataset.survey_id,
            period_id=raw_dataset.period_id,
            form_types=raw_dataset.form_types,
            title=raw_dataset.title,
        )

        # TODO Returns a copy of the create-dataset with added metadata.

        filename = oldest_filename
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        data = raw_dataset.data
        latest_dataset: DatasetMetadataWithoutId = self.dataset_storage_repo.get_latest_dataset(
            raw_dataset.survey_id, raw_dataset.period_id
        )

        # Determine next dataset version
        next_version = latest_dataset.sds_dataset_version + 1

        dataset_metadata_without_id = {
            "survey_id": raw_dataset.survey_id,
            "period_id": raw_dataset.period_id,
            "form_types": raw_dataset.form_types,
            "filename": filename,
            "sds_published_at": now,
            "total_reporting_units": len(data),
            "sds_dataset_version": next_version,
        }

        # Add the optional title field
        if "title" in raw_dataset:
            dataset_metadata_without_id["title"] = raw_dataset.title

        # TODO Transforms the new unit data to a new format for storing in firestore.

        data = raw_dataset.data  # todo remove duplication
        dataset_id = guid

        unit_data_collection_with_metadata = [
            {
                "dataset_id": dataset_id,
                "survey_id": dataset_metadata_without_id["survey_id"],
                "period_id": dataset_metadata_without_id["period_id"],
                "form_types": dataset_metadata_without_id["form_types"],
                "data": item["unit_data"],
            }
            for item in data
        ]

        # TODO extracted_unit_data_identifiers

        extracted_unit_data_identifiers = [
            item["identifier"] for item in data
        ]

        # TODO dataset_publish_response

        """
        Writes dataset metadata and unit data to Firestore in batches and checks the unit data count matches the total
        reporting units.
        """




        # Write to firestore

        # Publish to topic



    def delete_dataset(self):
        """
        Delete a dataset marked for deletion (only one)
        """

        # Fetch a single dataset guid from the marked_for_deletion collection in firestore

        # Get the dataset document based on this guid

        # Delete the document

        # Update the status of the record in marked_for_deletion collection to deleted
        pass
