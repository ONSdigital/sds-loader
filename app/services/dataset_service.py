import datetime
import uuid
from typing import Protocol

from app import get_logger
from app.enums.delete_status import DeleteStatus
from app.exceptions.dataset_deletion_empty_exception import DatasetDeletionEmptyException
from app.exceptions.dataset_deletion_exception import DatasetDeletionException
from app.exceptions.dataset_not_found_exception import DatasetNotFoundException
from app.exceptions.dataset_storing_exception import DatasetStoringException
from app.exceptions.dataset_validation_exception import DatasetValidationException
from app.exceptions.dataset_source_empty_exception import DatasetSourceEmptyException
from app.exceptions.dataset_invalid_filename_exception import DatasetInvalidFilenameException
from app.interfaces.dataset_broadcast_interface import DatasetBroadcastInterface
from app.interfaces.dataset_deletion_repository_interface import DatasetDeletionRepositoryInterface
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import RawDataset, DatasetMetadataWithoutId, UnitDataset, DatasetMetadata
from app.models.guid import Guid

logger = get_logger()


class DatasetSettings(Protocol):
    """
    The settings needed for the dataset service
    """

    autodelete_dataset: bool
    retain_old_datasets: bool = True


class DatasetService:
    """
    DatasetService provides a way to manage
    datasets
    """

    def __init__(
        self,
        dataset_source_repo: DatasetSourceRepositoryInterface,
        dataset_storage_repo: DatasetStorageRepositoryInterface,
        dataset_deletion_repo: DatasetDeletionRepositoryInterface,
        broadcaster: DatasetBroadcastInterface,
        settings: DatasetSettings
    ):
        self.dataset_source_repo = dataset_source_repo
        self.dataset_storage_repo = dataset_storage_repo
        self.dataset_deletion_repo = dataset_deletion_repo
        self.broadcaster = broadcaster
        self.settings = settings

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
        Create a single new dataset, this process...

        1. Looks for the oldest file in the source repository (bucket) and returns the filename
            -  If the source repository is empty, no datasets are ready to be created so the process throws DatasetSourceEmptyException (non critical)
        2. The filename is then validated e.g. must be json format
            -  If the filename is invalid, the file is deleted from the source repository and DatasetInvalidFilenameException is thrown (critical)
        3. If the filename is valid, the raw data for the file is fetched from the source repository
            -  If there is an issue with the contents of the dataset e.g. missing keys, DatasetValidationException is thrown (critical)
            -  If there is an issue retrieving the dataset from the source repository, DatasetMetadataRetrivalException is thrown (critical)
            -  If for some reason the dataset cannot be found in the source repository, DatasetNotFoundException is thrown (critical)
        4. Once we have read the raw data in, we delete the raw dataset from the source repository
            -  If an error occurs deleting this dataset, a DatasetDeletionException is thrown (critical)
        5. We then generate a guid and timestamp for this new dataset
        6. We then determine a version for this dataset by looking in the storage repository (firestore) for any current dataset with the same survey_id and period
            -  if a version already exists, we increment it
            -  if no version exists, the version is 1
        7. For each unit in the raw dataset, we create a UnitDataset object which attaches each unit with metadata for the current dataset
        8. We then extract a single list of all the identifiers for the unit data in the dataset
        9. We then store the dataset in the storage repository (firestore) with all the metadata and unit data
            -  If there is an issue storing the dataset in the storage a DatasetStoringException is thrown (critical)
        10. We then broadcast an event (pubsub) containing the metadata for the new dataset
        11. Finally, we run cleanup method which is responsible for cleaning up old dataset versions if necessary
            -  The previous version of a dataset will be deleted if all of these conditions are met...
                - retain_old_datasets is false
                - the current version of the dataset is greater than 1
                - the current version of the dataset was successfully stored

        :raises DatasetSourceEmptyException: if there are no files in the dataset source repository
        :raises DatasetInvalidFilenameException: if the filename of the dataset to be created is not valid
        :raises DatasetValidationException: if the contents of the dataset are invalid
        :raises DatasetMetadataRetrivalException: if there is an issue retrieving the latest dataset metadata from the dataset storage repository
        :raises DatasetStoringException: if there is an issue storing the new dataset in the dataset storage repository
        :raises DatasetNotFoundException: if the dataset to be created cannot be found in the dataset source repository
        :raises DatasetDeletionException: if there is an issue deleting the dataset from either the source or storage repository
        """

        logger.info(f"Starting create dataset process...")

        # Get the filename of the oldest dataset in the bucket
        dataset_filename: str | None = self.dataset_source_repo.get_oldest_filename()

        # If the source repository is empty, there are no datasets to create
        if not dataset_filename:
            logger.warning(f"No dataset found to create, skipping process")
            raise DatasetSourceEmptyException("No datasets found in the dataset source repository")

        # Validate the filename
        if not dataset_filename.endswith(".json"):
            logger.warning(f"Filename: {dataset_filename} is not valid")

            # Delete the dataset
            self._autodelete_dataset(dataset_filename)

            raise DatasetInvalidFilenameException(f"Filename: {dataset_filename} is not valid")

        logger.info(f"Fetching latest dataset from source repository: {dataset_filename}")

        try:
            # Fetch the raw data for given filename from bucket
            raw_dataset: RawDataset | None = self.dataset_source_repo.get_raw_data(dataset_filename)
        except DatasetValidationException as e:
            logger.error(f"Dataset with filename: {dataset_filename} failed validation with error: {e}")

            # Delete the dataset
            self._autodelete_dataset(dataset_filename)

            raise e

        # If the dataset could not be found
        if not raw_dataset:
            raise DatasetNotFoundException(f"{dataset_filename} could not be found in the dataset source repository")

        # Delete the dataset once we read it in
        # as creating a new dataset can take a while, this avoids creating the same one twice?

        # TODO rename file in bucket to another extension to solve this?
        self._autodelete_dataset(dataset_filename)

        # Process the new dataset
        logger.info("Creating new dataset ...")

        # Generate Guid
        dataset_id: Guid = str(uuid.uuid4())

        # Generate a current timestamp
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get the latest dataset based on survey_id and period_id
        current_dataset: DatasetMetadataWithoutId | None = self.dataset_storage_repo.get_latest_dataset_metadata(
            raw_dataset.survey_id, raw_dataset.period_id
        )

        # Determine next dataset version based on the latest dataset version
        if current_dataset:
            logger.info(
                f"Found previous dataset version: {current_dataset.sds_dataset_version} for survey {raw_dataset.survey_id}, period {raw_dataset.period_id}, incrementing version for new dataset")
            new_version = current_dataset.sds_dataset_version + 1
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

        #  TODO is this duplication needed?
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

        try:
            self.dataset_storage_repo.store_dataset(
                dataset_id=dataset_id,
                dataset_metadata=new_dataset_metadata,
                unit_data_collection_with_metadata=unit_data_collection_with_metadata,
                unit_data_identifiers=unit_data_identifiers,
            )
        except Exception as e:

            logger.error(f"Failed to save new dataset to storage repository, cleaning up: {e}")
            # If an error occurs, ensure this is fully deleted
            self.dataset_storage_repo.delete_dataset_by_guid(dataset_id)

            raise DatasetStoringException from e

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
        # we only need to attempt a cleanup if this is a new dataset version
        # i.e. current_dataset is not None
        if current_dataset:
            self._cleanup(
                survey_id=dataset_metadata.survey_id,
                period_id=dataset_metadata.period_id,
                new_version=new_dataset_metadata.sds_dataset_version,
                older_version=current_dataset.sds_dataset_version,
            )

            logger.info(f"Dataset creation process completed: {dataset_id}")

    def _cleanup(
        self,
        survey_id: str,
        period_id: str,
        new_version: int,
        older_version: int
    ):
        """
        Determine if the other versions of the dataset should be deleted

        :param survey_id: Survey ID of the new dataset
        :param period_id: Period ID of the new dataset
        :param new_version: The version of the newly created dataset
        :param older_version: The version of the latest dataset before the newly created version

        :raises DatasetDeletionException if an error occurs deleting the dataset from the storage repository
        """

        # Delete the previous version if the retention flag is false and this is not v1
        if not self.settings.retain_old_datasets and new_version > 1:

            logger.info("Deleting previous version of dataset...")

            # Delete the old version
            self.dataset_storage_repo.delete_dataset_version(
                survey_id=survey_id,
                period_id=period_id,
                version=older_version
            )

            logger.info(f"Older Dataset deleted successfully: survey: {survey_id}, period: {period_id}, version: {older_version}")
        else:
            logger.info("Nothing to cleanup")

    def delete_dataset(self):
        """
        Delete a dataset marked for deletion (only one)

        1. Fetch the GUID for a dataset that is marked for deletion (from the dataset_deletion repository) either PROCESSING OR PENDING
        2. If the GUID is None
            - Skip the process as no records have been found to delete
        3. Mark the deletion record as "PROCESSING" before we begin a delete
        4. Delete the dataset
        6. When the delete process finishes, update the deletion record to be "DELETED"

        :raises DatasetDeletionEmptyException: if there are no datasets marked for deletion in the dataset_deletion_repo (non critical)
        :raises DatasetDeletionException: if an error occurs deleting the dataset from the storage repository (critical)
        """

        logger.info("Starting delete dataset process")

        # Fetch a single dataset guid "marked for deletion" from the dataset_deletion_repo (firestore)
        dataset_guid_to_delete: Guid | None = self.dataset_deletion_repo.get_dataset_to_delete()

        if not dataset_guid_to_delete:
            logger.info("No datasets marked for deletion (skipping process)")
            raise DatasetDeletionEmptyException(
                "No datasets marked for deletion in the storage repository"
            )

        logger.info(f"Selected dataset to delete: {dataset_guid_to_delete}")

        # Mark as deletion record as processing
        self.dataset_deletion_repo.mark_record_status(
            guid=dataset_guid_to_delete,
            status=DeleteStatus.PROCESSING,
        )

        # Attempt to delete
        try:
            self.dataset_storage_repo.delete_dataset_by_guid(dataset_guid_to_delete)

        except (DatasetDeletionException, Exception) as e:

            logger.error("Error deleting dataset, updating delete record status to ERROR")

            # If an error occurred, update the status for the delete record
            self.dataset_deletion_repo.mark_record_status(
                guid=dataset_guid_to_delete,
                status=DeleteStatus.ERROR,
            )

            raise DatasetDeletionException from e

        # Mark the deletion record as deleted
        self.dataset_deletion_repo.mark_record_status(
            guid=dataset_guid_to_delete,
            status=DeleteStatus.DELETED,
        )

        logger.info(f"Dataset deleted successfully: {dataset_guid_to_delete}")
