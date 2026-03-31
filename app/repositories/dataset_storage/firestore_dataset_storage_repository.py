from typing import Protocol

from google.cloud import firestore

from app import get_logger
from app.exceptions.dataset_deletion_exception import DatasetDeletionException
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId, UnitDataset, DatasetMetadata
from app.models.guid import Guid

logger = get_logger()


class FirestoreSettings(Protocol):
    project_id: str
    firestore_database: str


class FirestoreDatasetStorageRepository(DatasetStorageRepositoryInterface):
    """
    This repository is an implementation of DatasetStorageRepositoryInterface
    that uses firestore
    """

    def __init__(
        self,
        settings: FirestoreSettings
    ):
        self.settings = settings

        # Create a firestore client
        self.client = firestore.Client(
            project=self.settings.project_id,
            database=self.settings.firestore_database
        )

        self.MAX_BATCH_SIZE_BYTES = 9 * 1024 * 1024

        # Initialize Firestore collections
        self.dataset_collection = self.client.collection("datasets")

    def get_latest_dataset_metadata(
        self,
        survey_id: str,
        period_id: str
    ) -> DatasetMetadataWithoutId | None:

        latest_dataset = (
            self.dataset_collection
            .where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .order_by("sds_dataset_version", direction=firestore.Query.DESCENDING)
            .limit(1)
            .stream()
        )

        datasets = list(latest_dataset)

        # If no results then return None
        if len(datasets) == 0:
            return None

        return DatasetMetadataWithoutId.model_validate(datasets[0])

    def _get_dataset_metadata(
        self,
        survey_id: str,
        period_id: str,
        version: int
    ) -> DatasetMetadata | None:

        latest_dataset = (
            self.dataset_collection
            .where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .where("version", "==", version)
            .limit(1)
            .stream()
        )

        datasets = list(latest_dataset)

        # If no results then return None
        if len(datasets) == 0:
            return None

        return DatasetMetadata.model_validate(datasets[0])

    def store_dataset(
        self,
        dataset_id: Guid,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str]
    ):

        # Create a new document for this dataset
        new_dataset_document = self.dataset_collection.document(dataset_id)

        # Create a new collection for the units
        units_collection = new_dataset_document.collection("units")

        # Go through unit data
        for (unit_data, unit_identifier) in zip(unit_data_collection_with_metadata, unit_data_identifiers):

            # Create and save the unit data as a new sub document
            units_collection.document(unit_identifier).set(unit_data.model_dump())

    def delete_dataset_version(
        self,
        survey_id: str,
        period_id: str,
        version: int
    ):

        dataset_metadata = self._get_dataset_metadata(survey_id, period_id, version)

        if not dataset_metadata:
            logger.warning("Attempting delete dataset previous version, but it could not be found.")
            return

        # Delete the dataset now we have a guid for it
        self.delete_dataset_by_guid(dataset_metadata.dataset_id)

    def delete_dataset_by_guid(self, guid: Guid):

        # Get all the collections for this dataset
        collections = self.dataset_collection.document(guid).collections()

        # Delete each collection
        for collection in collections:
            collection.recursive_delete()

        # Delete the dataset itself
        self.dataset_collection.document(guid).delete()
