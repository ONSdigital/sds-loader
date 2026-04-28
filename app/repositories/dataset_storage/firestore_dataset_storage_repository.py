from typing import Protocol

from google.cloud import firestore

from app import get_logger
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId, UnitDataset, DatasetMetadata
from app.models.guid import Guid

logger = get_logger()


class FirestoreSettings(Protocol):
    project_id: str
    firestore_database: str
    should_batch: bool


class FirestoreDatasetStorageRepository(DatasetStorageRepositoryInterface):
    """
    This repository is an implementation of DatasetStorageRepositoryInterface
    that uses firestore
    """

    def __init__(self, settings: FirestoreSettings):
        self.settings = settings

        # Create a firestore client
        self.client = firestore.Client(
            project=self.settings.project_id,
            database=self.settings.firestore_database,
        )

        logger.info(
            f"Connected to Firestore with project_id: {settings.project_id} and database: {settings.firestore_database}"
        )

        # Max size in bytes we can upload in one batch
        self.MAX_BATCH_SIZE_BYTES = 9 * 1024 * 1024
        self.MAX_NUMBER_OF_WRITES_PER_BATCH = 500

        # Initialize Firestore collections
        self.dataset_collection = self.client.collection("datasets")

    def get_latest_dataset_metadata(self, survey_id: str, period_id: str) -> DatasetMetadataWithoutId | None:
        latest_dataset = (
            self.dataset_collection.where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .order_by("sds_dataset_version", direction=firestore.Query.DESCENDING)
            .limit(1)
            .stream()
        )

        datasets = list(latest_dataset)

        # If no results then return None
        if len(datasets) == 0:
            return None

        # Parse into Pydantic Model
        return DatasetMetadataWithoutId.model_validate(
            datasets[0].to_dict()
        )

    def _get_dataset_metadata(self, survey_id: str, period_id: str, version: int) -> DatasetMetadata | None:
        latest_dataset = (
            self.dataset_collection.where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .where("version", "==", version)
            .limit(1)
            .stream()
        )

        datasets = list(latest_dataset)

        # If no results then return None
        if len(datasets) == 0:
            return None

        return DatasetMetadata.model_validate(
            datasets[0].to_dict()
        )

    def store_dataset(
        self,
        dataset_id: Guid,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str],
    ):
        # Write to firebase in batches or not depends on the settings

        if self.settings.should_batch:
            logger.info("Writing to Firestore in BATCH mode")
            self._store_dataset_with_batching(
                dataset_id=dataset_id,
                dataset_metadata=dataset_metadata,
                unit_data_collection_with_metadata=unit_data_collection_with_metadata,
                unit_data_identifiers=unit_data_identifiers,
            )
        else:
            logger.info("Writing to Firestore in NORMAL mode")
            self._store_dataset_without_batching(
                dataset_id=dataset_id,
                dataset_metadata=dataset_metadata,
                unit_data_collection_with_metadata=unit_data_collection_with_metadata,
                unit_data_identifiers=unit_data_identifiers,
            )

    def _store_dataset_without_batching(
        self,
        dataset_id: Guid,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str],
    ):
        """
        Write the data to firestore without batching
        """
        # Create a new document for this dataset
        new_dataset_document = self.dataset_collection.document(dataset_id)

        # Store the core data first
        new_dataset_document.set(dataset_metadata.model_dump(), merge=True)

        # Create a new collection for the units
        units_collection = new_dataset_document.collection("units")

        # Go through unit data
        for unit_data, unit_identifier in zip(unit_data_collection_with_metadata, unit_data_identifiers):
            # Create and save the unit data as a new sub document
            units_collection.document(unit_identifier).set(unit_data.model_dump())

    def _store_dataset_with_batching(
        self,
        dataset_id: Guid,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str],
    ):
        """
        Use batches to write the data to firestore
        """

        # Create a new document for this dataset
        new_dataset_document = self.dataset_collection.document(dataset_id)

        # Store the core data first
        new_dataset_document.set(dataset_metadata.model_dump(), merge=True)

        # Create a new collection for the units
        units_collection = new_dataset_document.collection("units")

        # Initialise a batch
        batch = self.client.batch()
        batch_size_bytes = 0
        batch_num_records = 0

        # Go through unit data
        for unit_data, unit_identifier in zip(unit_data_collection_with_metadata, unit_data_identifiers):
            """
            Add this unit to the current batch if ...

            1. adding it does not exceed the batch size limits
            2. adding it does not exceed the batch record limits
            """

            # Work out the size of this unit
            unit_size = len(unit_data.model_dump_json().encode("utf-8"))

            # Work out if the current batch is too big already
            if (batch_size_bytes + unit_size >= self.MAX_BATCH_SIZE_BYTES) or (
                batch_num_records + 1 >= self.MAX_NUMBER_OF_WRITES_PER_BATCH
            ):
                # Commit the current batch
                batch.commit()

                # Start a new batch
                batch = self.client.batch()
                batch_size_bytes = 0
                batch_num_records = 0

            # Add the unit to the new batch
            new_unit = units_collection.document(unit_identifier)
            batch.set(new_unit, unit_data.model_dump(), merge=True)
            batch_size_bytes += unit_size
            batch_num_records += 1

        # If we never exceeded batch limit we still need to commit
        if batch_size_bytes > 0:
            batch.commit()

    def delete_dataset_version(self, survey_id: str, period_id: str, version: int):
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
