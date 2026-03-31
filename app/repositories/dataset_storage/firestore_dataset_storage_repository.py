from typing import Protocol

from google.cloud import firestore

from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId, UnitDataset
from app.models.guid import Guid


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

        # Async client
        self.async_client = firestore.AsyncClient(
            project=self.settings.project_id,
            database=self.settings.firestore_database
        )

        # Initialize Firestore collections
        self.dataset_collection = self.client.collection("datasets")

        # Async collection
        self.async_dataset_collection = self.async_client.collection("datasets")


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

    def store_dataset(
        self,
        dataset_id: Guid,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str]
    ):
        pass

    def delete_dataset_version(
        self,
        survey_id: str,
        period_id: str,
        version: int
    ):
        pass

    async def delete_dataset_by_guid(self, guid: Guid):

        collections = self.async_dataset_collection.document(guid).collections()

        # Delete sub collections first
        async for sub_collection in collections:
            await sub_collection.recursive_delete()

        # Delete the document itself
        await self.async_dataset_collection.document(guid).delete()
