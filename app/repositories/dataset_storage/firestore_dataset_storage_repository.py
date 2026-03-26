from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId, UnitDataset
from app.models.guid import Guid


class FirestoreDatasetStorageRepository(DatasetStorageRepositoryInterface):
    """
    This repository is an implementation of DatasetStorageRepositoryInterface
    that uses firestore
    """

    def __init__(self):
        pass

    def get_latest_dataset_metadata(
        self,
        survey_id: str,
        period_id: str
    ) -> DatasetMetadataWithoutId | None:
        pass

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

    def delete_dataset_by_guid(self, guid: Guid):
        pass
