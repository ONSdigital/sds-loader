from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import DatasetMetadataWithoutId, UnitDataset


class FakeDatasetStorageRepository(DatasetStorageRepositoryInterface):
    """
    This repository is a fake implementation of DatasetStorageRepositoryInterface
    and intended for running the service locally
    """

    def __init__(self):

        # Store datasets in a dictionary
        # key is a tuple of (survey_id, period_id) and value is a 2D list of...
        # [dataset_id, dataset_metadata, unit_data_collection_with_metadata, unit_data_identifiers]
        self.datasets = {}

    def get_latest_dataset_metadata(self, survey_id: str, period_id: str) -> DatasetMetadataWithoutId | None:
        dataset = self.datasets.get((survey_id, period_id))
        if dataset is None:
            return None

        # Return the metadata with the largest sds_dataset_version
        return sorted(dataset, key=lambda x: x[1].sds_dataset_version, reverse=True)[0][1]

    def store_dataset(self, dataset_id: str, dataset_metadata: DatasetMetadataWithoutId,
                      unit_data_collection_with_metadata: list[UnitDataset], unit_data_identifiers: list[str]):
        dataset_key = (dataset_metadata.survey_id, dataset_metadata.period_id)
        self.datasets[dataset_key] = [
            dataset_id,
            dataset_metadata,
            unit_data_collection_with_metadata,
            unit_data_identifiers
        ]
