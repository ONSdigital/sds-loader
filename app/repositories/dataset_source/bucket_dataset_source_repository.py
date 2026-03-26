
from app.factories.dataset_factories import RawDatasetFactory
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.models.dataset import RawDataset


class BucketDatasetSourceRepository(DatasetSourceRepositoryInterface):
    """
    This repository is an implementation of the DatasetSourceRepositoryInterface that
    uses GCP buckets
    """

    def __init__(self):
        pass

    def get_oldest_filename(self) -> str | None:
        pass

    def get_raw_data(self, file_name: str) -> RawDataset | None:
        pass

    def delete_raw_data(self, file_name: str) -> None:
        pass
