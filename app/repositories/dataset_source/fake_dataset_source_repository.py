
from app.factories.dataset_factories import RawDatasetFactory
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.models.dataset import RawDataset


class FakeDatasetSourceRepository(DatasetSourceRepositoryInterface):
    """
    This repository is a fake implementation of DatasetSourceRepositoryInterface
    and intended for running the service locally
    """

    def __init__(self):

        # Store fake datasets along with a timestamp of when they were added to the repository
        # so that we can return the oldest one
        self.datasets = {
            f"dataset_{x}.json": RawDatasetFactory.build()
            for x in range(3)
        }

    def get_oldest_filename(self) -> str | None:
        if not self.datasets:
            return None

        # Return the first item
        return next(iter(self.datasets.keys()))

    def get_raw_data(self, file_name: str) -> RawDataset | None:

        if file_name in self.datasets:
            return self.datasets[file_name]
        return None

    def delete_raw_data(self, file_name: str) -> None:
        if file_name in self.datasets:
            del self.datasets[file_name]
