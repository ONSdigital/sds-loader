from abc import ABC, abstractmethod

from app.models.dataset import DatasetMetadata


class DatasetBroadcastInterface(ABC):
    """
    Interface for broadcasting datasets
    all classes must implement a broadcast method to broadcast the metadata of the given dataset
    """

    @abstractmethod
    def broadcast(self, dataset_metadata: DatasetMetadata) -> None:
        """
        Will broadcast the metadata of the given dataset
        """
        raise NotImplementedError
