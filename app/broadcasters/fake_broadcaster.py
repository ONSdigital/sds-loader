from app.interfaces.dataset_broadcast_interface import DatasetBroadcastInterface
from app.models.dataset import DatasetMetadata


class FakeBroadcaster(DatasetBroadcastInterface):
    """
    A fake broadcaster that doesn't actually broadcast the data
    """

    def __init__(self):
        self.broadcasted = []

    def broadcast(self, dataset_metadata: DatasetMetadata) -> None:
        self.broadcasted.append(dataset_metadata)
