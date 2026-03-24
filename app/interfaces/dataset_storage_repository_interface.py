from abc import ABC


class DatasetStorageRepositoryInterface(ABC):
    """
    This interface defines where datasets live after being
    moved from their source

    E.g DatasetStorage would be firestore
    """
