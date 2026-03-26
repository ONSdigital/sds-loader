from abc import ABC, abstractmethod

from app.models.guid import Guid


class DatasetDeletionRepositoryInterface(ABC):
    """
    This interface defines how a repository
    for storing which datasets should be deleted looks

    E.g. DatasetDeletionRepositoryInterface would be firestore collection "marked_for_deletion"
    """

    @abstractmethod
    def get_dataset_to_delete(self) -> Guid | None:
        """
        Get the next Guid for a dataset to be deleted

        :return The guid of the dataset to delete, or none if no datasets are marked for deletion
        """
        ...
