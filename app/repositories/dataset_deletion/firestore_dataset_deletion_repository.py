from app.enums.delete_status import DeleteStatus
from app.interfaces.dataset_deletion_repository_interface import DatasetDeletionRepositoryInterface
from app.models.guid import Guid


class FirestoreDatasetDeletionRepository(DatasetDeletionRepositoryInterface):
    """
    An implementation of a DatasetDeletionRepository that stores
    delete records in firestore
    """

    def __init__(self):
        pass

    def mark_record_status(self, guid: Guid, status: DeleteStatus) -> None:
        pass

    def get_dataset_to_delete(self) -> Guid | None:
        pass
