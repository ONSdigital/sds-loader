from app.enums.delete_status import DeleteStatus
from app.interfaces.dataset_deletion_repository_interface import DatasetDeletionRepositoryInterface
from app.models.guid import Guid


class FakeDatasetDeletionRepository(DatasetDeletionRepositoryInterface):
    """
    A fake dataset deletion repository
    """

    def __init__(self):

        # Key = guid, value = status
        self.delete_records = {}

    def mark_record_status(self, guid: Guid, status: DeleteStatus) -> None:
        if guid in self.delete_records:
            self.delete_records[guid] = status

    def get_dataset_to_delete(self) -> Guid | None:
        # Get the first item in the dict
        for guid, status in self.delete_records.items():
            if status == DeleteStatus.PROCESSING:
                return guid
        for guid, status in self.delete_records.items():
            if status == DeleteStatus.PENDING:
                return guid
        return None
