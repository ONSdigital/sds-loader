from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.models.dataset import RawDataset


class DatasetService:
    """
    DatasetService provides a way to manage
    datasets
    """

    def __init__(
        self,
        dataset_source_repo: DatasetSourceRepositoryInterface,
        dataset_storage_repo: DatasetStorageRepositoryInterface,
    ):
        self.dataset_source_repo = dataset_source_repo
        self.dataset_storage_repo = dataset_storage_repo

    def create_dataset(self):
        """
        Create a new dataset (only one)
        """

        # Get the oldest filename in the bucket
        oldest_filename: str = self.dataset_source_repo.get_oldest_file()

        # Validate the filename

        # Fetch the raw data for given filename from bucket
        raw_data: RawDataset = self.dataset_source_repo.get_raw_data(oldest_filename)

        # Validate the raw data contains required fields etc

        # Process the new dataset

            # Generate Guid

            # Remove the data

            # Write to firestore

            # Publish to topic

        pass

    def delete_dataset(self):
        """
        Delete a dataset marked for deletion (only one)
        """

        # Fetch a single dataset guid from the marked_for_deletion collection in firestore

        # Get the dataset document based on this guid

        # Delete the document

        # Update the status of the record in marked_for_deletion collection to deleted
        pass
