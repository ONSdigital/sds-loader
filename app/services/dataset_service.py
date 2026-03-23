

class DatasetService:
    """
    DatasetService provides a way to manage
    datasets
    """

    def __init__(self):
        """
        TODO repositories
        """
        pass

    def create_dataset(self):
        """
        Create a new dataset (only one)
        """

        # Get the oldest filename in the bucket

        # Fetch the raw data for given filename from bucket

        # Upload the raw data to firestore

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
