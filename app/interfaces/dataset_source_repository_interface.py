from abc import ABC, abstractmethod

from app.models.dataset import RawDataset


class DatasetSourceRepositoryInterface(ABC):
    """
    This interface defines where datasets are originally stored
    before being moved to their storage location

    E.g DatasetSource would be a bucket
    """

    @abstractmethod
    def get_oldest_file(self) -> str | None:
        """
        Returns the filename of the oldest file in the source repository.

        :returns: The filename of the oldest file in the source repository, or none
        if there are no files in the source repository
        """
        ...

    @abstractmethod
    def get_raw_data(self, file_name: str) -> RawDataset:
        """
        Returns the raw content of a file in the source repository.

        :param file_name: The name of the file to return
        :returns: The raw content of a file in the source repository

        :raises: DatasetNotFound if the specified file_name does not exist
        :raises DatasetValidationException if the content of the specified file_name is invalid
        """
        ...

    @abstractmethod
    def delete_raw_data(self, file_name: str) -> None:
        """
        Deletes the raw content of a file in the source repository.

        :param file_name: The name of the file to delete
        """
        ...
