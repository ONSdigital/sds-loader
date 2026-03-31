from abc import ABC, abstractmethod

from app.models.dataset import DatasetMetadataWithoutId, UnitDataset
from app.models.guid import Guid


class DatasetStorageRepositoryInterface(ABC):
    """
    This interface defines where datasets live after being
    moved from their source

    E.g. DatasetStorage would be firestore
    """

    @abstractmethod
    def get_latest_dataset_metadata(
        self,
        survey_id: str,
        period_id: str
    ) -> DatasetMetadataWithoutId | None:
        """
        Gets the latest dataset for a given survey and period id

        :param survey_id: survey id
        :param period_id: period id

        :raises DatasetMetadataRetrivalException
        """
        ...

    @abstractmethod
    def store_dataset(
        self,
        dataset_id: Guid,
        dataset_metadata: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        unit_data_identifiers: list[str],
    ):
        """
        Save the dataset to storage

        :param dataset_id: The unique id of the dataset (guid)
        :param dataset_metadata: The metadata of the dataset
        :param unit_data_collection_with_metadata: A list of the units in the dataset associated with this datasets metadata
        :param unit_data_identifiers: A list of the identifiers for the unit data in the dataset

        :raises DatasetStoringException: if there is an issue storing the dataset
        """
        ...

    @abstractmethod
    def delete_dataset_version(
        self,
        survey_id: str,
        period_id: str,
        version: int
    ):
        """
        Delete a specific version of the dataset from the repository

        :param survey_id: survey id of the dataset to delete
        :param period_id: period id of the dataset to delete
        :param version: version of the dataset to delete

        :raises DatasetDeletionException: if there is an issue removing the dataset
        """
        ...

    @abstractmethod
    def delete_dataset_by_guid(
        self,
        guid: Guid,
    ):
        """
        Delete a specific dataset from the repository

        :param guid: guid of the dataset to delete

        :raises DatasetDeletionException: if there is an issue removing the dataset
        """
        ...
