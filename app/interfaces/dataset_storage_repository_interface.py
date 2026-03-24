from abc import ABC

from app.models.dataset import DatasetMetadataWithoutId


class DatasetStorageRepositoryInterface(ABC):
    """
    This interface defines where datasets live after being
    moved from their source

    E.g DatasetStorage would be firestore
    """

    def get_latest_dataset_metadata(self, survey_id: str, period_id: str) -> DatasetMetadataWithoutId | None:
        """
        Gets the latest dataset for a given survey and period id

        :param survey_id: survey id
        :param period_id: period id

        :raises DatasetMetadataRetrivalException
        """
        ...
