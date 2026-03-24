from typing import Optional

from app.models import AllowExtraBase


class RawDatasetDataItem(AllowExtraBase):
    """
    Represents an item in the "data" block of the raw dataset
    """
    unit_data: list | dict  # TODO figure out which one
    identifier: str


class RawDataset(AllowExtraBase):
    """
    Represents a raw dataset JSON found in the source repository
    (i.e a Bucket)
    """
    survey_id: str
    period_id: str
    form_types: list[str]
    data: list[RawDatasetDataItem]
    title: Optional[str] = None


class RawDatasetWithoutData(AllowExtraBase):
    """
    Represents a raw dataset JSON found in the source repository (i.e a Bucket)
    but without the data block
    """
    survey_id: str
    period_id: str
    form_types: list[str]
    title: Optional[str]


class UnitDataset(AllowExtraBase):
    """
    Represents a unit from the datasets "data" block
    along with metadata from the parent dataset
    """
    dataset_id: str
    survey_id: str
    period_id: str
    form_types: list[str]
    data: dict | list  # The actual data


class DatasetMetadataWithoutId(AllowExtraBase):
    """
    Represents metadata about a dataset
    without the dataset_id (i.e. Guid)
    """
    survey_id: str
    period_id: str
    form_types: list[str]
    sds_published_at: str
    total_reporting_units: int
    sds_dataset_version: int
    filename: str
    title: Optional[str] = None


class DatasetMetadata(AllowExtraBase):
    """
    Represents metadata about a dataset
    """
    dataset_id: str
    survey_id: str
    period_id: str
    form_types: list[str]
    sds_published_at: str
    total_reporting_units: int
    sds_dataset_version: int
    filename: str
    title: Optional[str] = None
