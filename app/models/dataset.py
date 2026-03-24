from typing import Optional

from app.models import AllowExtraBase


class RawDataset(AllowExtraBase):
    """
    Represents a raw dataset
    """
    survey_id: str
    period_id: str
    form_types: list[str]
    data: dict | list  # TODO figure out which one
    title: Optional[str] = None


class RawDatasetWithoutData(AllowExtraBase):
    survey_id: str
    period_id: str
    form_types: list[str]
    title: Optional[str]


class DatasetMetadataWithoutId(AllowExtraBase):
    survey_id: str
    period_id: str
    form_types: list[str]
    sds_published_at: str
    total_reporting_units: int
    sds_dataset_version: int
    filename: str
    title: Optional[str] = None


class DatasetMetadata(AllowExtraBase):
    dataset_id: str
    survey_id: str
    period_id: str
    form_types: list[str]
    sds_published_at: str
    total_reporting_units: int
    sds_dataset_version: int
    filename: str
    title: Optional[str] = None
