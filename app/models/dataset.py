from typing import Optional

from app.models import AllowExtraBase


class RawDataset(AllowExtraBase):
    """
    Represents a raw dataset
    """
    survey_id: str
    period_id: str
    form_types: list[str]
    data: object
    title: Optional[str] = None
