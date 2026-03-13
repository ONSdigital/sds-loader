from app.models.bases import AllowExtraBase


class Schema(AllowExtraBase):
    survey_id: str
    schema_version: str
