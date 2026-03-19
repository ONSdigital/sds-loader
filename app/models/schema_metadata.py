from app.models.bases import AllowExtraBase

# TODO used?
class SchemaMetadata(AllowExtraBase):
    guid: str
    survey_id: str
    schema_location: str
    sds_schema_version: int
    sds_published_at: str
    schema_version: str
    title: str
