from abc import ABC

from app.models.schema import Schema
from app.models.schema_metadata import SchemaMetadata


class SchemaRepositoryInterface(ABC):

    def post_schema(self, schema: Schema):
        pass

    def get_all_schema_metadata(self) -> list[SchemaMetadata]:
        pass
