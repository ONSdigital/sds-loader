from abc import ABC
from typing import Any

from app.models.schema_metadata import SchemaMetadata


class SchemaRepositoryInterface(ABC):

    def post_schema(self, schema: Any):
        pass

    def get_all_schema_metadata(self) -> list[SchemaMetadata]:
        pass
