from typing import Protocol

from app.interfaces.schema_repository_interface import SchemaRepositoryInterface
from app.models.schema import Schema
from app.models.schema_metadata import SchemaMetadata


class ExistingSchema(Protocol):
    survey_id: str
    schema_version: str


class SchemaService:

    def __init__(self, schema_repository: SchemaRepositoryInterface):
        self._schema_repository = schema_repository

    def find_new_schemas(self, existing_schemas: list[ExistingSchema], schemas: list[Schema]) -> list[Schema]:
        existing_set = {(x.survey_id, x.schema_version) for x in existing_schemas}
        return [s for s in schemas if (s.survey_id, s.schema_version) not in existing_set]

    def filter_new_files(self, new_files: list[str]) -> list[str]:
        """
        Take a list of files and filter to contain only new schemas.
        """
        existing_schema: list[SchemaMetadata] = self._schema_repository.get_all_schema_metadata()

        # verify files are schemas (file name needs to match schemas/*/*
        schema_files = [f for f in new_files if f.startswith("schemas/") and f.count("/") == 2]



    def publish_new_schemas(self, file_list: list[str]):
        """
        Take the list of new schema files, verify and publish the new schemas.
        """
        pass
