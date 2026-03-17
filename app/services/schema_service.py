from typing import Protocol

from app.interfaces.schema_repository_interface import SchemaRepositoryInterface
from app.models.schema import Schema


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

        # Files need to be in /schemas

        # Be of the format v{x}.json

        pass

    def publish_new_schemas(self, file_list: list[str]):
        """
        Take the list of new schema files, verify and publish the new schemas.
        """

        # Fetch the metadata for the given files

        # TODO 

        pass
