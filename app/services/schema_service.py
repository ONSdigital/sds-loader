from typing import Protocol

from app.models.schema import Schema


class ExistingSchema(Protocol):
    survey_id: str
    schema_version: str


class SchemaService:

    def find_new_schemas(self, existing_schemas: list[ExistingSchema], schemas: list[Schema]) -> list[Schema]:
        new_schemas: list[Schema] = []
        for schema in schemas:
            is_new = True
            for existing in existing_schemas:
                if schema.survey_id == existing.survey_id:
                   if schema.schema_version == existing.schema_version:
                       is_new = False

            if is_new:
                new_schemas.append(schema)

        return new_schemas
