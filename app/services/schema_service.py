from typing import Protocol

from app.models.schema import Schema


class ExistingSchema(Protocol):
    survey_id: str
    schema_version: str


class SchemaService:

    def find_new_schemas(self, existing_schemas: list[ExistingSchema], schemas: list[Schema]) -> list[Schema]:
        existing_set = {(x.survey_id, x.schema_version) for x in existing_schemas}
        return [s for s in schemas if (s.survey_id, s.schema_version) not in existing_set]
