import unittest
from dataclasses import dataclass

from app.models.schema import Schema
from app.services.schema_service import ExistingSchema, SchemaService


@dataclass
class MockSchemaMetadata:
    survey_id: str
    schema_version: str


class TestFindNewSchemas(unittest.TestCase):

    def test_one_new(self):

        existing: list[ExistingSchema] = [
            MockSchemaMetadata(survey_id="123", schema_version="v1"),
        ]

        schemas: list[Schema] = [
            Schema(survey_id="123", schema_version="v1"),
            Schema(survey_id="123", schema_version="v2"),
        ]

        expected: list[Schema] = [
            Schema(survey_id="123", schema_version="v2"),
        ]

        self.assertEqual(expected, SchemaService().find_new_schemas(existing, schemas))

    def test_multiple_new(self):

        existing: list[ExistingSchema] = [
            MockSchemaMetadata(survey_id="123", schema_version="v1"),
            MockSchemaMetadata(survey_id="123", schema_version="v2"),
            MockSchemaMetadata(survey_id="456", schema_version="v1"),
        ]

        schemas: list[Schema] = [
            Schema(survey_id="123", schema_version="v1"),
            Schema(survey_id="123", schema_version="v2"),
            Schema(survey_id="123", schema_version="v3"),
            Schema(survey_id="123", schema_version="v4"),
            Schema(survey_id="456", schema_version="v1"),
            Schema(survey_id="456", schema_version="v2"),
            Schema(survey_id="789", schema_version="v1"),
        ]

        expected: list[Schema] = [
            Schema(survey_id="123", schema_version="v3"),
            Schema(survey_id="123", schema_version="v4"),
            Schema(survey_id="456", schema_version="v2"),
            Schema(survey_id="789", schema_version="v1"),
        ]

        self.assertEqual(expected, SchemaService().find_new_schemas(existing, schemas))

    def test_no_new(self):

        existing: list[ExistingSchema] = [
            MockSchemaMetadata(survey_id="123", schema_version="v1"),
            MockSchemaMetadata(survey_id="456", schema_version="v1"),
        ]

        schemas: list[Schema] = [
            Schema(survey_id="123", schema_version="v1"),
            Schema(survey_id="456", schema_version="v1"),
        ]

        expected: list[Schema] = []

        self.assertEqual(expected, SchemaService().find_new_schemas(existing, schemas))

    def test_less_schemas_than_existing_does_not_raise_exception(self):

        existing: list[ExistingSchema] = [
            MockSchemaMetadata(survey_id="123", schema_version="v1"),
            MockSchemaMetadata(survey_id="456", schema_version="v1"),
        ]

        schemas: list[Schema] = [
            Schema(survey_id="123", schema_version="v1"),
        ]

        expected: list[Schema] = []

        self.assertEqual(expected, SchemaService().find_new_schemas(existing, schemas))
