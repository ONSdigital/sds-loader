from typing import Protocol
from sds_common.schema.schema import Schema as CommonSchema

import requests

from app.interfaces.schema_repository_interface import SchemaRepositoryInterface
from app.models.schema import Schema
from app.models.schema_metadata import SchemaMetadata


class SdsSchemaRequestProtocol(Protocol):
    def get_all_schema_metadata(self) -> requests.Response:
        ...

    def post_schema(self, schema: CommonSchema) -> requests.Response:
        ...


class SchemaRepository(SchemaRepositoryInterface):
    def __init__(self, schema_repository: SdsSchemaRequestProtocol):
        self.schema_repository = schema_repository

    def post_schema(self, schema: Schema):
        pass

    def get_all_schema_metadata(self) -> list[SchemaMetadata]:
        pass

