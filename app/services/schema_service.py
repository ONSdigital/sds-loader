from typing import Protocol

from sds_common.models.schema_publish_errors import SchemaPublishError


class SchemaPublisher(Protocol):
    def publish_schema(self, file_name: str):
        ...


class SchemaService:

    def __init__(self, schema_publisher: SchemaPublisher):
        self.schema_publisher = schema_publisher

    def publish_new_schemas(self, file_list: list[str]):
        """
        Take the list of new schema files, verify and publish the new schemas.
        """

        for file in file_list:
            try:
                self.schema_publisher.publish_schema(file)
            except SchemaPublishError as exc:
                print(f"Failed to publish schema {file}: {exc}")
