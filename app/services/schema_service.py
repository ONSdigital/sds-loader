import re
from typing import Protocol

from sds_common.models.schema_publish_errors import SchemaPublishError


class SchemaPublisher(Protocol):
    def publish_schema(self, file_name: str):
        ...


class SchemaService:

    def __init__(self, schema_publisher: SchemaPublisher):
        self.schema_publisher = schema_publisher

    def filter_files(self, new_files: list[str]) -> list[str]:
        """
        Take a list of filepaths and match to regex of schemas/*/*.json.

        :param new_files: List of filepaths.
        :return: List of filepaths matching the regex.
        """

        pattern = re.compile(r"^schemas/[^/]+/[^/]+\.json$")
        return [f for f in new_files if pattern.match(f)]

    def publish_new_schemas(self, file_list: list[str]):
        """
        Take the list of new schema files, verify and publish the new schemas.
        """

        for file in file_list:
            try:
                self.schema_publisher.publish_schema(file)
            except SchemaPublishError as exc:
                print(f"Failed to publish schema {file}: {exc}")
