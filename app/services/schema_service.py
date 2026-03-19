from typing import Protocol

from sds_common.models.schema_publish_errors import SchemaPublishError

from app import get_logger

logger = get_logger()


class SchemaPublisher(Protocol):
    """
    This protocol defines the interface for a schema publisher
    which is responsible for publishing schema files.
    """

    def publish_schema(self, file_name: str):
        ...


class SchemaService:
    """
    SchemaService provides a way to publish new schema files.
    """

    def __init__(self, schema_publisher: SchemaPublisher):
        self.schema_publisher = schema_publisher

    def publish_new_schemas(self, file_list: list[str]):
        """
        Take a list of file names and publish each one using the provided SchemaPublisher.
        If any schema fails to publish, it will print an error message but continue processing the remaining files.
        """

        for file in file_list:
            try:
                self.schema_publisher.publish_schema(file)
            except SchemaPublishError as exc:
                logger.error(f"Failed to publish schema '{file}': {exc}")
