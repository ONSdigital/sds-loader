from typing import Callable

from sds_common.models.schema_publish_errors import SchemaDuplicationError

from app.services.schema_service import SchemaService


class MockPublisher:
    """
    Mock Publisher class
    """
    def __init__(self, side_effects: dict[str, Callable]):
        self.side_effects = side_effects
        self.published_schemas = []

    def publish_schema(self, file_name: str):
        if file_name in self.side_effects:
            try:
                self.side_effects[file_name]()
            except:
                return

        self.published_schemas.append(file_name)


def raise_schema_error():
    """
    Raise SchemaDuplicationError. which inherits from SchemaPublishError.
    """
    raise SchemaDuplicationError("error")


def test_publish_new_schemas_publishes_after_exception():
    publisher = MockPublisher(
        side_effects={
            "schemas/abc/v2.json": raise_schema_error,
        }
    )

    # Define input filenames
    filenames = [
        "schemas/abc/v1.json",
        "schemas/abc/v2.json",  # This one will raise a schema exception
        "schemas/abc/v3.json",
        "schemas/abc/v4.json",
    ]

    # Create a SchemaService
    service = SchemaService(publisher)

    # Call the publish_new_schemas method with the test filenames
    service.publish_new_schemas(filenames)

    should_be_published = [
        "schemas/abc/v1.json",
        "schemas/abc/v3.json",
        "schemas/abc/v4.json",
    ]

    # Assert length of published is correct
    assert len(publisher.published_schemas) == len(should_be_published)

    # Check each file has been published
    for filename in should_be_published:
        assert filename in publisher.published_schemas
