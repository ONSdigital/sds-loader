from lagom.container import Container
from sds_common.publishers.gcs_schema_publisher import GcsSchemaPublisher
from sds_common.publishers.github_schema_publisher import GithubSchemaPublisher

from app.services.schema_service import SchemaService

from app.settings import Settings, get_instance, QuickSettings


class FakePublisher:
    def __init__(self, name: str):
        self._name = name

    def publish_schema(self, file_name: str):
        print(f"Published: {file_name} to {self._name}")


def build_container() -> Container:
    """
    Build the dependency injection container for the application.

    Dependencies are automatically built from type hints, where an interface
    is used, the appropriate implementation is selected based on the environment.
    """

    # Create the DI container
    container = Container()

    # Determine environment
    is_prod = QuickSettings().is_production()

    # -----------------------------
    # Core / shared dependencies
    # -----------------------------
    container[Settings] = lambda: get_instance()

    # -----------------------------
    # Publishers
    # -----------------------------

    # -----------------------------
    # Services
    # -----------------------------

    if is_prod:
        container[SchemaService] = SchemaService(
            bucket_publisher=GcsSchemaPublisher,
            repository_publisher=GithubSchemaPublisher,
        )
    else:
        container[SchemaService] = SchemaService(
            bucket_publisher=FakePublisher(name="Fake bucket publisher"),
            repository_publisher=FakePublisher(name="Fake github publisher"),
        )

    return container
