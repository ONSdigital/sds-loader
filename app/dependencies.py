from lagom import Singleton
from lagom.container import Container
from sds_common.publishers.github_schema_publisher import GithubSchemaPublisher
from sds_common.publishers.schema_publisher import SchemaPublisher


from app.services.schema_service import SchemaService
from app.services.schema_service import SchemaPublisher

from app.settings import Settings, get_instance, QuickSettings


class FakePublisher:
    def publish_schema(self, file_name: str):
        print(f"Published m8: {file_name}")
        pass


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
    # SchemaRepositoryInterface implementation
    # -----------------------------
    if is_prod:
        container[SchemaPublisher] = GithubSchemaPublisher
    else:
        container[SchemaPublisher] = FakePublisher

    # -----------------------------
    # Services
    # -----------------------------
    container[SchemaService] = SchemaService

    return container
