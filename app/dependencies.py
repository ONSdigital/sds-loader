from lagom import Singleton
from lagom.container import Container

from app.interfaces.schema_repository_interface import SchemaRepositoryInterface
from app.repositories.sds_schema_repository import SdsSchemaRepository, SdsSchemaRequestProtocol
from app.services.schema_service import SchemaService
from app.settings import Settings, get_instance, QuickSettings


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

        # TODO add sds common as SdsSchemaRequestProtocol here
        container[SchemaRepositoryInterface] = Singleton(SdsSchemaRepository)
    else:
        container[SchemaRepositoryInterface] = Singleton(SdsSchemaRepository)

    # -----------------------------
    # Services
    # -----------------------------
    container[SchemaService] = SchemaService

    return container
