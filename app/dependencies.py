from lagom import Singleton
from lagom.container import Container

from app import get_logger
from app.profiles import PROFILES
from app.services.dataset_service import DatasetService, DatasetSettings
from app.settings import Settings, get_instance, QuickSettings

logger = get_logger()


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

    # -----------------------------
    # Core / shared dependencies
    # -----------------------------
    container[Settings] = lambda: get_instance()

    # -----------------------------
    # Apply profile
    # -----------------------------
    profile = QuickSettings().get_profile()

    try:
        profile_fn = PROFILES[profile]
    except KeyError:
        raise ValueError(f"Unknown profile '{profile}'. Available: {list(PROFILES.keys())}")
    logger.info(f"Using profile {profile}")

    # Apply profile
    profile_fn(container)

    # -----------------------------
    # Static Services
    # -----------------------------

    container[DatasetSettings] = lambda: get_instance()
    container[DatasetService] = Singleton(DatasetService)

    return container
