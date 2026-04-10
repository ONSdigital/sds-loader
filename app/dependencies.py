from lagom import Singleton, dependency_definition
from lagom.container import Container
from sds_common.publishers.gcs_schema_publisher import GcsSchemaPublisher
from sds_common.publishers.github_schema_publisher import GithubSchemaPublisher
from sdx_base.services.storage import StorageService

from app import get_logger
from app.broadcasters.fake_broadcaster import FakeBroadcaster
from app.broadcasters.pubsub_broadcaster import PubsubBroadcaster
from app.interfaces.dataset_broadcast_interface import DatasetBroadcastInterface
from app.interfaces.dataset_deletion_repository_interface import DatasetDeletionRepositoryInterface
from app.interfaces.dataset_source_repository_interface import DatasetSourceRepositoryInterface
from app.interfaces.dataset_storage_repository_interface import DatasetStorageRepositoryInterface
from app.profiles import PROFILES
from app.repositories.dataset_deletion.fake_dataset_deletion_repository import FakeDatasetDeletionRepository
from app.repositories.dataset_deletion.firestore_dataset_deletion_repository import FirestoreDatasetDeletionRepository
from app.repositories.dataset_source.bucket_dataset_source_repository import BucketDatasetSourceRepository
from app.repositories.dataset_source.fake_dataset_source_repository import FakeDatasetSourceRepository
from app.repositories.dataset_storage.fake_dataset_storage_repository import FakeDatasetStorageRepository
from app.repositories.dataset_storage.firestore_dataset_storage_repository import FirestoreDatasetStorageRepository
from app.services.dataset_service import DatasetService, DatasetSettings
from app.services.schema_service import SchemaService

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
        raise ValueError(
            f"Unknown profile '{profile}'. "
            f"Available: {list(PROFILES.keys())}"
        )
    logger.info(f"Using profile {profile}")

    # Apply profile
    profile_fn(container)

    # -----------------------------
    # Static Services
    # -----------------------------

    container[DatasetSettings] = lambda: get_instance()
    container[DatasetService] = Singleton(DatasetService)

    return container
