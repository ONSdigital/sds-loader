from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
from sdx_base.settings.app import AppSettings, get_settings

ROOT = Path(__file__).parent.parent


class QuickSettings(BaseSettings):
    """
    Quick settings are settings that are needed before
    SDX Base populates the AppSettings class.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    profile: str = "prod"

    def get_profile(self) -> str:
        return self.profile.lower()


class Settings(AppSettings):
    """
    project_id: The GCP project ID
    autodelete_dataset: Whether to automatically delete datasets from the source repo (bucket) after publishing
    retain_old_dataset: Whether to retain old versions of an updated dataset in the target repo (firestore)
    should_batch: Whether to batch write data to firestore to avoid memory limits
    dataset_bucket_name: The bucket name to pick up datasets from
    firestore_database: The Firestore database to publish datasets to
    publish_dataset_topic_id: The Pub/Sub topic ID to publish dataset updates to

    # Note this will get overridden by any duplicate entries in bash profile
    """

    project_id: str = "ons-sds-sandbox"
    autodelete_dataset: bool = True
    retain_old_dataset: bool = True
    should_batch: bool = True
    dataset_bucket_name: str
    firestore_database: str
    publish_dataset_topic_id: str


def get_instance() -> Settings:
    return get_settings(Settings)
