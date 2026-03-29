from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict
from sdx_base.settings.app import AppSettings, get_settings

ROOT = Path(__file__).parent.parent


class QuickSettings(BaseSettings):
    """
    Quick settings are settings that are needed before
    SDX Base populates the AppSettings class.
    """
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    env: str = 'production'

    def is_production(self) -> bool:
        return self.env.lower() in ('production', 'prod')


class Settings(AppSettings):
    project_id: str
    autodelete_dataset: bool
    dataset_bucket_name: str
    firestore_database: str


def get_instance() -> Settings:
    return get_settings(Settings)
