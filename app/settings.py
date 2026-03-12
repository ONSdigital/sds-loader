from sdx_base.settings.app import AppSettings, get_settings


class Settings(AppSettings):
    project_id: str


def get_instance() -> Settings:
    return get_settings(Settings)
