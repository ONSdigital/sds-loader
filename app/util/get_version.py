import toml


def get_app_version() -> str:
    """
    Get the version of the application.
    """
    with open("pyproject.toml", "r") as file:
        pyproject = toml.load(file)
        try:
            version = pyproject["project"]["version"]
        except KeyError:
            version = "unknown"
    return version
