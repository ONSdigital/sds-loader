import json

from sdx_base.models.pubsub import get_data

from app import get_logger

logger = get_logger()


def get_file_path_from_bucket_notification(message) -> str:
    """
    Extract the file name from the bucket notification message
    """
    raw_data = get_data(message)
    raw_dict = json.loads(raw_data)
    logger.debug("Bucket notification message: ", raw_dict)
    return raw_dict["name"]


def get_file_paths_from_github_notification(message) -> list[str]:
    """
    Extract the file names from the github notification message
    """
    return get_data(message).split("\n")
