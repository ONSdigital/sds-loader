from enum import Enum


class DeleteStatus(Enum):
    """
    This enum defines the different
    states a deletion record can have
    """

    PROCESSING = "processing"
    PENDING = "pending"
    ERROR = "error"
    DELETED = "deleted"
