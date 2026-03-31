from app.interfaces.dataset_broadcast_interface import DatasetBroadcastInterface
from app.models.dataset import DatasetMetadata


class PubsubBroadcaster(DatasetBroadcastInterface):
    """
    A broadcaster that will
    broadcast to pubsub
    """
    def __init__(self):
        self.broadcasted = []

    def broadcast(self, dataset_metadata: DatasetMetadata) -> None:

        try:
            publisher_service.publish_data_to_topic(
                dataset_publish_response,
                config.PUBLISH_DATASET_TOPIC_ID,
            )
            logger.debug(
                f"Dataset response {dataset_publish_response} published to topic {config.PUBLISH_DATASET_TOPIC_ID}"
            )
            logger.info("Dataset response published successfully.")
        except Exception as exc:
            logger.debug(
                f"Dataset response {dataset_publish_response} failed to publish to topic {config.PUBLISH_DATASET_TOPIC_ID} "
                f"with error {exc}"
            )
            raise RuntimeError("Error publishing dataset response to the topic.") from exc
