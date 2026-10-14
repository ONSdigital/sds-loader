from sds_common import SdsCommon
from sds_common.models.schema_publish_errors import SchemaPublishError

from app.services.schema_service import ErrorNotificationProtocol


class PubSubErrorNotificationAdapter(ErrorNotificationProtocol):
    """
    Adapter that routes schema publish errors through the `sds_common` facade pub/sub client.
    """

    def __init__(self, common: SdsCommon):
        self._common = common

    def send_message(self, error: SchemaPublishError, topic_id: str):
        self._common.pub_sub.publish(error.generate_message_content(), topic_id)

