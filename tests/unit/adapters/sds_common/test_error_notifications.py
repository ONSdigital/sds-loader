from unittest.mock import Mock

from sds_common.models.schema_publish_errors import SchemaDuplicationError

from app.adapters.sds_common.error_notifications import PubSubErrorNotificationAdapter


class TestSdsCommonErrorNotificationAdapter:
    def test_send_message_uses_facade_pubsub_client(self):
        facade = Mock()
        error = SchemaDuplicationError("schemas/example/v1.json")

        adapter = PubSubErrorNotificationAdapter(facade)

        adapter.send_message(error, "error-topic")

        facade.pub_sub.publish.assert_called_once_with(error.generate_message_content(), "error-topic")

