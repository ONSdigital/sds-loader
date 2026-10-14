from unittest.mock import Mock

from app.adapters.sds_common.schema_publishers import GcsSchemaPublisherAdapter, GithubSchemaPublisherAdapter


class TestSdsCommonSchemaPublisherAdapters:
    def test_github_adapter_calls_facade_publisher(self):
        facade = Mock()
        facade.github_publisher.publish.return_value = "published"

        adapter = GithubSchemaPublisherAdapter(facade)

        result = adapter.publish_schema("schemas/example/v1.json")

        assert result == "published"
        facade.github_publisher.publish.assert_called_once_with("schemas/example/v1.json")

    def test_gcs_adapter_calls_facade_publisher(self):
        facade = Mock()
        facade.gcs_publisher.publish.return_value = "published"

        adapter = GcsSchemaPublisherAdapter(facade)

        result = adapter.publish_schema("schemas/example/v2.json")

        assert result == "published"
        facade.gcs_publisher.publish.assert_called_once_with("schemas/example/v2.json")

