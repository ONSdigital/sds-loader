from sds_common import SdsCommon

from app.services.schema_service import PublisherProtocol


class GithubSchemaPublisherAdapter(PublisherProtocol):
    """
    Adapter that routes GitHub schema publishing through the `sds_common` facade.
    """

    def __init__(self, common: SdsCommon):
        self._common = common

    def publish_schema(self, file_name: str):
        return self._common.github_publisher.publish(file_name)


class GcsSchemaPublisherAdapter(PublisherProtocol):
    """
    Adapter that routes GCS schema publishing through the `sds_common` facade.
    """

    def __init__(self, common: SdsCommon):
        self._common = common

    def publish_schema(self, file_name: str):
        return self._common.gcs_publisher.publish(file_name)

