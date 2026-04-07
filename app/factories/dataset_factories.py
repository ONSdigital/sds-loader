from polyfactory.factories.pydantic_factory import ModelFactory

from app.factories import print_factory
from app.models.dataset import RawDataset, DatasetMetadataWithoutId


class RawDatasetFactory(ModelFactory[RawDataset]):
    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True


class DatasetMetadataWithoutIdFactory(ModelFactory[DatasetMetadataWithoutId]):
    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True


if __name__ == "__main__":
    """
    Example usage of the factories to print sample data.
    """

    print_factory(RawDatasetFactory)
