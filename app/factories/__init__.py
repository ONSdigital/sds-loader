from typing import Type, Any

from polyfactory.factories.pydantic_factory import ModelFactory


def print_factory(factory: Type[ModelFactory[Any]]):
    """
    Useful helper method to print JSON representation of the model
    """
    print(factory.build().model_dump_json(indent=4))
