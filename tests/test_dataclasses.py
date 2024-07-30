# pylint: disable=missing-function-docstring,redefined-outer-name
""" Tests to validate that the data classes are working as expected
"""

from pytest import fixture

from src.blueprints.common_models import Parameter, ParameterSet
from src.blueprints.custom_data_types import MTUrl
from src.mytardis_client.endpoints import URI


@fixture
def schema_namespace() -> MTUrl:
    return MTUrl("http://my.test.com/schema")


@fixture
def schema_uri() -> URI:
    return URI("/api/v1/project/1/")


@fixture
def parameter_1() -> Parameter:
    return Parameter(name="Test param 1", value="test string")


@fixture
def parameter_2() -> Parameter:
    return Parameter(name="Test param 2", value=123)


@fixture
def parameter_3() -> Parameter:
    return Parameter(name="Test param 3", value=123.4)


@fixture
def parameter_4() -> Parameter:
    return Parameter(name="Test param 4", value=True)


def test_create_parameter_set_with_uri(
    schema_uri: URI,
    parameter_1: Parameter,
    parameter_2: Parameter,
    parameter_3: Parameter,
    parameter_4: Parameter,
) -> None:
    parameterset = ParameterSet(
        schema=str(schema_uri),
        parameters=[
            parameter_1,
            parameter_2,
            parameter_3,
            parameter_4,
        ],
    )
    assert parameterset.parameter_schema == str(schema_uri)


def test_create_parameter_set_with_url(
    schema_namespace: MTUrl,
    parameter_1: Parameter,
    parameter_2: Parameter,
    parameter_3: Parameter,
    parameter_4: Parameter,
) -> None:
    parameterset = ParameterSet(
        schema=schema_namespace,
        parameters=[
            parameter_1,
            parameter_2,
            parameter_3,
            parameter_4,
        ],
    )
    assert parameterset.parameter_schema == schema_namespace
