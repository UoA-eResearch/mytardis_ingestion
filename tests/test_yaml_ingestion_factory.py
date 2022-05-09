import mock
import responses
from pytest import fixture

from src.smelters import YAMLSmelter
from src.specific_implementations import YAMLIngestionFactory

from .conftest import config_dict, processed_introspection_response


@fixture
@mock.patch("src.overseers.overseer.Overseer.get_mytardis_set_up")
def yaml_factory(mock_get_setup, config_dict, processed_introspection_response):
    mock_get_setup.return_value = processed_introspection_response
    return YAMLIngestionFactory(config_dict)


def test_get_smelter(yaml_factory, config_dict, introspection_response_dict):
    test_dict = config_dict
    assert isinstance(yaml_factory.get_smelter(test_dict), YAMLSmelter)
