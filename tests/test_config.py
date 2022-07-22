import logging
from urllib.parse import urljoin
import mock
import pytest
import responses
from requests.exceptions import HTTPError

from src.helpers.config import ConfigFromEnv

logger = logging.getLogger(__name__)
logger.propagate = True

# pylint: disable=missing-function-docstring


@responses.activate
def test_get_mytardis_setup(
    mytardis_settings_no_introspection: ConfigFromEnv,
    introspection_response_dict,
    mytardis_setup,
):
    assert mytardis_settings_no_introspection._mytardis_setup is None

    responses.add(
        responses.GET,
        urljoin(
            mytardis_settings_no_introspection.connection.api_template,
            "introspection",
        ),
        json=(introspection_response_dict),
        status=200,
    )

    assert mytardis_settings_no_introspection.mytardis_setup == mytardis_setup


@responses.activate
def test_get_mytardis_setup_http_error(
    caplog, mytardis_settings_no_introspection: ConfigFromEnv
):
    responses.add(
        responses.GET,
        urljoin(
            mytardis_settings_no_introspection.connection.api_template, "introspection"
        ),
        status=504,
    )

    caplog.set_level(logging.ERROR)
    error_str = "Introspection returned error:"
    with pytest.raises(HTTPError):
        _ = mytardis_settings_no_introspection.get_mytardis_setup()
        assert error_str in caplog.text


@mock.patch("src.helpers.config.ConfigFromEnv.get_mytardis_setup")
def test_get_mytardis_setup_general_error(
    mock_get_mytardis_setup,
    caplog,
    mytardis_settings_no_introspection: ConfigFromEnv,
):
    mock_get_mytardis_setup.side_effect = IOError()
    error_str = "Non-HTTP exception in ConfigFromEnv.get_mytardis_setup"
    with pytest.raises(IOError):
        _ = mytardis_settings_no_introspection.get_mytardis_setup()
        assert error_str in caplog.text


@responses.activate
def test_get_mytardis_setup_no_objects(
    caplog,
    mytardis_settings_no_introspection: ConfigFromEnv,
    response_dict_not_found,
):

    responses.add(
        responses.GET,
        urljoin(
            mytardis_settings_no_introspection.connection.api_template,
            "introspection",
        ),
        json=(response_dict_not_found),
        status=200,
    )
    caplog.set_level(logging.ERROR)
    error_str = "MyTardis introspection did not return any data when called from ConfigFromEnv.get_mytardis_setup"
    with pytest.raises(ValueError, match=error_str):
        _ = mytardis_settings_no_introspection.get_mytardis_setup()
        assert error_str in caplog.text


@responses.activate
def test_get_mytardis_setup_too_many_objects(
    caplog,
    mytardis_settings_no_introspection: ConfigFromEnv,
    introspection_response_dict,
):
    test_dict = introspection_response_dict
    test_dict["objects"].append("Some Fake Data")
    responses.add(
        responses.GET,
        urljoin(
            mytardis_settings_no_introspection.connection.api_template,
            "introspection",
        ),
        json=(test_dict),
        status=200,
    )
    caplog.set_level(logging.ERROR)
    log_error_str = f"""MyTardis introspection returned more than one object when called from
        ConfigFromEnv.get_mytardis_setup\n
        Returned response was: {test_dict}"""
    error_str = "MyTardis introspection returned more than one object when called from ConfigFromEnv.get_mytardis_setup"

    with pytest.raises(ValueError, match=error_str):
        _ = mytardis_settings_no_introspection.get_mytardis_setup()
        assert log_error_str in caplog.text
