# Old style - at some point in the future could be refactored to use the fixtures
# pylint: disable=missing-function-docstring,missing-module-docstring
# nosec assert_used
# flake8: noqa S101

import logging
from typing import Any, Dict

import mock
import pytest
import responses
from pytest import LogCaptureFixture

from src.blueprints.project import Project
from src.forges.forge import Forge, ForgeError
from src.mytardis_client.endpoints import URI, MyTardisEndpoint
from src.mytardis_client.mt_rest import MyTardisRESTFactory

logger = logging.getLogger(__name__)
logger.propagate = True


@responses.activate
def test_post_returns_URI_on_success(  # pylint: disable=invalid-name
    project_uri: URI,
    rest_factory: MyTardisRESTFactory,
    forge: Forge,
    project: Project,
    project_creation_response_dict: Dict[str, Any],
) -> None:

    endpoint: MyTardisEndpoint = "/project"

    responses.add(
        responses.POST,
        rest_factory.compose_url(endpoint) + "/",
        status=200,
        json=(project_creation_response_dict),
    )

    test_value = forge.forge_object(endpoint, project)

    assert test_value == project_uri


@responses.activate
def test_post_returns_none_on_missing_body(
    caplog: LogCaptureFixture,
    rest_factory: MyTardisRESTFactory,
    forge: Forge,
    project: Project,
) -> None:

    endpoint: MyTardisEndpoint = "/project"

    responses.add(
        responses.POST,
        rest_factory.compose_url(endpoint) + "/",
        status=201,
    )

    caplog.set_level(logging.INFO)

    with pytest.raises(ForgeError) as exc_info:
        _ = forge.forge_object(endpoint, project)

    error_message = "Expected a JSON return from the POST request"

    assert error_message in exc_info.value.args[0]

    assert any(
        level == logging.WARNING and error_message in message
        for _, level, message in caplog.record_tuples
    )


@responses.activate
def test_HTTPError_logs_warning(  # pylint: disable=invalid-name
    caplog: LogCaptureFixture,
    rest_factory: MyTardisRESTFactory,
    project: Project,
) -> None:

    endpoint: MyTardisEndpoint = "/project"

    responses.add(
        responses.POST,
        rest_factory.compose_url(endpoint),
        status=504,
    )
    caplog.set_level(logging.WARNING)

    forge = Forge(rest_factory)

    with pytest.raises(ForgeError):
        _ = forge.forge_object(endpoint, project)

    assert any(
        "forge" in name and level == logging.ERROR
        for name, level, _ in caplog.record_tuples
    )


@responses.activate
def test_HTTPError_fully_logs_error_at_error(  # pylint: disable=invalid-name
    caplog: LogCaptureFixture,
    rest_factory: MyTardisRESTFactory,
    project: Project,
) -> None:

    endpoint: MyTardisEndpoint = "/project"
    url = rest_factory.compose_url(endpoint) + "/"

    responses.add(
        responses.POST,
        url,
        status=504,
    )

    forge = Forge(rest_factory)

    caplog.set_level(logging.ERROR)

    with pytest.raises(ForgeError):
        _ = forge.forge_object(endpoint, project)

    info_str = "504 Server Error: Gateway Timeout for url: " f"{url}"
    assert any(
        "forge" in name and level == logging.ERROR and info_str in message
        for name, level, message in caplog.record_tuples
    )


def test_non_HTTPError_logs_error(  # pylint: disable=invalid-name
    caplog: LogCaptureFixture,
    forge: Forge,
    project: Project,
) -> None:
    caplog.set_level(logging.ERROR)

    with mock.patch(
        "src.mytardis_client.mt_rest.MyTardisRESTFactory.request"
    ) as mock_request_func:
        mock_request_func.side_effect = ValueError()
        with pytest.raises(ForgeError):
            _ = forge.forge_object("/project", project)

    assert any(
        "forge" in name and level == logging.ERROR
        for name, level, _ in caplog.record_tuples
    )


@pytest.mark.parametrize("status_code", [300, 301, 302])
@responses.activate
def test_response_status_larger_than_300_logs_error(
    caplog: Any,
    rest_factory: MyTardisRESTFactory,
    status_code: int,
    forge: Forge,
    project: Project,
) -> None:

    endpoint: MyTardisEndpoint = "/project"

    responses.add(
        responses.POST,
        rest_factory.compose_url(endpoint) + "/",
        status=status_code,
    )
    caplog.set_level(logging.WARNING)

    with pytest.raises(ForgeError):
        _ = forge.forge_object(endpoint, project)

    assert any(
        "forge" in name and level == logging.WARNING
        for name, level, _ in caplog.record_tuples
    )


@responses.activate
def test_no_uri_returns_warning(
    caplog: LogCaptureFixture,
    rest_factory: MyTardisRESTFactory,
    forge: Forge,
    project: Project,
    project_creation_response_dict: Dict[str, Any],
) -> None:
    test_response_dict_without_uri = project_creation_response_dict
    test_response_dict_without_uri.pop("resource_uri")

    endpoint: MyTardisEndpoint = "/project"

    responses.add(
        responses.POST,
        rest_factory.compose_url(endpoint) + "/",
        status=200,
        json=test_response_dict_without_uri,
    )
    caplog.set_level(logging.WARNING)

    with pytest.raises(ForgeError):
        _ = forge.forge_object("/project", project)

    assert any(
        "forge" in name and level == logging.WARNING
        for name, level, _ in caplog.record_tuples
    )
