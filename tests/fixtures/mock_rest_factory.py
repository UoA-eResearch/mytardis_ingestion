# pylint: disable=missing-function-docstring,missing-module-docstring

import responses
from pytest import fixture

from src.helpers import MyTardisRESTFactory
from src.helpers.config import AuthConfig, ConnectionConfig


@fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        rsps.get("https://example.com", status=200)
        yield rsps


@fixture
def mock_mt_rest(
    auth: AuthConfig,
    connection: ConnectionConfig,
    mocked_responses: responses.RequestsMock,
):
    return MockMtRest(auth=auth, connection=connection).setup(mocked_responses)


class MockMtRest(MyTardisRESTFactory):
    """A mock for the MyTardisRESTFactory class which uses the responses package to mock
    responses from the MyTardis API."""

    response_mock: responses.RequestsMock

    def setup(self, mock: responses.RequestsMock):
        """MUST BE CALLED. Sets up the response mock for the MyTardisRESTFactory class"""
        self.response_mock = mock
        self.response_mock.start()

        return self

    def __del__(self):
        self.response_mock.stop()
        self.response_mock.reset()
