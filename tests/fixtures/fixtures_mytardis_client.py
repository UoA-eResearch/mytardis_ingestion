"""Test fixtures related to the MyTardis client."""

# pylint: disable=missing-function-docstring

from typing import Any

from pytest import fixture

from src.mytardis_client.response_data import MyTardisIntrospection


@fixture
def mytardis_introspection(
    introspection_response: dict[str, Any],
) -> MyTardisIntrospection:

    object_data = introspection_response["objects"][0]

    return MyTardisIntrospection.model_validate(object_data)
