# pylint: disable=missing-docstring
# nosec assert_used
import pytest

from src.mytardis_client.data_types import (
    CONTEXT_USE_URI_ID_ONLY,
    URI,
    resource_uri_to_id,
)


def test_function_resource_uri_to_id() -> None:
    assert resource_uri_to_id("/v1/user/10") == 10
    assert resource_uri_to_id("/v1/user/10/") == 10
    assert resource_uri_to_id("v1/user/10") == 10

    with pytest.raises(ValueError):
        resource_uri_to_id("/v1/user/10/extra")

    with pytest.raises(ValueError):
        resource_uri_to_id("v1/user10")


def test_uri_creation_and_validation() -> None:

    valid_uri = "/api/v1/user/10/"
    uri = URI(valid_uri)

    assert uri.root == valid_uri
    assert uri.id == 10

    with pytest.raises(ValueError):
        URI("/api/v1/user/10")

    with pytest.raises(ValueError):
        URI("/api/v1/user10")

    with pytest.raises(ValueError):
        URI("api/v1/user/10")

    with pytest.raises(ValueError):
        URI("/api/v1/user/10/extra")

    with pytest.raises(ValueError):
        URI("/api/v1/user/10/extra/")


def test_uri_serialization() -> None:

    uri_content = "/api/v1/user/10/"
    uri = URI(uri_content)

    assert uri.model_dump() == uri_content
    assert uri.model_dump(context=CONTEXT_USE_URI_ID_ONLY) == "10"
