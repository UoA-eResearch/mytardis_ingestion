# pylint: disable=missing-docstring
# nosec assert_used
import pytest

from src.mytardis_client.data_types import URI, resource_uri_to_id
from src.mytardis_client.objects import list_mytardis_objects


def test_function_resource_uri_to_id() -> None:
    assert resource_uri_to_id("/v1/user/10") == 10
    assert resource_uri_to_id("/v1/user/10/") == 10
    assert resource_uri_to_id("v1/user/10") == 10

    with pytest.raises(ValueError):
        resource_uri_to_id("/v1/user/10/extra")

    with pytest.raises(ValueError):
        resource_uri_to_id("v1/user10")


VALID_URIS = [
    (f"/api/v1/{obj}/{id}/", id) for id, obj in enumerate(list_mytardis_objects())
]


def gen_valid_uri_ids() -> list[str]:
    return [uri_data[0] for uri_data in VALID_URIS]


@pytest.mark.parametrize("uri_data,uri_id", VALID_URIS, ids=gen_valid_uri_ids())
def test_good_uris(uri_data: str, uri_id: int) -> None:
    uri = URI(uri_data)

    assert str(uri) == uri_data
    assert uri.id == uri_id


@pytest.mark.parametrize(
    "uri",
    [
        "/api/v1/user/10",
        "/api/v1/user10",
        "api/v1/user/10",
        "/api/v1/user/10/extra",
        "/api/v1/user/10/extra/",
        "totally wrong",
        "/api/v1/banana/1/",
        "/api/v1/project",
        "/api/v1/project/1/additional/",
    ],
)
def test_malformed_uris(uri: str) -> None:
    with pytest.raises(ValueError):
        URI(uri)


def test_uri_serialization() -> None:

    uri_content = "/api/v1/user/10/"
    uri = URI(uri_content)

    assert uri.model_dump() == uri_content
