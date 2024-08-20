"""
Tests for caching in the Overseer.
"""

# pylint: disable=missing-function-docstring

from pathlib import Path
from typing import Any, Protocol, TypeVar

import pytest
import responses
from pydantic import BaseModel
from responses import matchers

from src.config.config import AuthConfig, ConnectionConfig
from src.mytardis_client.endpoints import URI
from src.mytardis_client.mt_rest import GetResponseMeta, MyTardisRESTFactory
from src.mytardis_client.objects import MyTardisObject
from src.mytardis_client.response_data import IngestedDatafile, MyTardisObjectData
from src.overseers.overseer import (
    MyTardisEndpointCache,
    Overseer,
    extract_values_for_matching,
)

T_co = TypeVar("T_co", bound=IngestedDatafile, covariant=True)


class TestModelFactory(Protocol[T_co]):
    """Protocol for a factory function that creates pydantic models to be used in tests."""

    def __call__(self, **kwargs: Any) -> T_co: ...


@pytest.fixture(name="make_ingested_datafile")
def _make_ingested_datafile() -> TestModelFactory[IngestedDatafile]:
    def _make_ingested_datafile(**kwargs: Any) -> IngestedDatafile:

        defaults = {
            "resource_uri": URI("/api/v1/dataset_file/1/"),
            "id": 1,
            "dataset": URI("/api/v1/dataset/1/"),
            "deleted": False,
            "directory": Path("path/to/df_1"),
            "filename": "df_1.txt",
            "md5sum": "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
            "mimetype": "text/plain",
            "parameter_sets": [],
            "public_access": False,
            "replicas": [],
            "size": 1024,
            "version": 1,
            "created_time": None,
            "deleted_time": None,
            "modification_time": None,
            "identifiers": ["dataset-id-1"],
        }

        result = {**defaults, **kwargs}
        return IngestedDatafile.model_validate(result)

    return _make_ingested_datafile


def test_overseer_endpoint_cache(
    make_ingested_datafile: TestModelFactory[IngestedDatafile],
) -> None:

    df_cache = MyTardisEndpointCache("/dataset_file")

    df_1 = make_ingested_datafile()

    objects: list[MyTardisObjectData] = [df_1]

    object_dict = df_1.model_dump()
    keys = {key: object_dict[key] for key in ["filename", "directory", "dataset"]}

    assert df_cache.get(keys) is None

    df_cache.emplace(keys, objects)

    assert df_cache.get(keys) == objects


class MockGetResponse(BaseModel):
    meta: GetResponseMeta
    objects: list[IngestedDatafile]


@responses.activate
def test_overseer_prefetch(
    auth: AuthConfig,
    connection: ConnectionConfig,
    make_ingested_datafile: TestModelFactory[IngestedDatafile],
    introspection_response: dict[str, Any],
) -> None:

    mt_client = MyTardisRESTFactory(auth, connection)
    overseer = Overseer(mt_client)

    ingested_datafile = make_ingested_datafile()

    responses.add(
        responses.GET,
        mt_client.compose_url("/dataset_file"),
        match=[
            matchers.query_param_matcher(
                {"dataset": "dataset-id-1", "limit": "500", "offset": "0"}
            ),
        ],
        status=200,
        json=MockGetResponse(
            objects=[ingested_datafile],
            meta=GetResponseMeta(
                limit=500, offset=0, total_count=1, next=None, previous=None
            ),
        ).model_dump(mode="json"),
    )

    responses.add(
        responses.GET,
        mt_client.compose_url("/introspection"),
        status=200,
        json=introspection_response,
    )

    overseer.prefetch("/dataset_file", {"dataset": "dataset-id-1"})

    matching_vals = extract_values_for_matching(
        MyTardisObject.DATAFILE, ingested_datafile.model_dump()
    )

    # NOTE: slightly wrong here to use IngestedDatafile for match values. Need to be sure of differences
    #       between IngestedDatafile and Datafile (dataset reference format etc.), as the latter is what
    #       we will be querying with.
    match_df = overseer.get_matching_objects(MyTardisObject.DATAFILE, matching_vals)

    assert match_df == [ingested_datafile]

    # Query for some individual objects, and ensure the cache is hit, and no request is made.
