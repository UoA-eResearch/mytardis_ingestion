"""
Tests for caching in the Overseer.
"""

# pylint: disable=missing-function-docstring, missing-class-docstring

from typing import Any

import responses
from responses import matchers

from src.config.config import AuthConfig, ConnectionConfig
from src.mytardis_client.mt_rest import (
    GetResponse,
    GetResponseMeta,
    MyTardisRESTFactory,
)
from src.mytardis_client.objects import MyTardisObject, get_type_info
from src.mytardis_client.response_data import IngestedDatafile, MyTardisObjectData
from src.overseers.overseer import MyTardisEndpointCache, Overseer
from src.utils.container import subdict


def test_overseer_endpoint_cache(
    ingested_datafile: IngestedDatafile,
) -> None:

    df_cache = MyTardisEndpointCache("/dataset_file")

    objects: list[MyTardisObjectData] = [ingested_datafile]

    object_dict = ingested_datafile.model_dump()
    keys = subdict(object_dict, ["filename", "directory", "dataset"])

    assert df_cache.get(keys) is None

    df_cache.emplace(keys, objects)

    assert df_cache.get(keys) == objects


@responses.activate
def test_overseer_prefetch(
    auth: AuthConfig,
    connection: ConnectionConfig,
    ingested_datafile: IngestedDatafile,
    introspection_response: dict[str, Any],
) -> None:

    mt_client = MyTardisRESTFactory(auth, connection)
    overseer = Overseer(mt_client)

    total_count = 100

    def make_ingested_datafile(i: int) -> IngestedDatafile:
        idf = ingested_datafile.model_copy()
        idf.filename = f"ingested-file-{i}.txt"
        return idf

    ingested_datafiles = [make_ingested_datafile(i) for i in range(total_count)]

    responses.add(
        responses.GET,
        mt_client.compose_url("/dataset_file"),
        match=[
            matchers.query_param_matcher(
                {"dataset": "dataset-id-1", "limit": "500", "offset": "0"}
            ),
        ],
        status=200,
        json=GetResponse(
            objects=ingested_datafiles,
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

    num_objects = overseer.prefetch("/dataset_file", {"dataset": "dataset-id-1"})

    assert num_objects == total_count

    match_fields = get_type_info(MyTardisObject.DATAFILE).match_fields

    for df in ingested_datafiles:
        match_keys = subdict(df.model_dump(), match_fields)
        matches = overseer.get_matching_objects(MyTardisObject.DATAFILE, match_keys)
        assert len(matches) == 1
        assert matches[0] == df
