# pylint: disable=missing-function-docstring
# nosec assert_used
import json

import pytest
import rocrate

import src.profiles.ro_crate._consts as RO_CrateConsts
from src.mytardis_client.types import MyTardisObjectType
from src.profiles.ro_crate.crate_to_tardis_mapper import CrateToTardisMapper
from tests.fixtures.fixtures_ro_crate import rocrate_profile_json


@pytest.fixture()
def rocrate_profile_reverse(rocrate_profile_json: str) -> dict[str, dict[str, str]]:
    return {
        object_key: {v: k for k, v in object_dict.items()}
        for object_key, object_dict in json.loads(rocrate_profile_json).items()
    }


@pytest.fixture()
def testing_rocrate_mapper(rocrate_profile_json: str) -> CrateToTardisMapper:
    return CrateToTardisMapper(json.loads(rocrate_profile_json))


# Test util for mapper
def test_load_rocrate_profile(
    rocrate_profile_json: str, rocrate_profile_reverse: dict[str, dict[str, str]]
) -> None:
    loaded_mapper: CrateToTardisMapper = CrateToTardisMapper(
        json.loads(rocrate_profile_json)
    )
    assert loaded_mapper.schema == json.loads(rocrate_profile_json)
    assert loaded_mapper.reverse_schema == rocrate_profile_reverse


# probably redundant to the above but checks if the mapping functions still work with a given input
def test_profile_mapping_functionality(
    testing_rocrate_mapper: CrateToTardisMapper,
) -> None:
    obj = MyTardisObjectType.PROJECT
    mt_field = "name"
    ro_crate_field = "@id"
    assert testing_rocrate_mapper.get_mt_field(obj, ro_crate_field) == mt_field
    assert testing_rocrate_mapper.get_roc_field(obj, mt_field) == ro_crate_field
