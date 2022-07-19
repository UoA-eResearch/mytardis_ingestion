# NB: For the purposes of testing the IngestionFactory codebase a YAML specific
# implementation of the IngestionFactory is used. This concrete class defines
# the YAMLSmelter class to be the smelter implementation. As this class is fully
# tested, there is little risk in using this as a test case.

import logging
from pathlib import Path
from urllib.parse import urljoin

import mock
import responses
from pytest import fixture
from responses import matchers
from src.helpers.config import MyTardisConnection, MyTardisSettings

from src.ingestion_factory import IngestionFactory
from src.smelters import Smelter


logger = logging.getLogger(__name__)
logger.propagate = True

GLOB_STRING = "*.test"

# pylint: disable=missing-function-docstring


@fixture
def factory(
    smelter: Smelter,
    mytardis_settings: MyTardisSettings,
) -> IngestionFactory:
    # with mock.patch(
    #     "src.ingestion_factory.factory.IngestionFactory.get_smelter"
    # ) as mock_get_smelter:
    #     mock_get_smelter.return_value = smelter
    IngestionFactory.__abstractmethods__ = set()
    return IngestionFactory(
        general=mytardis_settings.general,
        auth=mytardis_settings.auth,
        connection=mytardis_settings.connection,
        mytardis_setup=mytardis_settings.mytardis_setup,
        smelter=smelter,
    )


@fixture
def search_with_no_uri_dict():
    return {
        "institution": "Uni RoR",
        "project": "Test_Project",
        "experiment": "Test_Experiment",
        "dataset": "Test_Dataset",
        "instrument": "Instrument_1",
    }


def mock_smelter_get_objects_in_input_file(file_path):
    file_path = Path(file_path)
    with open(file_path) as test_file:
        for line in test_file.readlines():
            if line.startswith("project"):
                return ["project"]
            if line.startswith("experiment"):
                return ["experiment"]
            if line.startswith("dataset"):
                return ["dataset"]
            if line.startswith("datafile"):
                return ["datafile"]
    return [None]


def test_build_object_lists(datadir, factory: IngestionFactory):
    with mock.patch.object(
        factory.smelter, "get_objects_in_input_file"
    ) as mock_get_inputs_in_input_file:
        mock_get_inputs_in_input_file.side_effect = (
            mock_smelter_get_objects_in_input_file
        )
        assert factory.build_object_lists(
            Path(datadir / "projects.test"), "project"
        ) == [Path(datadir / "projects.test")]
        assert (
            factory.build_object_lists(Path(datadir / "projects.test"), "experiment")
            == []
        )
        # FIXME fix the datadir problems
        # assert set(factory.build_object_lists(datadir, "project")) == set(
        #     [
        #         Path(datadir / "projects.test"),
        #         Path(datadir / "projects_2.test"),
        #         Path(datadir / "projects_3.test"),
        #     ]
        # )
        # assert factory.build_object_lists(datadir, "experiment") == [
        #     Path(datadir / "experiment.test")
        # ]
        # assert factory.build_object_lists(datadir, "dataset") == [
        #     Path(datadir / "dataset.test")
        # ]
        # assert factory.build_object_lists(datadir, "datafile") == [
        #     Path(datadir / "datafile.test")
        # ]


@responses.activate
def test_replace_search_term_with_uri(
    factory: IngestionFactory,
    institution_response_dict,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(institution_response_dict),
    )
    object_type = "institution"
    test_dict = {object_type: search_with_no_uri_dict[object_type]}
    assert factory.replace_search_term_with_uri(object_type, test_dict, "name") == {
        "institution": ["/api/v1/institution/1/"]
    }


@responses.activate
def test_replace_search_term_with_uri_fallback_search(
    factory: IngestionFactory,
    response_dict_not_found,
    institution_response_dict,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(institution_response_dict),
    )
    object_type = "institution"
    test_dict = {object_type: search_with_no_uri_dict[object_type]}
    assert factory.replace_search_term_with_uri(object_type, test_dict, "name") == {
        "institution": ["/api/v1/institution/1/"]
    }


@responses.activate
def test_replace_search_term_with_uri_not_found(
    factory: IngestionFactory,
    response_dict_not_found,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["institution"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    object_type = "institution"
    assert (
        factory.replace_search_term_with_uri(
            object_type, search_with_no_uri_dict, "name"
        )
        == search_with_no_uri_dict
    )


@responses.activate
def test_replace_search_term_with_uri_http_error(
    factory: IngestionFactory,
    institution_response_dict,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["institution"]}
            )
        ],
        status=404,
        json=(institution_response_dict),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "institution"),
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["institution"]}
            )
        ],
        status=404,
        json=(institution_response_dict),
    )
    object_type = "institution"
    assert (
        factory.replace_search_term_with_uri(
            object_type, search_with_no_uri_dict, "name"
        )
        == search_with_no_uri_dict
    )


@responses.activate
def test_get_project_uri(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    project_response_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "project"),
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(project_response_dict),
    )
    assert factory.get_project_uri(search_with_no_uri_dict["project"]) == [
        "/api/v1/project/1/"
    ]


@responses.activate
def test_get_project_uri_fall_back_search(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    project_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "project"),
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "project"),
        match=[
            matchers.query_param_matcher({"name": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(project_response_dict),
    )
    assert factory.get_project_uri(search_with_no_uri_dict["project"]) == [
        "/api/v1/project/1/"
    ]


@responses.activate
def test_get_project_uri_not_found(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "project"),
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "project"),
        match=[
            matchers.query_param_matcher({"name": search_with_no_uri_dict["project"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert factory.get_project_uri(search_with_no_uri_dict["project"]) == None


@responses.activate
def test_get_experiment_uri(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    experiment_response_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "experiment"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(experiment_response_dict),
    )
    assert factory.get_experiment_uri(search_with_no_uri_dict["experiment"]) == [
        "/api/v1/experiment/1/"
    ]


@responses.activate
def test_get_experiment_uri_fall_back_search(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    experiment_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "experiment"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "experiment"),
        match=[
            matchers.query_param_matcher(
                {"title": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(experiment_response_dict),
    )
    assert factory.get_experiment_uri(search_with_no_uri_dict["experiment"]) == [
        "/api/v1/experiment/1/"
    ]


@responses.activate
def test_get_experiment_uri_not_found(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "experiment"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "experiment"),
        match=[
            matchers.query_param_matcher(
                {"title": search_with_no_uri_dict["experiment"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert factory.get_experiment_uri(search_with_no_uri_dict["experiment"]) == None


@responses.activate
def test_get_dataset_uri(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    dataset_response_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "dataset"),
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["dataset"]})
        ],
        status=200,
        json=(dataset_response_dict),
    )
    assert factory.get_dataset_uri(search_with_no_uri_dict["dataset"]) == [
        "/api/v1/dataset/1/"
    ]


@responses.activate
def test_get_dataset_uri_fall_back_search(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    dataset_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "dataset"),
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["dataset"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "dataset"),
        match=[
            matchers.query_param_matcher(
                {"description": search_with_no_uri_dict["dataset"]}
            )
        ],
        status=200,
        json=(dataset_response_dict),
    )
    assert factory.get_dataset_uri(search_with_no_uri_dict["dataset"]) == [
        "/api/v1/dataset/1/"
    ]


@responses.activate
def test_get_dataset_uri_not_found(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "dataset"),
        match=[
            matchers.query_param_matcher({"pids": search_with_no_uri_dict["dataset"]})
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "dataset"),
        match=[
            matchers.query_param_matcher(
                {"description": search_with_no_uri_dict["dataset"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert factory.get_dataset_uri(search_with_no_uri_dict["dataset"]) == None


@responses.activate
def test_get_instrument_uri(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    instrument_response_dict,
    search_with_no_uri_dict,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "instrument"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(instrument_response_dict),
    )
    assert factory.get_instrument_uri(search_with_no_uri_dict["instrument"]) == [
        "/api/v1/instrument/1/"
    ]


@responses.activate
def test_get_instrument_uri_fall_back_search(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    instrument_response_dict,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "instrument"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "instrument"),
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(instrument_response_dict),
    )
    assert factory.get_instrument_uri(search_with_no_uri_dict["instrument"]) == [
        "/api/v1/instrument/1/"
    ]


@responses.activate
def test_get_instrument_uri_not_found(
    factory: IngestionFactory,
    connection: MyTardisConnection,
    search_with_no_uri_dict,
    response_dict_not_found,
):
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "instrument"),
        match=[
            matchers.query_param_matcher(
                {"pids": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    responses.add(
        responses.GET,
        urljoin(connection.api_template, "instrument"),
        match=[
            matchers.query_param_matcher(
                {"name": search_with_no_uri_dict["instrument"]}
            )
        ],
        status=200,
        json=(response_dict_not_found),
    )
    assert factory.get_instrument_uri(search_with_no_uri_dict["instrument"]) is None