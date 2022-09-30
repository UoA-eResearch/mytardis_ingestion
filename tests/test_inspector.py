import json
from urllib.parse import urljoin
import pytest
import responses
from responses import matchers

from src.blueprints.project import RawProject
from src.blueprints.experiment import RawExperiment
from src.blueprints.dataset import RawDataset
from src.blueprints.datafile import RawDatafile
from src.helpers.enumerators import (
    MyTardisObject,
    ObjectEnum,
    ObjectIngestionState,
    ObjectSearchDict,
    URLSubstring,
)
from src.overseers.inspector import Inspector
from src.helpers.config import ConnectionConfig

# TODO add other id fixtures here
@pytest.fixture
def search_value_by_substring(
    project_name: str,
    experiment_name: str,
    dataset_name: str,
    filename: str,
):
    def _get_search_value(substring: URLSubstring):
        match substring:
            case URLSubstring.PROJECT:
                return project_name
            case URLSubstring.EXPERIMENT:
                return experiment_name
            case URLSubstring.DATASET:
                return dataset_name
            case URLSubstring.DATAFILE:
                return filename
        return None

    return _get_search_value


@pytest.fixture
def mock_responses(
    search_value_by_substring,
    response_by_substring: dict,
    response_dict_not_found: dict,
    connection: ConnectionConfig,
):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        for obj in [
            ObjectEnum.PROJECT,
            ObjectEnum.EXPERIMENT,
            ObjectEnum.DATASET,
            ObjectEnum.DATAFILE,
        ]:
            search_type: ObjectSearchDict = obj.value["search_type"]
            url = urljoin(connection.api_template, search_type["url_substring"])
            query_param = {
                search_type["target"]: search_value_by_substring(
                    search_type["url_substring"]
                )
            }

            rsps.get(
                url,
                match=[matchers.query_param_matcher(query_param)],
                body=json.dumps(response_by_substring(search_type["url_substring"])),
            )
            rsps.get(url, body=json.dumps(response_dict_not_found))

        yield rsps


@pytest.fixture
def raw_object(
    raw_project: RawProject,
    raw_experiment: RawExperiment,
    raw_dataset: RawDataset,
    raw_datafile: RawDatafile,
):
    def _get_raw_object(type: MyTardisObject):
        match type:
            case MyTardisObject.PROJECT:
                return raw_project
            case MyTardisObject.EXPERIMENT:
                return raw_experiment
            case MyTardisObject.DATASET:
                return raw_dataset
            case MyTardisObject.DATAFILE:
                return raw_datafile

    return _get_raw_object


@pytest.fixture
def inspector_with_response_mock(
    mock_inspector, mock_responses: responses.RequestsMock
):
    return mock_inspector(mock_responses)


@pytest.mark.parametrize(
    "object_type",
    [
        MyTardisObject.PROJECT,
        MyTardisObject.EXPERIMENT,
        MyTardisObject.DATASET,
        MyTardisObject.DATAFILE,
    ],
)
def test_ingestion_state_existing_object(
    object_type: MyTardisObject,
    raw_object,
    inspector_with_response_mock: Inspector,
):
    obj: RawProject | RawExperiment | RawDataset | RawDatafile = raw_object(object_type)
    ingestion_state = inspector_with_response_mock.check_ingestion_state(obj)

    if isinstance(obj, RawDatafile):
        assert ingestion_state == ObjectIngestionState.BLOCKED
    else:
        assert ingestion_state == ObjectIngestionState.INGEST_CHILDREN
