# pylint: disable=missing-module-docstring,missing-function-docstring

from copy import deepcopy

import responses
from responses import matchers

from .conftest import (
    config_dict,
    crucible,
    institution_response_dict,
    project_response_dict,
    tidied_experiment_dictionary,
    tidied_project_dictionary,
)


@responses.activate
def test_prepare_project(
    crucible,
    config_dict,
    tidied_project_dictionary,
    institution_response_dict,
):
    expected_dict = deepcopy(tidied_project_dictionary)
    expected_dict["institution"] = ["/api/v1/institution/1/"]
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/institution",
        match=[
            matchers.query_param_matcher(
                {"pids": tidied_project_dictionary["institution"][0]}
            )
        ],
        status=200,
        json=(institution_response_dict),
    )
    assert crucible.prepare_project(tidied_project_dictionary) == expected_dict


@responses.activate
def test_prepare_experiment(
    crucible,
    config_dict,
    project_response_dict,
    tidied_experiment_dictionary,
):
    expected_dict = deepcopy(tidied_experiment_dictionary)
    expected_dict["projects"] = ["/api/v1/project/1/"]
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher(
                {"pids": tidied_experiment_dictionary["projects"][0]}
            )
        ],
        status=200,
        json=(project_response_dict),
    )
    responses.add(
        responses.GET,
        f"{config_dict['hostname']}/api/v1/project",
        match=[
            matchers.query_param_matcher(
                {"pids": tidied_experiment_dictionary["projects"][1]}
            )
        ],
        status=200,
        json=(project_response_dict),
    )
    assert crucible.prepare_experiment(tidied_experiment_dictionary) == expected_dict
