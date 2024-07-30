# pylint: disable=missing-function-docstring
"""Fixtures for the data classification app
"""

from pytest import fixture

from src.mytardis_client.common_types import DataClassification


@fixture
def project_data_classification() -> DataClassification:
    return DataClassification.SENSITIVE


@fixture
def experiment_data_classification() -> DataClassification:
    return DataClassification.SENSITIVE


@fixture
def dataset_data_classification() -> DataClassification:
    return DataClassification.SENSITIVE
