import logging
from typing import Literal
from unittest.mock import MagicMock, Mock
import pytest
from pytest import fixture

from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import Datafile, RawDatafile, RefinedDatafile
from src.blueprints.dataset import (
    Dataset,
    DatasetParameterSet,
    RawDataset,
    RefinedDataset,
)
from src.blueprints.experiment import (
    Experiment,
    ExperimentParameterSet,
    RawExperiment,
    RefinedExperiment,
)
from src.blueprints.project import (
    Project,
    ProjectParameterSet,
    RawProject,
    RefinedProject,
)
from src.crucible.crucible import Crucible
from src.forges.forge import Forge
from src.helpers.config import ConfigFromEnv
from src.helpers.enumerators import MyTardisObject
from src.helpers.mt_rest import MyTardisRESTFactory
from src.ingestion_factory import IngestionFactory
from src.ingestion_factory.factory import IngestionResult
from src.overseers.overseer import Overseer
from src.smelters import Smelter

logger = logging.getLogger(__name__)
logger.propagate = True

# pylint: disable=missing-function-docstring


@fixture
def mock_ingestion_factory(
    mytardis_settings: ConfigFromEnv,
    rest_factory: MyTardisRESTFactory,
    overseer: Overseer,
    smelter: Smelter,
    crucible: Crucible,
    forge: Forge,
):
    def _get_mock_ingestion_factory(
        object_type: Literal[
            MyTardisObject.PROJECT,
            MyTardisObject.EXPERIMENT,
            MyTardisObject.DATASET,
            MyTardisObject.DATAFILE,
        ],
        smelter_method_mock: Mock = MagicMock(return_value=None),
        crucible_method_mock: Mock = MagicMock(return_value=None),
        forge_method_mock: Mock = MagicMock(return_value=(None, None)),
    ):
        match object_type:
            case MyTardisObject.PROJECT:
                smelter.smelt_project = smelter_method_mock
                crucible.prepare_project = crucible_method_mock
                forge.forge_project = forge_method_mock
            case MyTardisObject.EXPERIMENT:
                smelter.smelt_experiment = smelter_method_mock
                crucible.prepare_experiment = crucible_method_mock
                forge.forge_experiment = forge_method_mock
            case MyTardisObject.DATASET:
                smelter.smelt_dataset = smelter_method_mock
                crucible.prepare_dataset = crucible_method_mock
                forge.forge_dataset = forge_method_mock
            case MyTardisObject.DATAFILE:
                smelter.smelt_datafile = smelter_method_mock
                crucible.prepare_datafile = crucible_method_mock
                forge.forge_datafile = forge_method_mock

        return IngestionFactory(
            config=mytardis_settings,
            mt_rest=rest_factory,
            overseer=overseer,
            smelter=smelter,
            forge=forge,
            crucible=crucible,
        )

    return _get_mock_ingestion_factory


def test_ingest_project(
    caplog: pytest.LogCaptureFixture,
    mock_ingestion_factory: IngestionFactory,
    raw_project: RawProject,
    refined_project: RefinedProject,
    project: Project,
    raw_project_parameterset: ProjectParameterSet,
    project_uri: URI,
):
    ingestion_factory: IngestionFactory = mock_ingestion_factory(
        MyTardisObject.PROJECT,
        smelter_method_mock=MagicMock(
            return_value=(refined_project, raw_project_parameterset)
        ),
        crucible_method_mock=MagicMock(return_value=project),
        forge_method_mock=MagicMock(return_value=(project_uri, None)),
    )
    caplog.set_level(logging.INFO)
    info = f"Successfully ingested 1 projects: {[(raw_project.name, URI(project_uri))]}"
    expected_result = IngestionResult(
        success=[(raw_project.name, URI(project_uri))], error=[]
    )

    assert ingestion_factory.ingest_projects([raw_project]) == expected_result
    assert info in caplog.text

    caplog.clear()
    caplog.set_level(logging.WARNING)

    ingestion_factory.crucible.prepare_project = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 projects: {[raw_project.name]}"
    expected_result = IngestionResult(success=[], error=[raw_project.name])

    assert ingestion_factory.ingest_projects([raw_project]) == expected_result
    assert warning in caplog.text

    caplog.clear()

    ingestion_factory.smelter.smelt_project = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 projects: {[raw_project.name]}"
    expected_result = IngestionResult(success=[], error=[raw_project.name])

    assert ingestion_factory.ingest_projects([raw_project]) == expected_result
    assert warning in caplog.text


def test_ingest_experiment(
    caplog: pytest.LogCaptureFixture,
    mock_ingestion_factory: IngestionFactory,
    raw_experiment: RawExperiment,
    refined_experiment: RefinedExperiment,
    experiment: Experiment,
    raw_experiment_parameterset: ExperimentParameterSet,
    experiment_uri: URI,
):
    ingestion_factory: IngestionFactory = mock_ingestion_factory(
        MyTardisObject.EXPERIMENT,
        smelter_method_mock=MagicMock(
            return_value=(refined_experiment, raw_experiment_parameterset)
        ),
        crucible_method_mock=MagicMock(return_value=experiment),
        forge_method_mock=MagicMock(return_value=(experiment_uri, None)),
    )
    caplog.set_level(logging.INFO)
    info = f"Successfully ingested 1 experiments: {[(raw_experiment.title, URI(experiment_uri))]}"
    expected_result = IngestionResult(
        success=[(raw_experiment.title, URI(experiment_uri))], error=[]
    )

    assert ingestion_factory.ingest_experiments([raw_experiment]) == expected_result
    assert info in caplog.text

    caplog.clear()
    caplog.set_level(logging.WARNING)

    ingestion_factory.crucible.prepare_experiment = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 experiments: {[raw_experiment.title]}"
    expected_result = IngestionResult(success=[], error=[raw_experiment.title])

    assert ingestion_factory.ingest_experiments([raw_experiment]) == expected_result
    assert warning in caplog.text

    caplog.clear()

    ingestion_factory.smelter.smelt_experiment = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 experiments: {[raw_experiment.title]}"
    expected_result = IngestionResult(success=[], error=[raw_experiment.title])

    assert ingestion_factory.ingest_experiments([raw_experiment]) == expected_result
    assert warning in caplog.text


def test_ingest_dataset(
    caplog: pytest.LogCaptureFixture,
    mock_ingestion_factory: IngestionFactory,
    raw_dataset: RawDataset,
    refined_dataset: RefinedDataset,
    dataset: Dataset,
    raw_dataset_parameterset: DatasetParameterSet,
    dataset_uri: URI,
):
    ingestion_factory: IngestionFactory = mock_ingestion_factory(
        MyTardisObject.DATASET,
        smelter_method_mock=MagicMock(
            return_value=(refined_dataset, raw_dataset_parameterset)
        ),
        crucible_method_mock=MagicMock(return_value=dataset),
        forge_method_mock=MagicMock(return_value=(dataset_uri, None)),
    )
    caplog.set_level(logging.INFO)
    info = f"Successfully ingested 1 datasets: {[(raw_dataset.description, URI(dataset_uri))]}"
    expected_result = IngestionResult(
        success=[(raw_dataset.description, URI(dataset_uri))], error=[]
    )

    assert ingestion_factory.ingest_datasets([raw_dataset]) == expected_result
    assert info in caplog.text

    caplog.clear()
    caplog.set_level(logging.WARNING)

    ingestion_factory.crucible.prepare_dataset = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 datasets: {[raw_dataset.description]}"
    expected_result = IngestionResult(success=[], error=[raw_dataset.description])

    assert ingestion_factory.ingest_datasets([raw_dataset]) == expected_result
    assert warning in caplog.text

    caplog.clear()

    ingestion_factory.smelter.smelt_dataset = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 datasets: {[raw_dataset.description]}"
    expected_result = IngestionResult(success=[], error=[raw_dataset.description])

    assert ingestion_factory.ingest_datasets([raw_dataset]) == expected_result
    assert warning in caplog.text


def test_ingest_datafile(
    caplog: pytest.LogCaptureFixture,
    mock_ingestion_factory: IngestionFactory,
    raw_datafile: RawDatafile,
    refined_datafile: RefinedDatafile,
    datafile: Datafile,
    datafile_uri: URI,
):
    ingestion_factory: IngestionFactory = mock_ingestion_factory(
        MyTardisObject.DATAFILE,
        smelter_method_mock=MagicMock(return_value=refined_datafile),
        crucible_method_mock=MagicMock(return_value=datafile),
        forge_method_mock=MagicMock(return_value=datafile_uri),
    )
    caplog.set_level(logging.INFO)
    info = f"Successfully ingested 1 datafiles: {[(raw_datafile.filename, URI(datafile_uri))]}"
    expected_result = IngestionResult(
        success=[(raw_datafile.filename, URI(datafile_uri))], error=[]
    )

    result = ingestion_factory.ingest_datafiles([raw_datafile])
    assert result.success == expected_result.success
    assert result.error == expected_result.error
    assert result == expected_result
    assert info in caplog.text

    caplog.clear()
    caplog.set_level(logging.WARNING)

    ingestion_factory.crucible.prepare_datafile = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 datafiles: {[raw_datafile.filename]}"
    expected_result = IngestionResult(success=[], error=[raw_datafile.filename])

    assert ingestion_factory.ingest_datafiles([raw_datafile]) == expected_result
    assert warning in caplog.text

    caplog.clear()

    ingestion_factory.smelter.smelt_datafile = MagicMock(return_value=None)
    warning = f"There were errors ingesting 1 datafiles: {[raw_datafile.filename]}"
    expected_result = IngestionResult(success=[], error=[raw_datafile.filename])

    assert ingestion_factory.ingest_datafiles([raw_datafile]) == expected_result
    assert warning in caplog.text
