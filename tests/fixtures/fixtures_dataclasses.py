# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=missing-module-docstring

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from pytest import fixture

from src.blueprints.common_models import GroupACL, Parameter, ParameterSet, UserACL
from src.blueprints.custom_data_types import URI, ISODateTime, Username
from src.blueprints.datafile import (
    Datafile,
    DatafileReplica,
    RawDatafile,
    RefinedDatafile,
)
from src.blueprints.dataset import Dataset, RawDataset, RefinedDataset
from src.blueprints.experiment import Experiment, RawExperiment, RefinedExperiment
from src.blueprints.project import Project, RawProject, RefinedProject
from src.blueprints.storage_boxes import StorageBox
from src.helpers.enumerators import DataClassification


@fixture
def archive_box(
    archive_box_name: str,
    archive_box_dir: str,
    archive_box_uri: str,
    archive_box_description: str,
) -> StorageBox:
    return StorageBox(
        name=archive_box_name,
        location=Path(archive_box_dir),
        uri=URI(archive_box_uri),
        description=archive_box_description,
    )


@fixture
def storage_box(
    storage_box_name: str,
    storage_box_dir: str,
    storage_box_uri: str,
    storage_box_description: str,
) -> StorageBox:
    return StorageBox(
        name=storage_box_name,
        location=Path(storage_box_dir),
        uri=URI(storage_box_uri),
        description=storage_box_description,
    )


@fixture
def datafile_replica(
    storage_box: StorageBox,
    dataset_dir: Path,
    filename: str,
    target_dir: Path,
) -> DatafileReplica:
    return DatafileReplica(
        uri=Path(target_dir / dataset_dir / filename).as_posix(),
        location=storage_box.name,
        protocol="file",
    )


@fixture
def archive_replica(
    archive_box: StorageBox,
    dataset_dir: Path,
    filename: str,
    target_dir: Path,
) -> DatafileReplica:
    return DatafileReplica(
        uri=Path(target_dir / dataset_dir / filename).as_posix(),
        location=archive_box.name,
        protocol="file",
    )


@fixture
def raw_project_parameterset(
    project_schema: str,
    project_metadata_processed: List[Parameter],
) -> ParameterSet:
    return ParameterSet(schema=project_schema, parameters=project_metadata_processed)


@fixture
def raw_project(  # pylint: disable=too-many-locals,too-many-arguments
    project_name: str,
    project_description: str,
    project_ids: List[str],
    project_principal_investigator: str,
    project_institutions: List[str],
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    created_by_upi: str,
    start_time_datetime: datetime,
    end_time_datetime: datetime,
    embargo_time_datetime: datetime,
    project_metadata: Dict[str, Any],
    project_url: str,
    project_data_classification: DataClassification,
    archive_in_days: int,
    delete_in_days: int,
) -> RawProject:
    return RawProject(
        name=project_name,
        description=project_description,
        principal_investigator=Username(project_principal_investigator),
        url=project_url,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        institution=project_institutions,
        created_by=created_by_upi,
        start_time=start_time_datetime,
        end_time=end_time_datetime,
        embargo_until=embargo_time_datetime,
        identifiers=project_ids,
        metadata=project_metadata,
        data_classification=project_data_classification,
        archive_in_days=archive_in_days,
        delete_in_days=delete_in_days,
    )


@fixture
def raw_experiment_parameterset(
    experiment_schema: str,
    experiment_metadata_processed: List[Parameter],
) -> ParameterSet:
    return ParameterSet(
        schema=experiment_schema, parameters=experiment_metadata_processed
    )


@fixture
def raw_experiment(  # pylint: disable=too-many-locals,too-many-arguments
    experiment_name: str,
    experiment_description: str,
    experiment_ids: List[str],
    experiment_institution: str,
    experiment_projects: List[str],
    created_by_upi: str,
    experiment_url: str,
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    start_time_datetime: datetime,
    end_time_datetime: datetime,
    created_time_datetime: datetime,
    modified_time_datetime: datetime,
    embargo_time_datetime: datetime,
    experiment_metadata: Dict[str, Any],
    experiment_data_classification: DataClassification,
) -> RawExperiment:
    return RawExperiment(
        title=experiment_name,
        description=experiment_description,
        institution_name=experiment_institution,
        created_by=created_by_upi,
        url=experiment_url,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        locked=False,
        projects=experiment_projects,
        start_time=start_time_datetime,
        end_time=end_time_datetime,
        created_time=created_time_datetime,
        update_time=modified_time_datetime,
        embargo_until=embargo_time_datetime,
        identifiers=experiment_ids,
        metadata=experiment_metadata,
        data_classification=experiment_data_classification,
    )


@fixture
def raw_dataset_parameterset(
    dataset_schema: str,
    dataset_metadata_processed: List[Parameter],
) -> ParameterSet:
    return ParameterSet(schema=dataset_schema, parameters=dataset_metadata_processed)


@fixture
def raw_dataset(  # pylint:disable=too-many-arguments
    dataset_name: str,
    dataset_experiments: List[str],
    dataset_instrument: str,
    dataset_ids: List[str],
    dataset_dir: Path,
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    created_time_datetime: datetime,
    modified_time_datetime: datetime,
    dataset_metadata: Dict[str, Any],
    dataset_data_classification: DataClassification,
) -> RawDataset:
    return RawDataset(
        description=dataset_name,
        directory=dataset_dir,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        immutable=False,
        experiments=dataset_experiments,
        instrument=dataset_instrument,
        created_time=created_time_datetime,
        modified_time=modified_time_datetime,
        identifiers=dataset_ids,
        metadata=dataset_metadata,
        data_classification=dataset_data_classification,
    )


@fixture
def raw_datafile_parameterset(
    datafile_schema: str,
    datafile_metadata_processed: List[Parameter],
) -> ParameterSet:
    return ParameterSet(schema=datafile_schema, parameters=datafile_metadata_processed)


@fixture
def raw_datafile(
    filename: str,
    datafile_md5sum: str,
    datafile_mimetype: str,
    datafile_size: int,
    datafile_dataset: str,
    directory_relative_to_storage_box: Path,
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    datafile_metadata: Dict[str, Any],
) -> RawDatafile:
    return RawDatafile(
        filename=filename,
        md5sum=datafile_md5sum,
        mimetype=datafile_mimetype,
        size=datafile_size,
        directory=Path(directory_relative_to_storage_box),
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        dataset=datafile_dataset,
        metadata=datafile_metadata,
    )


@fixture
def refined_project(  # pylint:disable=too-many-arguments
    project_name: str,
    project_description: str,
    project_ids: List[str],
    project_principal_investigator: str,
    project_institutions: List[str],
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    created_by_upi: str,
    start_time_datetime: datetime,
    end_time_datetime: datetime,
    embargo_time_datetime: datetime,
    project_url: str,
    project_data_classification: DataClassification,
) -> RefinedProject:
    return RefinedProject(
        name=project_name,
        description=project_description,
        data_classification=project_data_classification,
        principal_investigator=Username(project_principal_investigator),
        dataclassification=project_data_classification,
        url=project_url,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        institution=project_institutions,
        created_by=created_by_upi,
        start_time=start_time_datetime,
        end_time=end_time_datetime,
        embargo_until=embargo_time_datetime,
        identifiers=project_ids,
    )


@fixture
def refined_experiment(  # pylint: disable=too-many-arguments
    experiment_name: str,
    experiment_description: str,
    experiment_ids: List[str],
    experiment_institution: str,
    experiment_projects: List[str],
    created_by_upi: str,
    experiment_url: str,
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    start_time_datetime: datetime,
    end_time_datetime: datetime,
    created_time_datetime: datetime,
    modified_time_datetime: datetime,
    embargo_time_datetime: datetime,
    experiment_data_classification: Enum,
) -> RefinedExperiment:
    return RefinedExperiment(
        title=experiment_name,
        description=experiment_description,
        data_classification=experiment_data_classification,
        institution_name=experiment_institution,
        created_by=created_by_upi,
        url=experiment_url,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        locked=False,
        projects=experiment_projects,
        start_time=start_time_datetime,
        end_time=end_time_datetime,
        created_time=created_time_datetime,
        update_time=modified_time_datetime,
        embargo_until=embargo_time_datetime,
        identifiers=experiment_ids,
    )


@fixture
def refined_dataset(
    dataset_name: str,
    dataset_experiments: List[str],
    dataset_instrument: str,
    dataset_ids: List[str],
    dataset_dir: Path,
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    created_time_datetime: datetime,
    modified_time_datetime: datetime,
    dataset_data_classification: Enum,
) -> RefinedDataset:
    return RefinedDataset(
        description=dataset_name,
        directory=dataset_dir,
        data_classification=dataset_data_classification,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        immutable=False,
        experiments=dataset_experiments,
        instrument=dataset_instrument,
        created_time=created_time_datetime,
        modified_time=modified_time_datetime,
        identifiers=dataset_ids,
    )


@fixture
def refined_datafile(
    filename: str,
    datafile_md5sum: str,
    datafile_mimetype: str,
    datafile_size: int,
    datafile_dataset: str,
    directory_relative_to_storage_box: Path,
    split_and_parse_users: List[UserACL],
    split_and_parse_groups: List[GroupACL],
    raw_datafile_parameterset: ParameterSet,
) -> RefinedDatafile:
    return RefinedDatafile(
        filename=filename,
        md5sum=datafile_md5sum,
        mimetype=datafile_mimetype,
        size=datafile_size,
        directory=Path(directory_relative_to_storage_box),
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        dataset=datafile_dataset,
        parameter_sets=raw_datafile_parameterset,
    )


@fixture
def project(
    refined_project: RefinedProject,
    institution_uri: URI,
    archive_box: StorageBox,
    storage_box: StorageBox,
) -> Project:
    return Project(
        name=refined_project.name,
        description=refined_project.description,
        principal_investigator=refined_project.principal_investigator,
        url=refined_project.url,
        users=refined_project.users,
        groups=refined_project.groups,
        institution=[institution_uri],
        created_by=refined_project.created_by,
        data_classification=refined_project.data_classification,
        start_time=(
            ISODateTime(refined_project.start_time.isoformat())
            if isinstance(refined_project.start_time, datetime)
            else None
        ),
        end_time=(
            ISODateTime(refined_project.end_time.isoformat())
            if isinstance(refined_project.end_time, datetime)
            else None
        ),
        embargo_until=(
            ISODateTime(refined_project.embargo_until.isoformat())
            if isinstance(refined_project.embargo_until, datetime)
            else None
        ),
        identifiers=refined_project.identifiers,
        archives=[archive_box],
        active_stores=[storage_box],
    )


@fixture
def experiment(
    refined_experiment: RefinedExperiment,
    project_uri: URI,
) -> Experiment:
    return Experiment(
        title=refined_experiment.title,
        description=refined_experiment.description,
        data_classification=refined_experiment.data_classification,
        institution_name=refined_experiment.institution_name,
        created_by=refined_experiment.created_by,
        url=refined_experiment.url,
        users=refined_experiment.users,
        groups=refined_experiment.groups,
        locked=refined_experiment.locked,
        projects=[project_uri],
        start_time=(
            refined_experiment.start_time.isoformat()
            if isinstance(refined_experiment.start_time, datetime)
            else refined_experiment.start_time
        ),
        end_time=(
            refined_experiment.end_time.isoformat()
            if isinstance(refined_experiment.end_time, datetime)
            else refined_experiment.end_time
        ),
        created_time=(
            refined_experiment.created_time.isoformat()
            if isinstance(refined_experiment.created_time, datetime)
            else refined_experiment.created_time
        ),
        update_time=(
            refined_experiment.update_time.isoformat()
            if isinstance(refined_experiment.update_time, datetime)
            else refined_experiment.update_time
        ),
        embargo_until=(
            refined_experiment.embargo_until.isoformat()
            if isinstance(refined_experiment.embargo_until, datetime)
            else refined_experiment.embargo_until
        ),
        identifiers=refined_experiment.identifiers,
    )


@fixture
def dataset(
    refined_dataset: RefinedDataset,
    experiment_uri: URI,
    instrument_uri: URI,
) -> Dataset:
    return Dataset(
        description=refined_dataset.description,
        directory=refined_dataset.directory,
        data_classification=refined_dataset.data_classification,
        users=refined_dataset.users,
        groups=refined_dataset.groups,
        immutable=refined_dataset.immutable,
        experiments=[experiment_uri],
        instrument=instrument_uri,
        created_time=(
            refined_dataset.created_time.isoformat()
            if isinstance(refined_dataset.created_time, datetime)
            else refined_dataset.created_time
        ),
        modified_time=(
            refined_dataset.modified_time.isoformat()
            if isinstance(refined_dataset.modified_time, datetime)
            else refined_dataset.modified_time
        ),
        identifiers=refined_dataset.identifiers,
    )


@fixture
def datafile(
    refined_datafile: RefinedDatafile,
    dataset_uri: URI,
    datafile_replica: DatafileReplica,
    archive_date: datetime,
    delete_date: datetime,
    archive_replica: DatafileReplica,
) -> Datafile:
    return Datafile(
        filename=refined_datafile.filename,
        md5sum=refined_datafile.md5sum,
        mimetype=refined_datafile.mimetype,
        size=refined_datafile.size,
        directory=refined_datafile.directory,
        users=refined_datafile.users,
        groups=refined_datafile.groups,
        dataset=dataset_uri,
        parameter_sets=refined_datafile.parameter_sets,
        archive_date=ISODateTime(archive_date.isoformat()),
        delete_date=ISODateTime(delete_date.isoformat()),
        replicas=[
            archive_replica,
            datafile_replica,
        ],
    )
