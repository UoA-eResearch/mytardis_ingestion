# pylint:

from pathlib import Path

from _pytest.config import directory_arg
from pytest import fixture

import tests.fixtures_constants as const
from src.blueprints import (
    URI,
    DatafileReplica,
    ParameterSet,
    RawDatafile,
    RawDataset,
    RawExperiment,
    RawProject,
    StorageBox,
    Username,
)
from src.blueprints.project import RefinedProject

storage_box_name = const.storage_box_name
storage_box_uri = const.storage_box_uri
storage_box_description = const.storage_box_description
storage_box_dir = const.storage_box_dir
filename = const.filename
target_dir = const.target_dir
project_name = const.project_name
project_description = const.project_description
project_pid = const.project_pid
project_ids = const.project_ids
project_principal_investigator = const.project_principal_investigator
project_institutions = const.project_institutions
project_metadata = const.project_metadata
project_schema = const.project_schema
project_metadata_processed = const.project_metadata_processed
project_url = const.project_url
experiment_name = const.experiment_name
experiment_description = const.experiment_description
experiment_institution = const.experiment_institution
experiment_projects = const.experiment_projects
experiment_pid = const.experiment_pid
experiment_ids = const.experiment_ids
experiment_metadata = const.experiment_metadata
experiment_schema = const.experiment_schema
experiment_metadata_processed = const.experiment_metadata_processed
dataset_dir = const.dataset_dir
dataset_name = const.dataset_name
dataset_experiments = const.dataset_experiments
dataset_instrument = const.dataset_instrument
datset_pid = const.dataset_pid
dataset_ids = const.dataset_ids
dataset_metadata = const.dataset_metadata
dataset_schema = const.dataset_schema
dataset_metadata_processed = const.dataset_metadata_processed
datafile_md5sum = const.datafile_md5sum
datafile_mimetype = const.datafile_mimetype
datafile_size = const.datafile_size
datafile_dataset = const.datafile_dataset
datafile_metadata = const.datafile_metadata
datafile_schema = const.datafile_schema
datafile_metadata_processed = const.datafile_metadata_processed
split_and_parse_groups = const.split_and_parse_groups
split_and_parse_users = const.split_and_parse_users
start_time_datetime = const.start_time_datetime
start_time_str = const.start_time_str
end_time_datetime = const.end_time_datetime
end_time_str = const.end_time_str
modified_time_datetime = const.modified_time_datetime
modified_time_str = const.modified_time_str
embargo_time_datetime = const.embargo_time_datetime
embargo_time_str = const.embargo_time_str
created_by_upi = const.created_by_upi
created_time_datetime = const.created_time_datetime
created_time_str = const.created_time_str
directory_relative_to_storage_box = const.directory_relative_to_storage_box


@fixture
def storage_box(
    storage_box_name, storage_box_dir, storage_box_uri, storage_box_description
):
    return StorageBox(
        name=storage_box_name,
        location=Path(storage_box_dir),
        uri=URI(storage_box_uri),
        description=storage_box_description,
    )


@fixture
def datafile_replica(storage_box, filename, target_dir):
    return DatafileReplica(
        uri=Path(Path(target_dir) / Path(filename)).as_posix(),
        location=storage_box.name,
        protocol="file",
    )


@fixture
def raw_project_parameterset(project_schema, project_metadata_processed):
    return ParameterSet(
        schema=project_schema, parameters=project_metadata_processed
    )


@fixture
def raw_project(
    project_name,
    project_description,
    project_ids,
    project_pid,
    project_principal_investigator,
    project_institutions,
    split_and_parse_users,
    split_and_parse_groups,
    created_by_upi,
    start_time_datetime,
    end_time_str,
    embargo_time_datetime,
    project_metadata,
    project_url,
):
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
        end_time=end_time_str,
        embargo_until=embargo_time_datetime,
        persistent_id=project_pid,
        alternate_ids=project_ids,
        metadata=project_metadata,
    )


@fixture
def raw_experiment_parameterset(
    experiment_schema, experiment_metadata_processed
):
    return ParameterSet(
        schema=experiment_schema, parameters=experiment_metadata_processed
    )


@fixture
def raw_experiment(
    experiment_name,
    experiment_description,
    experiment_ids,
    experiment_pid,
    experiment_institution,
    experiment_projects,
    created_by_upi,
    experiment_url,
    split_and_parse_users,
    split_and_parse_groups,
    start_time_str,
    end_time_datetime,
    created_time_datetime,
    modified_time_str,
    embargo_time_datetime,
    experiment_metadata,
):
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
        start_time=start_time_str,
        end_time=end_time_datetime,
        created_time=created_time_datetime,
        update_time=modified_time_str,
        embargo_until=embargo_time_datetime,
        persistent_id=experiment_pid,
        alternate_ids=experiment_ids,
        metadata=experiment_metadata,
    )


@fixture
def raw_dataset_parameterset(dataset_schema, dataset_metadata_processed):
    return ParameterSet(
        schema=dataset_schema, parameters=dataset_metadata_processed
    )


@fixture
def raw_dataset(
    dataset_name,
    dataset_experiments,
    dataset_instrument,
    dataset_pid,
    dataset_ids,
    dataset_dir,
    split_and_parse_users,
    split_and_parse_groups,
    created_time_str,
    modified_time_datetime,
    dataset_metadata,
):
    return RawDataset(
        description=dataset_name,
        directory=dataset_dir,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        immutable=False,
        experiments=dataset_experiments,
        instrument=dataset_instrument,
        created_time=created_time_str,
        modified_time=modified_time_datetime,
        persistent_id=dataset_pid,
        alternate_ids=dataset_ids,
        metadata=dataset_metadata,
    )


@fixture
def raw_datafile_parameterset(datafile_schema, datafile_metadata_processed):
    return ParameterSet(
        schema=datafile_schema, parameters=datafile_metadata_processed
    )


@fixture
def raw_datafile(
    filename,
    datafile_md5sum,
    datafile_mimetype,
    datafile_size,
    datafile_dataset,
    directory_relative_to_storage_box,
    split_and_parse_users,
    split_and_parse_groups,
    datafile_metadata,
):
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
def refined_project(
    project_name,
    project_description,
    project_principal_investigator,
    project_url,
    split_and_parse_users,
    split_and_parse_groups,
    project_institutions,
    created_by_upi,
    start_time_datetime,
    end_time_str,
    embargo_time_datetime,
    project_pid,
    project_ids,
) -> RefinedProject:
    return RefinedProject(
        name=project_name,
        description=project_description,
        principal_investigator=Username(project_principal_investigator),
        url=project_url,
        users=split_and_parse_users,
        groups=split_and_parse_groups,
        institution=project_institutions,
        created_by=created_by_upi,
        start_time=start_time_datetime,
        end_time=end_time_str,
        embargo_until=embargo_time_datetime,
        persistent_id=project_pid,
        alternate_ids=project_ids,
    )
