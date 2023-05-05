# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring

from datetime import datetime
from pathlib import Path

from pytest import fixture

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
from src.blueprints.custom_data_types import ISODateTime
from src.blueprints.datafile import Datafile, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject


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
def datafile_replica(storage_box, dataset_dir, filename, target_dir):
    return DatafileReplica(
        uri=Path(target_dir / dataset_dir / filename).as_posix(),
        location=storage_box.name,
        protocol="file",
    )


@fixture
def raw_project_parameterset(project_schema, project_metadata_processed):
    return ParameterSet(schema=project_schema, parameters=project_metadata_processed)


@fixture
def raw_project(
    project_name,
    project_description,
    project_ids,
    project_principal_investigator,
    project_institutions,
    split_and_parse_users,
    split_and_parse_groups,
    created_by_upi,
    start_time_datetime,
    end_time_datetime,
    embargo_time_datetime,
    project_metadata,
    project_url,
    project_data_classification,
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
        end_time=end_time_datetime,
        embargo_until=embargo_time_datetime,
        identifiers=project_ids,
        metadata=project_metadata,
        data_classification=project_data_classification,
    )


@fixture
def raw_experiment_parameterset(experiment_schema, experiment_metadata_processed):
    return ParameterSet(
        schema=experiment_schema, parameters=experiment_metadata_processed
    )


@fixture
def raw_experiment(
    experiment_name,
    experiment_description,
    experiment_ids,
    experiment_institution,
    experiment_projects,
    created_by_upi,
    experiment_url,
    split_and_parse_users,
    split_and_parse_groups,
    start_time_datetime,
    end_time_datetime,
    created_time_datetime,
    modified_time_datetime,
    embargo_time_datetime,
    experiment_metadata,
    experiment_data_classification,
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
def raw_dataset_parameterset(dataset_schema, dataset_metadata_processed):
    return ParameterSet(schema=dataset_schema, parameters=dataset_metadata_processed)


@fixture
def raw_dataset(
    dataset_name,
    dataset_experiments,
    dataset_instrument,
    dataset_ids,
    dataset_dir,
    split_and_parse_users,
    split_and_parse_groups,
    created_time_datetime,
    modified_time_datetime,
    dataset_metadata,
    dataset_data_classification,
):
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
def raw_datafile_parameterset(datafile_schema, datafile_metadata_processed):
    return ParameterSet(schema=datafile_schema, parameters=datafile_metadata_processed)


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
    end_time_datetime,
    embargo_time_datetime,
    project_ids,
    project_data_classification,
) -> RefinedProject:
    return RefinedProject(
        name=project_name,
        description=project_description,
        data_classification=project_data_classification,
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
    )


@fixture
def refined_experiment(
    experiment_name,
    experiment_description,
    experiment_ids,
    experiment_institution,
    experiment_projects,
    created_by_upi,
    experiment_url,
    split_and_parse_users,
    split_and_parse_groups,
    start_time_datetime,
    end_time_datetime,
    created_time_datetime,
    modified_time_datetime,
    embargo_time_datetime,
    experiment_data_classification,
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
    dataset_name,
    dataset_experiments,
    dataset_instrument,
    dataset_ids,
    dataset_dir,
    split_and_parse_users,
    split_and_parse_groups,
    created_time_datetime,
    modified_time_datetime,
    dataset_data_classification,
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
    filename,
    datafile_md5sum,
    datafile_mimetype,
    datafile_size,
    datafile_dataset,
    directory_relative_to_storage_box,
    split_and_parse_users,
    split_and_parse_groups,
    datafile_replica,
    raw_datafile_parameterset,
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
        replicas=[datafile_replica],
        parameter_sets=raw_datafile_parameterset,
    )


@fixture
def project(refined_project: RefinedProject, institution_uri: URI):
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
    )


@fixture
def experiment(refined_experiment: RefinedExperiment, project_uri: URI):
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
def dataset(refined_dataset: RefinedDataset, experiment_uri: URI, instrument_uri: URI):
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
def datafile(refined_datafile: RefinedDatafile, dataset_uri: URI):
    return Datafile(
        filename=refined_datafile.filename,
        md5sum=refined_datafile.md5sum,
        mimetype=refined_datafile.mimetype,
        size=refined_datafile.size,
        directory=refined_datafile.directory,
        users=refined_datafile.users,
        groups=refined_datafile.groups,
        dataset=dataset_uri,
        replicas=refined_datafile.replicas,
        parameter_sets=refined_datafile.parameter_sets,
    )
