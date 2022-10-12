from datetime import datetime
from pathlib import Path
from typing import List

import pytz
from pytest import fixture

from src.blueprints import GroupACL, Parameter, UserACL, Username
from src.blueprints.custom_data_types import URI


@fixture
def timezone():
    return pytz.timezone("Pacific/Auckland")


@fixture
def username():
    return "upi000"


@fixture
def api_key():
    return "test_api_key"


@fixture
def hostname():
    return "https://test-mytardis.nectar.auckland.ac.nz"


@fixture
def verify_certificate():
    return True


@fixture
def proxies():
    return {"http": "http://myproxy.com", "https": "http://myproxy.com"}


@fixture
def source_dir():
    return Path("/a/source/directory/")


@fixture
def target_dir():
    return Path("directory/")


@fixture
def storage_box_dir():
    return Path("/a/target/")


@fixture
def default_institution():
    return "Test Institution"


@fixture
def old_acls():
    return False


@fixture
def projects_enabled():
    return True


@fixture
def objects_with_ids():
    return [
        "dataset",
        "experiment",
        "facility",
        "instrument",
        "project",
        "institution",
    ]


@fixture
def filename():
    return "test_data.dat"


@fixture
def created_by_upi():
    return "upi001"


@fixture
def start_time_datetime(timezone):
    return datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def start_time_str(start_time_datetime: datetime) -> str:
    return start_time_datetime.isoformat()


@fixture
def created_time_datetime(timezone):
    return datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def created_time_str(start_time_datetime: datetime) -> str:
    return start_time_datetime.isoformat()


@fixture
def end_time_datetime(timezone):
    return datetime(2001, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def end_time_str(end_time_datetime: datetime) -> str:
    return end_time_datetime.isoformat()


@fixture
def modified_time_datetime(timezone):
    return datetime(2000, 7, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def modified_time_str(modified_time_datetime: datetime) -> str:
    return modified_time_datetime.isoformat()


@fixture
def embargo_time_datetime(timezone):
    return datetime(2005, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def embargo_time_str(embargo_time_datetime: datetime) -> str:
    return embargo_time_datetime.isoformat()


@fixture
def admin_groups():
    return [
        "Test_group_1",
        "Test_group_2",
    ]


@fixture
def read_groups():
    return [
        "Test_group_11",
        "Test_group_12",
        "Test_group_13",
        "Test_group_14",
    ]


@fixture
def download_groups():
    return [
        "Test_group_12",
        "Test_group_14",
        "Test_group_21",
        "Test_group_22",
    ]


@fixture
def sensitive_groups():
    return [
        "Test_group_13",
        "Test_group_14",
        "Test_group_22",
        "Test_group_31",
    ]


@fixture
def admin_users():
    return [
        "upi001",
        "upi002",
    ]


@fixture
def read_users():
    return [
        "upi011",
        "upi012",
        "upi013",
        "upi014",
    ]


@fixture
def download_users():
    return [
        "upi012",
        "upi014",
        "upi021",
        "upi022",
    ]


@fixture
def sensitive_users():
    return [
        "upi013",
        "upi014",
        "upi022",
        "upi031",
    ]


@fixture
def project_name():
    return "Test_Project"


@fixture
def project_description():
    return "A test project for the purposes of testing"


@fixture
def project_pid():
    return "Project_1"


@fixture
def project_ids():
    return [
        "Test_Project",
        "Project_Test_1",
    ]


@fixture
def project_principal_investigator():
    return "upi001"


@fixture
def project_institutions():
    return ["Test Institution"]


@fixture
def project_metadata():
    return {
        "project_my_test_key_1": "Test Value",
        "project_my_test_key_2": "Test Value 2",
    }


@fixture
def project_schema():
    return "https://test-mytardis.nectar.acukland.ac.nz/Test/v1/Project"


@fixture
def project_metadata_processed(project_metadata):
    return_list = []
    for key in project_metadata.keys():
        return_list.append(Parameter(name=key, value=project_metadata[key]))
    return sorted(return_list)


@fixture
def project_url():
    return "http://myproject.com"


@fixture
def experiment_name():
    return "Test_Experiment"


@fixture
def experiment_description():
    return "A test experiment for the purposes of testing"


@fixture
def experiment_institution(default_institution):
    return default_institution


@fixture
def experiment_projects():
    return [
        "Project_1",
        "Test_Project",
    ]


@fixture
def experiment_pid():
    return "Experiment_1"


@fixture
def experiment_ids():
    return [
        "Test_Experiment",
        "Experiment_Test_1",
    ]


@fixture
def experiment_metadata():
    return {
        "experiment_my_test_key_1": "Test Value",
        "experiment_my_test_key_2": "Test Value 2",
    }


@fixture
def experiment_schema():
    return "https://test-mytardis.nectar.acukland.ac.nz/Test/v1/Experiment"


@fixture
def experiment_metadata_processed(experiment_metadata):
    return_list = []
    for key in experiment_metadata.keys():
        return_list.append(Parameter(name=key, value=experiment_metadata[key]))
    return sorted(return_list)


@fixture
def experiment_url():
    return "http://myexperiment.com"


@fixture
def dataset_dir() -> Path:
    return Path("dataset/")


@fixture
def dataset_name():
    return "Test_Dataset"


@fixture
def dataset_description():
    return "This is a dataset"


@fixture
def dataset_experiments():
    return [
        "Experiment_1",
        "Test_Experiment",
    ]


@fixture
def dataset_instrument():
    return "Test_Instrument"


@fixture
def dataset_pid():
    return "Dataset_1"


@fixture
def dataset_ids():
    return ["Test_Dataset", "Dataset_Test_1"]


@fixture
def dataset_metadata():
    return {
        "dataset_my_test_key_1": "Test Value",
        "dataset_my_test_key_2": "Test Value 2",
    }


@fixture
def dataset_schema():
    return "https://test-mytardis.nectar.acukland.ac.nz/Test/v1/Dataset"


@fixture
def dataset_metadata_processed(dataset_metadata):
    return_list = []
    for key in dataset_metadata.keys():
        return_list.append(Parameter(name=key, value=dataset_metadata[key]))
    return sorted(return_list)


@fixture
def datafile_md5sum():
    return "0d32909e86e422d04a053d1ba26a990e"


@fixture
def datafile_mimetype():
    return "text/plain"


@fixture
def datafile_size():
    return 52428800


@fixture
def datafile_dataset():
    return "Test_Dataset"


@fixture
def datafile_metadata():
    return {
        "datafile_my_test_key_1": "Test Value",
        "datafile_my_test_key_2": "Test Value 2",
    }


@fixture
def datafile_schema():
    return "https://test-mytardis.nectar.auckland.ac.nz/Test/v1/Datafile"


@fixture
def datafile_metadata_processed(datafile_metadata):
    return_list = []
    for key in datafile_metadata.keys():
        return_list.append(Parameter(name=key, value=datafile_metadata[key]))
    return sorted(return_list)


@fixture
def instrument_alternate_ids():
    return [
        "Test_Instrument",
        "Instrument_Test_1",
    ]


@fixture
def instrument_pid():
    return "Instrument_1"


@fixture
def instrument_name():
    return "Test Instrument"


@fixture
def split_and_parse_users(
    admin_users: List[str],
    read_users: List[str],
    download_users: List[str],
    sensitive_users: List[str],
) -> List[UserACL]:
    return_list: List[UserACL] = []
    for admin_user in admin_users:
        return_list.append(
            UserACL(
                user=Username(admin_user),
                is_owner=True,
                can_download=True,
                see_sensitive=True,
            )
        )
    combined_users = list(set(read_users + download_users + sensitive_users))
    for user in combined_users:
        if user in admin_users:
            continue
        download = False
        sensitive = False
        if user in download_users:
            download = True
        if user in sensitive_users:
            sensitive = True
        return_list.append(
            UserACL(
                user=Username(user),
                is_owner=False,
                can_download=download,
                see_sensitive=sensitive,
            )
        )
    return return_list


@fixture
def split_and_parse_groups(
    admin_groups, read_groups, download_groups, sensitive_groups
):
    return_list: List[GroupACL] = []
    for admin_group in admin_groups:
        return_list.append(
            GroupACL(
                group=admin_group,
                is_owner=True,
                can_download=True,
                see_sensitive=True,
            )
        )
    combined_groups = list(set(read_groups + download_groups + sensitive_groups))
    for group in combined_groups:
        if group in admin_groups:
            continue
        download = False
        sensitive = False
        if group in download_groups:
            download = True
        if group in sensitive_groups:
            sensitive = True
        return_list.append(
            GroupACL(
                group=group,
                is_owner=False,
                can_download=download,
                see_sensitive=sensitive,
            )
        )
    return return_list


@fixture
def storage_box_name():
    return "Test_storage_box"


@fixture
def storage_box_uri():
    return "/api/v1/storagebox/1/"


@fixture
def storage_box_description():
    return "A test storage box"


@fixture
def directory_relative_to_storage_box(
    dataset_dir: Path,
    target_dir: Path,
    filename: str,
) -> Path:
    return Path(target_dir / dataset_dir / filename)


@fixture
def project_uri():
    return URI("/api/v1/project/1/")


@fixture
def experiment_uri():
    return URI("/api/v1/experiment/1/")


@fixture
def dataset_uri():
    return URI("/api/v1/dataset/1/")


@fixture
def datafile_uri():
    return URI("/api/v1/dataset_file/1002735/")


@fixture
def institution_uri():
    return URI("/api/v1/institution/1/")


@fixture
def instrument_uri():
    return URI("/api/v1/instrument/1/")


@fixture
def user_uri():
    return URI("/api/v1/user/1/")


@fixture
def institution_address():
    return "23 Symonds Street"


@fixture
def institution_alternate_ids():
    return ["UoA", "Uni"]


@fixture
def institution_country():
    return "NZ"


@fixture
def institution_name():
    return "University of Auckland"


@fixture
def institution_pid():
    return "UoA123"
