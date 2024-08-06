# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pytz
from pytest import fixture
from pytz import BaseTzInfo

from src.blueprints.common_models import GroupACL, Parameter, UserACL
from src.blueprints.custom_data_types import Username
from src.blueprints.storage_boxes import StorageTypesEnum
from src.mytardis_client.common_types import ISODateTime
from src.mytardis_client.endpoints import URI


@fixture
def timezone() -> BaseTzInfo:
    return pytz.timezone("Pacific/Auckland")


@fixture
def username() -> str:
    return "upi000"


@fixture
def api_key() -> str:
    return "test_api_key"


@fixture
def hostname() -> str:
    return "https://test-mytardis.nectar.auckland.ac.nz"


@fixture
def verify_certificate() -> bool:
    return True


@fixture
def proxies() -> Dict[str, str]:
    return {"http": "http://myproxy.com", "https": "http://myproxy.com"}


@fixture
def source_dir() -> Path:
    return Path("/a/source/directory/")


@fixture
def target_dir() -> Path:
    return Path("directory/")


@fixture
def default_institution() -> str:
    return "Test Institution"


@fixture
def old_acls() -> bool:
    return False


@fixture
def projects_enabled() -> bool:
    return True


@fixture
def objects_with_ids() -> List[str]:
    return [
        "dataset",
        "experiment",
        "facility",
        "instrument",
        "project",
        "institution",
    ]


@fixture
def filename() -> str:
    return "test_data.dat"


@fixture
def created_by_upi() -> str:
    return "upi001"


@fixture
def start_time_datetime(timezone: BaseTzInfo) -> datetime:
    return datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def start_time_str(start_time_datetime: datetime) -> str:
    return start_time_datetime.isoformat()


@fixture
def created_time_datetime(timezone: BaseTzInfo) -> datetime:
    return datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def created_time_str(start_time_datetime: datetime) -> str:
    return start_time_datetime.isoformat()


@fixture
def end_time_datetime(timezone: BaseTzInfo) -> datetime:
    return datetime(2001, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def end_time_str(end_time_datetime: datetime) -> str:
    return end_time_datetime.isoformat()


@fixture
def modified_time_datetime(timezone: BaseTzInfo) -> datetime:
    return datetime(2000, 7, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def modified_time_str(modified_time_datetime: datetime) -> str:
    return modified_time_datetime.isoformat()


@fixture
def embargo_time_datetime(timezone: BaseTzInfo) -> datetime:
    return datetime(2005, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def embargo_time_str(embargo_time_datetime: datetime) -> str:
    return embargo_time_datetime.isoformat()


@fixture
def admin_groups() -> List[str]:
    return [
        "Test_group_1",
        "Test_group_2",
    ]


@fixture
def read_groups() -> List[str]:
    return [
        "Test_group_11",
        "Test_group_12",
        "Test_group_13",
        "Test_group_14",
    ]


@fixture
def download_groups() -> List[str]:
    return [
        "Test_group_12",
        "Test_group_14",
        "Test_group_21",
        "Test_group_22",
    ]


@fixture
def sensitive_groups() -> List[str]:
    return [
        "Test_group_13",
        "Test_group_14",
        "Test_group_22",
        "Test_group_31",
    ]


@fixture
def admin_users() -> List[str]:
    return [
        "upi001",
        "upi002",
    ]


@fixture
def read_users() -> List[str]:
    return [
        "upi011",
        "upi012",
        "upi013",
        "upi014",
    ]


@fixture
def download_users() -> List[str]:
    return [
        "upi012",
        "upi014",
        "upi021",
        "upi022",
    ]


@fixture
def sensitive_users() -> List[str]:
    return [
        "upi013",
        "upi014",
        "upi022",
        "upi031",
    ]


@fixture
def project_name() -> str:
    return "Test_Project"


@fixture
def project_description() -> str:
    return "A test project for the purposes of testing"


@fixture
def project_ids() -> List[str]:
    return [
        "Project_1",
        "Test_Project",
        "Project_Test_1",
    ]


@fixture
def project_principal_investigator() -> str:
    return "upi001"


@fixture
def project_institutions() -> List[str]:
    return ["Test Institution"]


@fixture
def project_metadata() -> Dict[str, str]:
    return {
        "project_my_test_key_1": "Test Value",
        "project_my_test_key_2": "Test Value 2",
    }


@fixture
def project_schema() -> str:
    return "https://test-mytardis.nectar.acukland.ac.nz/Test/v1/Project"


@fixture
def project_metadata_processed(project_metadata: Dict[str, Any]) -> List[Parameter]:
    return sorted(
        [Parameter(name=key, value=project_metadata[key]) for key in project_metadata]
    )


@fixture
def project_url() -> str:
    return "http://myproject.com"


@fixture
def experiment_name() -> str:
    return "Test_Experiment"


@fixture
def experiment_description() -> str:
    return "A test experiment for the purposes of testing"


@fixture
def experiment_institution(default_institution: str) -> str:
    return default_institution


@fixture
def experiment_projects() -> List[str]:
    return [
        "Project_1",
        "Test_Project",
    ]


@fixture
def experiment_ids() -> List[str]:
    return [
        "Experiment_1",
        "Test_Experiment",
        "Experiment_Test_1",
    ]


@fixture
def experiment_metadata() -> Dict[str, str]:
    return {
        "experiment_my_test_key_1": "Test Value",
        "experiment_my_test_key_2": "Test Value 2",
    }


@fixture
def experiment_schema() -> str:
    return "https://test-mytardis.nectar.acukland.ac.nz/Test/v1/Experiment"


@fixture
def experiment_metadata_processed(
    experiment_metadata: Dict[str, Any]
) -> List[Parameter]:
    return sorted(
        [
            Parameter(name=key, value=experiment_metadata[key])
            for key in experiment_metadata
        ]
    )


@fixture
def experiment_url() -> str:
    return "http://myexperiment.com"


@fixture
def dataset_dir() -> Path:
    return Path("dataset/")


@fixture
def dataset_name() -> str:
    return "Test_Dataset"


@fixture
def dataset_description() -> str:
    return "This is a dataset"


@fixture
def dataset_experiments() -> List[str]:
    return [
        "Experiment_1",
        "Test_Experiment",
    ]


@fixture
def dataset_instrument() -> str:
    return "Test_Instrument"


@fixture
def dataset_ids() -> List[str]:
    return [
        "Dataset_1",
        "Test_Dataset",
        "Dataset_Test_1",
    ]


@fixture
def dataset_metadata() -> Dict[str, str]:
    return {
        "dataset_my_test_key_1": "Test Value",
        "dataset_my_test_key_2": "Test Value 2",
    }


@fixture
def dataset_schema() -> str:
    return "https://test-mytardis.nectar.acukland.ac.nz/Test/v1/Dataset"


@fixture
def dataset_metadata_processed(dataset_metadata: Dict[str, Any]) -> List[Parameter]:
    return sorted(
        [Parameter(name=key, value=dataset_metadata[key]) for key in dataset_metadata]
    )


@fixture
def datafile_md5sum() -> str:
    return "0d32909e86e422d04a053d1ba26a990e"


@fixture
def datafile_mimetype() -> str:
    return "text/plain"


@fixture
def datafile_size() -> int:
    return 52428800


@fixture
def datafile_dataset() -> str:
    return "Test_Dataset"


@fixture
def datafile_metadata() -> Dict[str, str]:
    return {
        "datafile_my_test_key_1": "Test Value",
        "datafile_my_test_key_2": "Test Value 2",
    }


@fixture
def datafile_schema() -> str:
    return "https://test-mytardis.nectar.auckland.ac.nz/Test/v1/Datafile"


@fixture
def datafile_metadata_processed(datafile_metadata: Dict[str, Any]) -> List[Parameter]:
    return sorted(
        [Parameter(name=key, value=datafile_metadata[key]) for key in datafile_metadata]
    )


@fixture
def instrument_ids() -> List[str]:
    return [
        "Instrument_1",
        "Test_Instrument",
        "Instrument_Test_1",
    ]


@fixture
def instrument_name() -> str:
    return "Test Instrument"


@fixture
def split_and_parse_users(
    admin_users: List[str],
    read_users: List[str],
    download_users: List[str],
    sensitive_users: List[str],
) -> List[UserACL]:
    return_list = [
        UserACL(
            user=Username(admin_user),
            is_owner=True,
            can_download=True,
            see_sensitive=True,
        )
        for admin_user in admin_users
    ]
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
    admin_groups: List[str],
    read_groups: List[str],
    download_groups: List[str],
    sensitive_groups: List[str],
) -> List[GroupACL]:
    return_list = [
        GroupACL(
            group=admin_group,
            is_owner=True,
            can_download=True,
            see_sensitive=True,
        )
        for admin_group in admin_groups
    ]
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
def directory_relative_to_storage_box(
    dataset_dir: Path,
    target_dir: Path,
    filename: str,
) -> Path:
    return Path(target_dir / dataset_dir / filename)


@fixture
def project_uri() -> str:
    return URI("/api/v1/project/1/")


@fixture
def experiment_uri() -> str:
    return URI("/api/v1/experiment/1/")


@fixture
def dataset_uri() -> str:
    return URI("/api/v1/dataset/1/")


@fixture
def datafile_uri() -> str:
    return URI("/api/v1/dataset_file/1002735/")


@fixture
def institution_uri() -> str:
    return URI("/api/v1/institution/1/")


@fixture
def instrument_uri() -> str:
    return URI("/api/v1/instrument/1/")


@fixture
def user_uri() -> str:
    return URI("/api/v1/user/1/")


@fixture
def institution_address() -> str:
    return "23 Symonds Street"


@fixture
def institution_ids() -> List[str]:
    return ["UoA123", "UoA", "Uni"]


@fixture
def institution_country() -> str:
    return "NZ"


@fixture
def institution_name() -> str:
    return "University of Auckland"


@fixture
def datafile_archive_date() -> ISODateTime:
    return ISODateTime("2023-12-31T12:00:00+13:00")


@fixture
def datafile_delete_date() -> ISODateTime:
    return ISODateTime("2025-12-31T12:00:00+13:00")


@fixture
def archive_class() -> StorageTypesEnum:
    return StorageTypesEnum.S3


@fixture
def storage_class() -> StorageTypesEnum:
    return StorageTypesEnum.FILE_SYSTEM


@fixture
def archive_box_name() -> str:
    return "Test archive box"


@fixture
def storage_box_name() -> str:
    return "Test_storage_box"


@fixture
def archive_box_uri() -> str:
    return "/api/v1/storagebox/2/"


@fixture
def storage_box_uri() -> str:
    return "/api/v1/storagebox/1/"


@fixture
def archive_box_description() -> str:
    return "A test archive box"


@fixture
def storage_box_description() -> str:
    return "A test storage box"


@fixture
def archive_options() -> Dict[str, str]:
    return {
        "S3_Key": "mykey",
        "S3_password": "mypassword",
    }


@fixture
def storage_options() -> Dict[str, str]:
    return {"Location": "/srv/test"}


@fixture
def archive_attributes() -> Dict[str, str]:
    return {"type": "tape"}


@fixture
def storage_attributes() -> Dict[str, str]:
    return {"type": "disk"}


@fixture
def datetime_now(timezone: BaseTzInfo) -> datetime:
    return datetime(2001, 1, 1, 12, 0, 0, tzinfo=timezone)


@fixture
def autoarchive_offset() -> int:
    return 547


@fixture
def delete_offset() -> int:
    return 5470


@fixture
def archive_box_dir() -> Path:
    return Path("/an/archive/target/")


@fixture
def storage_box_dir() -> Path:
    return Path("/an/active/target/")


@fixture
def archive_date(
    datetime_now: datetime,
    autoarchive_offset: int,
) -> datetime:
    return datetime_now + timedelta(days=autoarchive_offset)


@fixture
def delete_date(
    datetime_now: datetime,
    delete_offset: int,
) -> datetime:
    return datetime_now + timedelta(days=delete_offset)
