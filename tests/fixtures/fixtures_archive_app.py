# pylint: disable=missing-function-docstring,redefined-outer-name
"""Fixtures specific to the archive in X days app
"""

from pathlib import Path
from typing import Dict

from pytest import fixture
from slugify import slugify

from src.blueprints.storage_boxes import StorageTypesEnum


@fixture
def archive_in_days() -> int:
    return 365


@fixture
def delete_in_days() -> int:
    return -1


@fixture
def archive_options() -> Dict[str, str]:
    return {
        "s3_Key": "mykey",
        "s3_password": "mypassword",
        "s3_bucket": "my_test_bucket",
    }


@fixture
def archive_options_reduced() -> Dict[str, str]:
    return {
        "s3_Key": "mykey",
        "s3_password": "mypassword",
    }


@fixture
def storage_options(target_dir: Path) -> Dict[str, str | Path]:
    return {
        "source_dir": "/srv/test",
        "target_root_directory": target_dir,
    }


@fixture
def storage_options_reduced() -> Dict[str, str]:
    return {
        "source_dir": "/srv/test",
    }


@fixture
def target_dir() -> Path:
    return Path("/srv/mytardis/mytest")


@fixture
def s3_bucket() -> str:
    return "my_test_bucket"


@fixture
def target_key(project_name: str) -> str:
    return slugify(project_name)


@fixture
def project_storage_box_name(
    project_name: str,
    storage_box_name: str,
) -> str:
    return f"{slugify(project_name)}-{slugify(storage_box_name)}"


@fixture
def project_archive_box_name(
    project_name: str,
    archive_box_name: str,
) -> str:
    return f"{slugify(project_name)}-{slugify(archive_box_name)}"
