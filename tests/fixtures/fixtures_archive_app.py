# pylint: disable=missing-function-docstring
"""Fixtures specific to the archive in X days app
"""

from pytest import fixture

from src.blueprints.project import ProjectFileSystemStorageBox


@fixture
def archive_in_days() -> int:
    return 365


@fixture
def delete_in_days() -> int:
    return -1

@fixture
def project_active_store() -> ProjectFileSystemStorageBox:
    return ProjectFileSystemStorageBox(
        name=
        storage_class=
        options = <- REDUCED OPTIONS
        delete_in_days=delete_in_days,
        archive_in_days=archive_in_days,
        target_directory= <- GET THIS FROM SMELTING OPTIONS
    )