# pylint: disable=fixme

"""Functions to generate project specific storage boxes depending on type
"""

from pathlib import Path

from slugify import slugify

from src.blueprints.project import (
    ProjectFileSystemStorageBox,
    ProjectS3StorageBox,
    ProjectStorageBox,
)
from src.blueprints.storage_boxes import StorageTypesEnum
from src.config.config import StorageBoxConfig


def create_storage_box(
    project_name: str,
    storage_config: StorageBoxConfig,
    delete_in_days: int = -1,
    archive_in_days: int = 365,
) -> ProjectStorageBox:
    """Function to determine the type of storage box and run the type specific
    function to create the storage box

    Args:
        project_name (str): the project name
        storage_config (StorageBoxConfig): The storage box config from the environment
        delete_in_days (int): The number of days after creation that the data can be
            considered for deletion
        archive_in_days (int): The number of days after creation that the data can be
            removed from active storage

    Returns:
        ProjectStorageBox: a project specific storage box
    """
    if storage_config.storage_class == StorageTypesEnum.FILE_SYSTEM:
        return _create_file_system_storage_box(
            project_name=project_name,
            storage_config=storage_config,
            delete_in_days=delete_in_days,
            archive_in_days=archive_in_days,
        )
    if storage_config.storage_class == StorageTypesEnum.S3:
        return _create_s3_storage_box(
            project_name=project_name,
            storage_config=storage_config,
            delete_in_days=delete_in_days,
            archive_in_days=archive_in_days,
        )
    raise NotImplementedError(
        f"Storage of {storage_config.storage_class} has not been implemented"
    )


def _create_file_system_storage_box(
    project_name: str,
    storage_config: StorageBoxConfig,
    delete_in_days: int,
    archive_in_days: int,
) -> ProjectFileSystemStorageBox:
    """Function to create a file system specific storage box

    Args:
        project_name (str): the project name
        storage_config (StorageBoxConfig): The storage box config from the environment
        delete_in_days (int): The number of days after creation that the data can be
            considered for deletion
        archive_in_days (int): The number of days after creation that the data can be
            removed from active storage

    Returns:
        ProjectFileSystemStorageBox: a project specific file system storage box
    """
    if not storage_config.options:
        raise ValueError("Poorly defined config for a file system storage box")
    try:
        target_root_directory = storage_config.options.pop("target_root_directory")
    except KeyError as err:
        # TODO: Log and continue
        raise err
    if isinstance(target_root_directory, Path):
        target_root_directory = target_root_directory.as_posix()
    target_dir = Path(f"{target_root_directory}/{slugify(project_name)}")
    storage_box_name = f"{slugify(project_name)}-{slugify(storage_config.storage_name)}"
    options = storage_config.options or None
    attributes = storage_config.attributes or None
    return ProjectFileSystemStorageBox(
        name=storage_box_name,
        storage_class=StorageTypesEnum.FILE_SYSTEM,
        target_directory=target_dir,
        options=options,
        attributes=attributes,
        delete_in_days=delete_in_days,
        archive_in_days=archive_in_days,
    )


def _create_s3_storage_box(
    project_name: str,
    storage_config: StorageBoxConfig,
    delete_in_days: int,
    archive_in_days: int,
) -> ProjectS3StorageBox:
    """Function to create a file system specific storage box

    Args:
        project_name (str): the project name
        storage_config (StorageBoxConfig): The storage box config from the environment
        delete_in_days (int): The number of days after creation that the data can be
            considered for deletion
        archive_in_days (int): The number of days after creation that the data can be
            removed from active storage

    Returns:
        ProjectS3StorageBox: a project specific s3 storage box
    """
    if not storage_config.options:
        raise ValueError("Poorly defined config for a S3 storage box")
    try:
        bucket = storage_config.options.pop("s3_bucket")
    except KeyError as err:
        # TODO: Log and continue
        raise err
    storage_box_name = f"{slugify(project_name)}-{slugify(storage_config.storage_name)}"
    options = storage_config.options or None
    attributes = storage_config.attributes or None
    return ProjectS3StorageBox(
        name=storage_box_name,
        storage_class=StorageTypesEnum.S3,
        bucket=bucket,
        target_key=f"{slugify(project_name)}",
        options=options,
        attributes=attributes,
        delete_in_days=delete_in_days,
        archive_in_days=archive_in_days,
    )
