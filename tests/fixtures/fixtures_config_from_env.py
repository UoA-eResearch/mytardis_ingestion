# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=missing-module-docstring

from pathlib import Path
from typing import Any, Dict, List

from pytest import fixture

from src.config.config import (
    AuthConfig,
    ConfigFromEnv,
    ConnectionConfig,
    GeneralConfig,
    IntrospectionConfig,
    ProxyConfig,
    SchemaConfig,
    StorageBoxConfig,
    StorageConfig,
    StorageTypesEnum,
)


@fixture
def archive_class() -> StorageTypesEnum:
    return StorageTypesEnum.S3


@fixture
def storage_class() -> StorageTypesEnum:
    return StorageTypesEnum.FILE_SYSTEM


@fixture
def archive_box_name() -> str:
    return "Test_archive_box"


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
        "bucket": "my_test_bucket",
    }


@fixture
def storage_options() -> Dict[str, str]:
    return {
        "Location": "/srv/test",
        "target_root_directory": "/srv/mytardis/mytest",
    }


@fixture
def archive_attributes() -> Dict[str, str]:
    return {"type": "tape"}


@fixture
def storage_attributes() -> Dict[str, str]:
    return {"type": "disk"}


@fixture
def active_store(
    storage_box_name: str,
    storage_class: StorageTypesEnum,
    storage_options: Dict[str, str],
    storage_attributes: Dict[str, str],
) -> StorageBoxConfig:
    return StorageBoxConfig(
        storage_name=storage_box_name,
        storage_class=storage_class,
        options=storage_options,
        attributes=storage_attributes,
    )


@fixture
def archive_store(
    archive_box_name: str,
    archive_class: StorageTypesEnum,
    archive_options: Dict[str, Any],
    archive_attributes: Dict[str, Any],
) -> StorageBoxConfig:
    return StorageBoxConfig(
        storage_name=archive_box_name,
        storage_class=archive_class,
        options=archive_options,
        attributes=archive_attributes,
    )


@fixture
def storage(
    active_store: StorageBoxConfig,
    archive_store: StorageBoxConfig,
) -> StorageConfig:
    return StorageConfig(
        active_stores=[active_store],
        archives=[archive_store],
    )


@fixture
def processed_introspection_response() -> Dict[str, bool | List[str]]:
    return {
        "old_acls": False,
        "projects_enabled": True,
        "objects_with_ids": [
            "dataset",
            "experiment",
            "facility",
            "instrument",
            "project",
            "institution",
        ],
    }


@fixture
def general(
    default_institution: str,
    source_dir: Path,
) -> GeneralConfig:
    return GeneralConfig(
        default_institution=default_institution,
        source_directory=source_dir,
    )


@fixture
def auth(
    username: str,
    api_key: str,
) -> AuthConfig:
    return AuthConfig(username=username, api_key=api_key)


@fixture
def connection(
    hostname: str,
    proxies: Dict[str, str],
    verify_certificate: bool,
) -> ConnectionConfig:
    return ConnectionConfig(
        hostname=hostname,
        proxy=ProxyConfig(http=proxies["http"], https=proxies["https"]),
        verify_certificate=verify_certificate,
    )


@fixture
def active_stores(
    storage_class: StorageTypesEnum,
    storage_options: Dict[str, str],
    storage_attributes: Dict[str, str],
) -> StorageBoxConfig:
    return StorageBoxConfig(
        storage_name="Test Active",
        storage_class=storage_class,
        options=storage_options,
        attributes=storage_attributes,
    )


@fixture
def archive(
    archive_class: StorageTypesEnum,
    archive_options: Dict[str, Any],
    archive_attributes: Dict[str, Any],
) -> StorageConfig:
    return StorageBoxConfig(
        storage_name="Test Archive",
        storage_class=archive_class,
        options=archive_options,
        attributes=archive_attributes,
    )


@fixture
def storage(
    active_stores: StorageBoxConfig,
    archive: StorageBoxConfig,
) -> StorageConfig:
    return StorageConfig(active_stores=[active_stores], archives=[archive])


@fixture
def default_schema(
    project_schema: str,
    experiment_schema: str,
    dataset_schema: str,
    datafile_schema: str,
) -> SchemaConfig:
    return SchemaConfig(
        project=project_schema,
        experiment=experiment_schema,
        dataset=dataset_schema,
        datafile=datafile_schema,
    )


@fixture
def mytardis_setup(
    processed_introspection_response: Dict[str, bool | List[str]],
) -> IntrospectionConfig:
    return IntrospectionConfig(
        old_acls=processed_introspection_response["old_acls"],
        projects_enabled=processed_introspection_response["projects_enabled"],
        objects_with_ids=processed_introspection_response["objects_with_ids"],
    )


@fixture
def mytardis_settings(
    general: GeneralConfig,
    auth: AuthConfig,
    connection: ConnectionConfig,
    active_store: StorageConfig,
    default_schema: SchemaConfig,
) -> ConfigFromEnv:
    return ConfigFromEnv(
        general=general,
        auth=auth,
        connection=connection,
        storage=active_store,
        default_schema=default_schema,
    )
