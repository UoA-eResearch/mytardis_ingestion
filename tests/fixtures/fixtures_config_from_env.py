# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=missing-module-docstring

from typing import Dict

from pytest import fixture

from src.blueprints.storage_boxes import StorageTypesEnum
from src.config.config import (
    AuthConfig,
    ConfigFromEnv,
    ConnectionConfig,
    GeneralConfig,
    ProxyConfig,
    SchemaConfig,
)


@fixture
def storage_class() -> StorageTypesEnum:
    return StorageTypesEnum.FILE_SYSTEM


@fixture
def storage_box_name() -> str:
    return "Test_storage_box"


@fixture
def storage_box_uri() -> str:
    return "/api/v1/storagebox/1/"


@fixture
def storage_box_description() -> str:
    return "A test storage box"


@fixture
def storage_options() -> Dict[str, str]:
    return {
        "Location": "/srv/test",
        "target_root_directory": "/srv/mytardis/mytest",
    }


@fixture
def storage_attributes() -> Dict[str, str]:
    return {"type": "disk"}


@fixture
def general(
    default_institution: str,
) -> GeneralConfig:
    return GeneralConfig(
        default_institution=default_institution,
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
def mytardis_settings(
    general: GeneralConfig,
    auth: AuthConfig,
    connection: ConnectionConfig,
    default_schema: SchemaConfig,
) -> ConfigFromEnv:
    return ConfigFromEnv(
        general=general,
        auth=auth,
        connection=connection,
        default_schema=default_schema,
    )
