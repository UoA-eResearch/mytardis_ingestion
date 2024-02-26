# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring
from pathlib import Path
from typing import Any, Callable

import responses
from pytest import fixture

from src.blueprints.storage_boxes import StorageTypesEnum
from src.config.config import (
    AuthConfig,
    ConfigFromEnv,
    ConnectionConfig,
    FilesystemStorageBoxConfig,
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageBoxConfig,
)
from src.conveyor.conveyor import Conveyor
from src.crucible.crucible import Crucible
from src.forges.forge import Forge
from src.ingestion_factory import IngestionFactory
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter
from tests.fixtures.mock_rest_factory import MockMtRest


@fixture
def storage() -> StorageBoxConfig:
    return FilesystemStorageBoxConfig(storage_name="unix_fs", target_root_dir=Path("."))


@fixture
def rest_factory(
    auth: AuthConfig,
    connection: ConnectionConfig,
) -> MyTardisRESTFactory:
    return MyTardisRESTFactory(auth, connection)


@fixture
def overseer(
    rest_factory: MyTardisRESTFactory,
    mytardis_setup: IntrospectionConfig,
) -> Overseer:
    overseer = Overseer(rest_factory)
    overseer._mytardis_setup = mytardis_setup  # pylint: disable=W0212
    return overseer


@fixture
def mock_overseer(
    mock_mt_rest: MockMtRest,
) -> Callable[[responses.RequestsMock], Any]:
    def _get_mock_overseer(mock_responses: responses.RequestsMock) -> Overseer:
        return Overseer(mock_mt_rest(mock_responses))  # type: ignore[operator]

    return _get_mock_overseer


@fixture
def smelter(
    overseer: Overseer,
    general: GeneralConfig,
    default_schema: SchemaConfig,
) -> Smelter:
    return Smelter(overseer, general, default_schema)


@fixture
def forge(
    rest_factory: MyTardisRESTFactory,
) -> Forge:
    return Forge(rest_factory)


@fixture
def crucible(overseer: Overseer) -> Crucible:
    return Crucible(overseer=overseer)


@fixture
def conveyor(storage: FilesystemStorageBoxConfig) -> Conveyor:
    return Conveyor(storage)


@fixture
def factory(
    mytardis_settings: ConfigFromEnv,
    rest_factory: MyTardisRESTFactory,
    overseer: Overseer,
    smelter: Smelter,
    forge: Forge,
    crucible: Crucible,
    conveyor: Conveyor,
) -> IngestionFactory:
    return IngestionFactory(
        config=mytardis_settings,
        mt_rest=rest_factory,
        overseer=overseer,
        smelter=smelter,
        forge=forge,
        crucible=crucible,
        conveyor=conveyor,
    )
