# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring
from typing import Any, Callable

import responses
from pytest import fixture

from src.config.config import (
    AuthConfig,
    ConfigFromEnv,
    ConnectionConfig,
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
)
from src.crucible.crucible import Crucible
from src.forges.forge import Forge
from src.helpers.mt_rest import MyTardisRESTFactory
from src.ingestion_factory import IngestionFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter
from tests.fixtures.mock_rest_factory import MockMtRest


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
    storage: StorageConfig,
) -> Smelter:
    return Smelter(overseer, general, default_schema, storage)


@fixture
def forge(
    rest_factory: MyTardisRESTFactory,
) -> Forge:
    return Forge(rest_factory)


@fixture
def crucible(
    overseer: Overseer,
    active_store: StorageConfig,
    archive: StorageConfig,
) -> Crucible:
    return Crucible(
        overseer=overseer,
        active_stores=active_store,
        archive=archive,
    )


@fixture
def factory(
    mytardis_settings: ConfigFromEnv,
    rest_factory: MyTardisRESTFactory,
    overseer: Overseer,
    smelter: Smelter,
    forge: Forge,
    crucible: Crucible,
) -> IngestionFactory:
    return IngestionFactory(
        config=mytardis_settings,
        mt_rest=rest_factory,
        overseer=overseer,
        smelter=smelter,
        forge=forge,
        crucible=crucible,
    )
