from pytest import fixture
import responses

from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter
from src.forges.forge import Forge
from src.crucible.crucible import Crucible
from src.ingestion_factory import IngestionFactory
from src.helpers.config import (
    AuthConfig,
    ConnectionConfig,
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
    ConfigFromEnv,
)
from src.helpers.mt_rest import MyTardisRESTFactory
from tests.fixtures.mock_rest_factory import MockMtRest


@fixture
def rest_factory(auth: AuthConfig, connection: ConnectionConfig):
    return MyTardisRESTFactory(auth, connection)


@fixture
def overseer(rest_factory: MyTardisRESTFactory, mytardis_setup: IntrospectionConfig):
    overseer = Overseer(rest_factory)
    overseer._mytardis_setup = mytardis_setup
    return overseer


@fixture
def mock_overseer(mock_mt_rest: MockMtRest):
    def _get_mock_overseer(mock_responses: responses.RequestsMock):
        return Overseer(mock_mt_rest(mock_responses))

    return _get_mock_overseer


@fixture
def smelter(
    overseer: Overseer,
    general: GeneralConfig,
    default_schema: SchemaConfig,
    storage: StorageConfig,
):
    return Smelter(overseer, general, default_schema, storage)


@fixture
def forge(rest_factory: MyTardisRESTFactory):
    return Forge(rest_factory)


@fixture
def crucible(overseer: Overseer):
    return Crucible(overseer)


@fixture
def factory(
    mytardis_settings: ConfigFromEnv,
    rest_factory: MyTardisRESTFactory,
    overseer: Overseer,
    smelter: Smelter,
    forge: Forge,
    crucible: Crucible,
):
    return IngestionFactory(
        config=mytardis_settings,
        mt_rest=rest_factory,
        overseer=overseer,
        smelter=smelter,
        forge=forge,
        crucible=crucible,
    )