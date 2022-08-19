from pytest import fixture
from src.blueprints.storage_boxes import StorageBox

from src.overseers.overseer import Overseer
from src.overseers.inspector import Inspector
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
)
from src.helpers.mt_rest import MyTardisRESTFactory


@fixture
def rest_factory(auth: AuthConfig, connection: ConnectionConfig):
    return MyTardisRESTFactory(auth, connection)


@fixture
def overseer(rest_factory: MyTardisRESTFactory, mytardis_setup: IntrospectionConfig):
    return Overseer(rest_factory, mytardis_setup)


@fixture
def smelter(
    general: GeneralConfig,
    default_schema: SchemaConfig,
    storage_box: StorageBox,
    mytardis_setup: IntrospectionConfig,
):
    return Smelter(general, default_schema, storage_box, mytardis_setup)


@fixture
def forge(rest_factory: MyTardisRESTFactory):
    return Forge(rest_factory)


@fixture
def crucible(overseer: Overseer, mytardis_setup: IntrospectionConfig):
    return Crucible(overseer, mytardis_setup)


@fixture
def inspector(overseer: Overseer):
    return Inspector(overseer)


@fixture
def factory(
    smelter: Smelter,
    inspector: Inspector,
    forge: Forge,
    overseer: Overseer,
    crucible: Crucible,
):
    return IngestionFactory(smelter, inspector, forge, overseer, crucible)
