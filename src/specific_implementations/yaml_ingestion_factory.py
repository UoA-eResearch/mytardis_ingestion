# pylint: disable=missing-module-docstring

from src.helpers.config import (
    MyTardisGeneral,
    MyTardisIntrospection,
    MyTardisSchema,
    MyTardisStorage,
)
from src.ingestion_factory import IngestionFactory
from src.smelters import YAMLSmelter


class YAMLIngestionFactory(IngestionFactory):
    """A specific implementation of the IngestionFactory tailored for YAML files"""

    def get_smelter(
        self,
        general: MyTardisGeneral,
        default_schema: MyTardisSchema,
        storage: MyTardisStorage,
        mytardis_setup: MyTardisIntrospection,
    ):
        """Function to get the specific Smelter subclass to use in this instance"""
        smelter = YAMLSmelter(general, default_schema, storage, mytardis_setup)
        return smelter
