# pylint: disable=missing-module-docstring

from src.ingestion_factory import IngestionFactory
from src.smelters import YAMLSmelter


class YAMLIngestionFactory(IngestionFactory):
    """A specific implementation of the IngestionFactory tailored for YAML files"""

    def get_smelter(self, config_dict):
        """Function to get the specific Smelter subclass to use in this instance"""
        smelter = YAMLSmelter(config_dict)
        return smelter
