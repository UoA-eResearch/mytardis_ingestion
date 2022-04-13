from src.ingestion_factory import IngestionFactory
from src.smelters import YAMLSmelter


class PrintGroupIngestionFactory(IngestionFactory):
    """A specific implementation of the IngestionFactory tailored for Cris Print's group"""

    def __init__(self, config_dict):
        super().__init__(config_dict)

    def get_smelter(self, config_dict):
        smelter = YAMLSmelter(config_dict)
        return smelter
