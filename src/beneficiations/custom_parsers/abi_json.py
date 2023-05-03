"""ABI JSON parser module. 

Main objective of this module is to map the ABI json files to 
a format accepted by the mytardis_ingestion
"""

# ---Imports
import json
import logging
from pathlib import Path
from typing import Optional, Tuple

from pydantic import AnyUrl, ValidationError

from src.blueprints import (
    BaseDatafile,
    BaseDataset,
    BaseExperiment,
    BaseProject,
    RawDatafile,
    RawDataset,
    RawExperiment,
    RawProject,
)
from src.blueprints.common_models import Parameter
from src.helpers import (
    GeneralConfig,
    SchemaConfig,
    log_if_projects_disabled,
)
from src.parsers import parser_consts as psr_consts


# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class Parser:
    def __init__(
        self,
    ) -> None:
        pass

    def get_parser(self, dataclass) -> any:
        if dataclass == psr_consts.DATAFILE_CLASS_NAME:
            return DatafileParser()
        elif dataclass == psr_consts.DATASET_CLASS_NAME:
            return DatasetParser()
        elif dataclass == psr_consts.EXPERIMENT_CLASS_NAME:
            return ExperimentParser()
        elif dataclass == psr_consts.PROJECT_CLASS_NAME:
            return ProjectParser()
        else:
            logger.error("{0} dataclass not supported".format(dataclass))
            raise Exception("{0} dataclass not supported".format(dataclass))


class DatafileParser:
    def __init__(
        self,
    ) -> None:
        pass


class DatasetParser:
    def __init__(
        self,
    ) -> None:
        pass


class ExperimentParser:
    def __init__(
        self,
    ) -> None:
        pass


class ProjectParser:
    def __init__(
        self,
    ) -> None:
        pass
