"""Generic JSON parser module. 

Main objective of this module is to parse in JSON metadata files
into the appropriate, raw dataclasses for further refining.
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
from src.parsers.custom_parsers import abi_json


# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class DatafileMap:
    def __init__(
        self,
        custom_parser: str,
    ) -> None:
        self.base_map = _BaseMap(custom_parser)


class _BaseMap:
    def __init__(
        self,
        custom_parser: str,
        dataclass: str,
    ) -> None:
        self.mapper = _KeyMapper()
        self.attr_mngr = _AttributeManager()
        logger.debug("compiling attrs")
        self.target_attrs = self.attr_mngr.compile_attrs([BaseDatafile, RawDatafile])
        self.custom_parser = self._determine_custom_parser(custom_parser, dataclass)
        logger.debug("custom_parser = {0}".format(type(self.custom_parser)))

    def get_custom_parser(
        self,
    ) -> any:
        return self.custom_parser

    def load_json(
        self,
        filepath: str,
    ) -> dict:
        data = {}
        try:
            with open(filepath) as f:
                data = json.load(f)
        except FileNotFoundError as e:
            raise e
        return data

    def _determine_custom_parser(
        self,
        map_type: str,
        dataclass: str,
    ) -> any:
        # For a new mapping format, add cases here
        if map_type == psr_consts.ABI_JSON_PARSER:
            parser_base = abi_json.Parser()
            return parser_base.get_parser(dataclass)
        else:
            logger.error("{0} not implemented".format(map_type))
            raise Exception("custom parser not yet supported")


class _KeyMapper:
    def __init__(
        self,
    ) -> None:
        pass

    def create_map(
        self,
        target_attr: str,
    ) -> dict:
        map_dict = dict.fromkeys(psr_consts.MAP_DICT_KEYS)
        map_dict[psr_consts.TARGET_KEY] = target_attr
        return map_dict

    def set_raw_attr(
        self,
        map_dict: dict,
        raw_attr: str,
    ) -> dict:
        modded_dict = map_dict.copy()
        modded_dict[psr_consts.RAW_KEY] = raw_attr
        return modded_dict


class _AttributeManager:
    def __init__(
        self,
    ) -> None:
        pass

    def compile_attrs(
        self,
        classes: list,
    ) -> list:
        attrs = []
        for current_class in classes:
            attrs.append(self._get_all_attrs_in_class(current_class))
        return attrs

    def _get_all_attrs_in_class(
        self,
        class_type: any,
    ) -> list:
        all_attrs = dir(class_type)
        dbl_und = "__"
        return [
            attr
            for attr in all_attrs
            if not (attr[0].startswith(dbl_und) and attr[0].endswith(dbl_und))
        ]
