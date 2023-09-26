# pylint: disable=missing-function-docstring
"""Helpers used throughout a profile in the Extraction Plant
"""
from typing import Any, Dict, List

from src.helpers.constants import (
    DATACLASS_ENTRY_DICT_KEYS,
    DATACLASSES_LIST,
    KEY_IDX_CL,
    KEY_IDX_OP,
    KEY_LVL_SEP,
    OUTPUT_DICT_KEYS,
    OUTPUT_SUBDICT_KEYS,
)


def create_dataclass_entry_dict() -> Dict[str, Any]:
    return Dict.fromkeys(DATACLASS_ENTRY_DICT_KEYS)


def create_output_dict() -> Dict[str, Any]:
    return Dict.fromkeys(OUTPUT_DICT_KEYS)


def create_output_subdict() -> Dict[str, Any]:
    return Dict.fromkeys(OUTPUT_SUBDICT_KEYS)


def create_ingestion_dict() -> Dict[str, Any]:
    return Dict.fromkeys(DATACLASSES_LIST)


def format_field_for_key_seq(
    key_seq: List[str],
) -> str:
    frmtd_key = key_seq[0]
    for i in range(len(key_seq) - 1):
        frmtd_key += KEY_LVL_SEP + key_seq[i + 1]
    return frmtd_key


def format_key_for_indexed_field(
    key: str,
    idx: int,
) -> str:
    frmtd_key = key
    frmtd_key += KEY_IDX_OP + str(idx) + KEY_IDX_CL
    return frmtd_key
