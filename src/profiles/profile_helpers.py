"""Helpers used throughout a profile in the Extraction Plant 
"""
from src.profiles import profile_consts as pc
from typing import Any, Dict, List


def create_dataclass_entry_dict() -> Dict[str, Any]:
    return Dict.fromkeys(pc.DATACLASS_ENTRY_DICT_KEYS)


def create_output_dict() -> Dict[str, Any]:
    return Dict.fromkeys(pc.OUTPUT_DICT_KEYS)


def create_output_subdict() -> Dict[str, Any]:
    return Dict.fromkeys(pc.OUTPUT_SUBDICT_KEYS)


def create_ingestion_dict() -> Dict[str, Any]:
    return Dict.fromkeys(pc.DATACLASSES_LIST)


def format_field_for_key_seq(
    key_seq: List[str],
) -> str:
    frmtd_key = key_seq[0]
    for i in range(len(key_seq) - 1):
        frmtd_key += pc.KEY_LVL_SEP + key_seq[i + 1]
    return frmtd_key


def format_key_for_indexed_field(
    key: str,
    idx: int,
) -> str:
    frmtd_key = key
    frmtd_key += pc.KEY_IDX_OP + str(idx) + pc.KEY_IDX_CL
    return frmtd_key