"""Mines dataset metadata
"""


# ---Imports
import copy
import json
import logging

from pathlib import Path
from src.profiles import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music.miner_helpers import metadata_helpers as mh
from src.profiles.abi_music import abi_music_consts as amc
from typing import Optional, Any, Dict

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class DatasetMiner:
    """Mines dataset metadata"""

    def __init__(self) -> None:
        pass

    def mine_dataset_metadata(
        self,
        path: Path,
        dclass_struct: Dict[str, Any],
        mappings: Dict[str, Any],
        out_man: om.OutputManager,
    ) -> om.OutputManager:
        """
        Mine dataset metadata from a given dictionary of dataset keys and mappings.

        Args:
            path (Path): Path of the dataset.
            dclass_struct (Dict[str, Any]): Dictionary of dataset keys.
            mappings (Dict[str, Any]): Mappings of dataset keys.
            out_man (om.OutputManager): OutputManager object.

        Returns:
            om.OutputManager: Updated OutputManager object with dataset metadata file written.
        """
        new_out_man = copy.deepcopy(out_man)
        for proj_key in dclass_struct.keys():
            proj_pth = Path(proj_key)
            for expt_key in dclass_struct[proj_key].keys():
                expt_pth = Path(expt_key)
                for dset_key in dclass_struct[proj_key][expt_key].keys():
                    dset_pth = Path(dset_key)
                    fname = Path(dset_key + amc.METADATA_FILE_TYPE)
                    fp = path / proj_pth / expt_pth / dset_pth / fname
                    dset_metadata_fp = fp
                    with dset_metadata_fp.open("r") as f:
                        dset_metadata = json.load(f)
                    config_key = "config"
                    if config_key in dset_metadata:
                        preproc_dset_metadata = {}
                        for con_key in dset_metadata[config_key].keys():
                            preproc_dset_metadata[con_key] = dset_metadata[config_key][con_key]
                        for key in dset_metadata.keys():
                            if key == config_key:
                                continue
                            preproc_dset_metadata[key] = dset_metadata[key]
                        dset_metadata = preproc_dset_metadata.copy()
                    flat_dset_metadata = self._flatten_dataset_dict(dset_metadata)
                    remapped_metadata = self._remap_dataset_fields(
                        flat_dset_metadata, mappings
                    )
                    remapped_metadata = mh.add_schema_to_metadata(remapped_metadata)
                    fname = Path(dset_key + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE)
                    fp = path / proj_pth / expt_pth / fname
                    mh.write_metadata_file(fp, remapped_metadata)
                    new_out_man.add_success_entry_to_dict(
                        fp, pc.PROCESS_MINER, "dataset metadata file written"
                    )
                    new_out_man.add_metadata_file_to_ingest(fp, pc.DATASET_NAME)
        return new_out_man

    def _remap_dataset_fields(
        self,
        flattened_dict: Dict[str, str | int | float],
        mappings: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Remap a dataset fields according to a set of mappings.

        Args:
            flattened_dict (Dict[str, str|int|float]): A dictionary of fields to remap.
            mappings (Dict[str, Any]): A dictionary containing the mappings to remap the fields.

        Returns:
            Dict[str, Any]: A dictionary of the remapped fields.
        """
        remapped_dict = {}
        req_keys = [key for key in mappings.keys() if mappings[key][pc.REQUIRED_KEY]]
        other_keys = [
            key for key in mappings.keys() if not mappings[key][pc.REQUIRED_KEY]
        ]
        for req_key in req_keys:
            if mappings[req_key][pc.USEDEFAULT_KEY] and req_key not in flattened_dict:
                remapped_dict[mappings[req_key][pc.NAME_KEY]] = mappings[req_key][
                    pc.DEFAULT_KEY
                ]
            else:
                remapped_dict[mappings[req_key][pc.NAME_KEY]] = flattened_dict[req_key]

        for key in other_keys:
            if mappings[key][pc.USEDEFAULT_KEY] and key not in flattened_dict:
                remapped_dict[mappings[key][pc.NAME_KEY]] = mappings[key][
                    pc.DEFAULT_KEY
                ]

        param_sets = {}
        for key in flattened_dict.keys():
            if key not in remapped_dict and key not in mappings:
                param_sets[key] = flattened_dict[key]
                # remapped_dict[key] = flattened_dict[key]
                if key == "SequenceID":
                    remapped_dict["identifiers"] = [flattened_dict[key]]
        param_sets["summary"] = flattened_dict["Description"]

        remapped_dict["experiments"] = [remapped_dict["experiments"]]
        remapped_dict["metadata"] = param_sets

        dict_keys = list(remapped_dict.keys())
        dict_keys.sort()
        remapped_dict = {i: remapped_dict[i] for i in dict_keys}

        return remapped_dict

    def _flatten_dataset_dict(
        self,
        d: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Flatten a dataset dictionary.

        Args:
            d (Dict[str, Any]): A dictionary to flatten.

        Returns:
            Dict[str, Any]: A flattened dictionary.
        """
        flat_dset_metadata = {}
        for key, value in d.items():
            if isinstance(value, dict):
                nested = self._flatten_dataset_dict(value)
                for nested_key, nested_value in nested.items():
                    frmtd_key = key + pc.KEY_LVL_SEP + nested_key
                    flat_dset_metadata[frmtd_key] = nested_value
            elif isinstance(value, list):
                for idx, elem in enumerate(value):
                    if isinstance(elem, dict):
                        nested = self._flatten_dataset_dict(elem)
                        for nested_key, nested_value in nested.items():
                            frmtd_key = (
                                key
                                + pc.KEY_IDX_OP
                                + str(idx)
                                + pc.KEY_IDX_CL
                                + pc.KEY_LVL_SEP
                                + nested_key
                            )
                            flat_dset_metadata[frmtd_key] = nested_value
                    else:
                        frmtd_key = key + pc.KEY_IDX_OP + str(idx) + pc.KEY_IDX_CL
                        flat_dset_metadata[frmtd_key] = elem
            else:
                flat_dset_metadata[key] = value

        return flat_dset_metadata