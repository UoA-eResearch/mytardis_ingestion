# pylint: disable-all
# type: ignore
# noqa
# nosec


"""Mines datafile metadata
"""


# ---Imports
import copy
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.extraction_output_manager import output_manager as om
from src.miners.utils import datafile_metadata_helpers as dmh
from src.profiles import profile_consts as pc
from src.profiles import profile_helpers as ph
from src.profiles.abi_music import abi_music_consts as amc
from src.profiles.abi_music.miner_helpers import metadata_helpers as mh

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class DatafileMiner:
    """Mines datafile metadata"""

    def __init__(self) -> None:
        pass

    def mine_datafile_metadata(
        self,
        path: str,
        dclass_struct: dict[str, Any],
        mappings: dict[str, dict[str, int | str | bool | float]],
        out_man: om.OutputManager,
    ) -> om.OutputManager:
        """Mine metadata from a datafile.

        Args:
            path (str): The path of the datafile.
            dclass_struct ([str, Any]): A dictionary containing datafile structure.
            mappings (dict[str, dict[str, int|str|bool|float]]): A dictionary containing the mappings to remap the fields.
            out_man (om.OutputManager): An output manager instance.

        Returns:
            om.OutputManager: An updated output manager instance.
        """
        new_out_man = copy.deepcopy(out_man)
        files_to_ignore = new_out_man.files_to_ignore
        files_to_ignore_lut = dict.fromkeys(files_to_ignore)

        for proj_key in dclass_struct.keys():
            for expt_key in dclass_struct[proj_key].keys():
                for dset_key in dclass_struct[proj_key][expt_key].keys():
                    base_path = os.path.join(path, proj_key, expt_key, dset_key)
                    logger.info("mining files in: {0}".format(base_path))
                    for root, dirs, files in os.walk(base_path):
                        rel_path = os.path.relpath(root, base_path)
                        for file in files:
                            if os.path.join(root, file) in files_to_ignore_lut:
                                continue
                            metadata = self._generate_datafile_metadata(
                                root_pth, rel_path, dset_key, file
                            )
                            fname = Path(
                                file + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE
                            )
                            fp = root_pth / fname
                            mh.write_metadata_file(fp, metadata)
                            new_out_man.add_success_entry_to_dict(
                                fp, pc.PROCESS_MINER, "dataset metadata file written"
                            )
                            new_out_man.add_metadata_file_to_ingest(
                                fp, pc.DATAFILE_NAME
                            )
        return new_out_man

    def _generate_datafile_metadata(
        self,
        root: str,
        rel_path: str,
        dset_key: str,
        fn: str,
    ) -> dict[str, Any]:
        """Generate datafile metadata.

        Args:
            root (str): The root path of the datafile.
            rel_path (str): The relative path of the datafile.
            dset_key (str): The key for the dataset.
            fn (str): The filename of the datafile.

        Returns:
            dict[str, Any]: A dictionary containing the generated metadata.
        """
        metadata: dict[str, Any] = {}
        fp = os.path.join(root, fn)

        metadata["dataset"] = dset_key
        metadata["filename"] = fn
        metadata["directory"] = str(rel_path)
        metadata["md5sum"] = dmh.calculate_md5sum(fp)
        mtype = dmh.determine_mimetype(fn)
        if not mtype:
            mtype = fn.split(".")[-1]
        metadata["mimetype"] = mtype
        metadata["size"] = os.path.getsize(fp)
        metadata = mh.add_schema_to_metadata(metadata)

        return metadata
