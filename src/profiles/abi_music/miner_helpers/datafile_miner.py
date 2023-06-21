"""Mines datafile metadata
"""


# ---Imports
import copy
import json
import logging
import os
import yaml

from pathlib import Path
from src.profiles import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles import profile_helpers as ph
from src.miners.utils import datafile_metadata_helpers as dmh
from src.profiles.abi_music.miner_helpers import metadata_helpers as mh
from src.profiles.abi_music import abi_music_consts as amc
from typing import Optional, Any, Dict

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
        path: Path,
        dclass_struct: Dict[str, Any],
        mappings: Dict[str, Dict[str, int | str | bool | float]],
        out_man: om.OutputManager,
    ) -> om.OutputManager:
        """Mine metadata from a datafile.

        Args:
            path (Path): The path of the datafile.
            dclass_struct ([str, Any]): A dictionary containing datafile structure.
            mappings (Dict[str, Dict[str, int|str|bool|float]]): A dictionary containing the mappings to remap the fields.
            out_man (om.OutputManager): An output manager instance.

        Returns:
            om.OutputManager: An updated output manager instance.
        """
        new_out_man = copy.deepcopy(out_man)
        files_to_ignore = new_out_man.files_to_ignore
        files_to_ignore_lut = dict.fromkeys(files_to_ignore)

        for proj_key in dclass_struct.keys():
            proj_pth = Path(proj_key)
            for expt_key in dclass_struct[proj_key].keys():
                expt_pth = Path(expt_key)
                for dset_key in dclass_struct[proj_key][expt_key].keys():
                    dset_pth = Path(dset_key)
                    base_path = path / proj_pth / expt_pth / dset_pth
                    logger.info(f"mining files in: {base_path}")
                    for root, dirs, files in os.walk(base_path):
                        root_pth = Path(root)
                        rel_path = root_pth.relative_to(base_path)
                        for file in files:
                            fname = Path(file)
                            fp = root_pth / fname
                            if fp in files_to_ignore_lut:
                                continue
                            metadata = self._generate_datafile_metadata(root_pth, rel_path, dset_key, file)
                            fname = Path(file + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE)
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
        root_pth: Path,
        rel_path: Path,
        dset_key: str,
        fn: str,
    ) -> Dict[str, Any]:
        """Generate datafile metadata.

        Args:
            root (Path): The root path of the datafile.
            rel_path (Path): The relative path of the datafile.
            dset_key (str): The key for the dataset.
            fn (str): The filename of the datafile.

        Returns:
            Dict[str, Any]: A dictionary containing the generated metadata.
        """
        metadata: Dict[str, Any] = {}
        fp = root_pth / Path(fn)

        metadata["dataset"] = dset_key
        metadata["filename"] = fn
        metadata["directory"] = str(rel_path)
        metadata["md5sum"] = dmh.calculate_md5sum(fp)
        metadata["mimetype"] = dmh.determine_mimetype(fn)
        metadata["size"] = fp.stat().st_size
        metadata = mh.add_schema_to_metadata(metadata)

        return metadata