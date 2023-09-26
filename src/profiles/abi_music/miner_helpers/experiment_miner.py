"""Mines experiment metadata
"""


# ---Imports
import copy
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from src.extraction_output_manager import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music import abi_music_consts as amc
from src.profiles.abi_music.miner_helpers import metadata_helpers as mh

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class ExperimentMiner:
    """Mines experiment metadata"""

    def __init__(self) -> None:
        pass

    def mine_experiment_metadata(
        self,
        path: str,
        dclass_struct: dict[str, Any],
        mappings: dict[str, dict[str, int | str | bool | float]],
        out_man: om.OutputManager,
    ) -> om.OutputManager:
        """
        Mine experiment metadata from a given dictionary of experiment keys and mappings.

        Args:
            path (str): Path of the experiment.
            dclass_struct (dict[str, Any]): Dictionary of experiment keys.
            mappings (dict[str, dict[str, int|str|bool|float]]): Mappings of experiment keys.
            out_man (om.OutputManager): OutputManager object.

        Returns:
            om.OutputManager: Updated OutputManager object with experiment metadata file written.
        """
        new_out_man = copy.deepcopy(out_man)
        for proj_key in dclass_struct.keys():
            for expt_key in dclass_struct[proj_key].keys():
                metadata = self._generate_experiment_metadata(
                    proj_key, expt_key, mappings
                )
                proj_path = Path(proj_key)
                f_name = Path(
                    expt_key + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE
                )
                fp = path / proj_path / f_name
                mh.write_metadata_file(fp, metadata)
                new_out_man.add_success_entry_to_dict(
                    fp, pc.PROCESS_MINER, "experiment metadata file written"
                )
                new_out_man.add_metadata_file_to_ingest(fp, pc.EXPERIMENT_NAME)
        return new_out_man

    def _generate_experiment_metadata(
        self,
        proj_key: str,
        expt_key: str,
        mappings: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate experiment metadata from a given project key, experiment key and mappings.

        Args:
            proj_key (str): Project key.
            expt_key (str): Experiment key.
            mappings (dict[str, Any]): Mappings of experiment keys.

        Returns:
            dict[str, Any]: Dictionary of experiment metadata.
        """
        metadata = {}
        req_keys = [key for key in mappings.keys() if mappings[key][pc.REQUIRED_KEY]]
        for req_key in req_keys:
            if mappings[req_key][pc.USEDEFAULT_KEY]:
                metadata[mappings[req_key][pc.NAME_KEY]] = mappings[req_key][
                    pc.DEFAULT_KEY
                ]
        metadata["title"] = expt_key
        metadata["description"] = expt_key
        metadata["projects"] = [proj_key]
        metadata = mh.add_schema_to_metadata(metadata)

        return metadata
