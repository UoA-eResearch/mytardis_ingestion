"""Mines project metadata
"""


# ---Imports
import copy
import logging
import os

from src.profiles import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music.miner_helpers import metadata_helpers as mh
from src.profiles.abi_music import abi_music_consts as amc
from typing import Optional, Any

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class ProjectMiner:
    """Mines project metadata"""
    def __init__(self) -> None:
        pass

    def mine_project_metadata(
        self,
        path: str,
        dclass_struct: dict[str, Any],
        mappings: dict[str, dict[str, int | str | bool | float]],
        out_man: om.OutputManager,
    ) -> om.OutputManager:
        """
        Mine project metadata from a given dictionary of project keys and mappings.

        Args:
            path (str): Path of the project.
            dclass_struct (dict[str, Any]): Dictionary of project keys.
            mappings (dict[str, dict[str, int|str|bool|float]]): Mappings of project keys.
            out_man (om.OutputManager): OutputManager object.

        Returns:
            om.OutputManager: Updated OutputManager object with project metadata file written.
        """
        new_out_man = copy.deepcopy(out_man)
        for proj_key in dclass_struct.keys():
            metadata = self._generate_project_metadata(proj_key, mappings)
            fp = os.path.join(
                path, proj_key + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE
            )
            mh.write_metadata_file(fp, metadata)
            new_out_man.add_success_entry_to_dict(
                fp, pc.PROCESS_MINER, "project metadata file written"
            )
            new_out_man.add_metadata_file_to_ingest(fp, pc.PROJECT_NAME)
        return new_out_man

    def _generate_project_metadata(
        self,
        proj_key: str,
        mappings: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate project metadata from a given project key and mappings.

        Args:
            proj_key (str): Project key.
            mappings (dict[str, Any]): Mappings of project keys.

        Returns:
            dict[str, Any]: Dictionary of project metadata.
        """
        metadata: dict[str, Any] = {}
        req_keys = [key for key in mappings.keys() if mappings[key][pc.REQUIRED_KEY]]
        for req_key in req_keys:
            if mappings[req_key][pc.USEDEFAULT_KEY]:
                metadata[mappings[req_key][pc.NAME_KEY]] = mappings[req_key][
                    pc.DEFAULT_KEY
                ]
        metadata["principal_investigator"] = "gsan005"
        metadata["name"] = proj_key
        metadata["description"] = proj_key
        metadata = mh.add_schema_to_metadata(metadata)

        return metadata