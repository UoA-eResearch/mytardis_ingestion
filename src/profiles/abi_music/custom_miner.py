"""Defines the methodology to convert the source metadata to a beneficiable
format on a path.
"""


# ---Imports
import copy
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.extraction_output_manager import output_manager as om
from src.profiles.abi_music.miner_helpers.dataclass_identifier import (
    DataclassIdentifier,
)
from src.profiles.abi_music.miner_helpers.datafile_miner import DatafileMiner
from src.profiles.abi_music.miner_helpers.dataset_miner import DatasetMiner
from src.profiles.abi_music.miner_helpers.experiment_miner import ExperimentMiner
from src.profiles.abi_music.miner_helpers.project_miner import ProjectMiner

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class CustomMiner:
    """Profile-specific miner class

    Each profile has a custom miner class whose behaviour is based on the
    requirements of the researcher
    """

    def __init__(
        self,
    ) -> None:
        """Do not modify this method"""
        return None

    def mine(
        self,
        path: str,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
        options: Optional[dict[str, Any]] = None,
    ) -> om.OutputManager:
        """Mines metadata in a path

        Args:
            path (str): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes
            options (dict[str, any]): extra options for the inspection

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        if not out_man:
            out_man = om.OutputManager()
        else:
            out_man = copy.deepcopy(out_man)
        # Write the main inspection implementation here
        base_yaml_pth = Path("src") / Path("profiles") / Path("abi_music")
        proj_yaml_fp = base_yaml_pth / Path("project.yaml")
        expt_yaml_fp = base_yaml_pth / Path("experiment.yaml")
        dset_yaml_fp = base_yaml_pth / Path("dataset.yaml")
        dfile_yaml_fp = base_yaml_pth / Path("datafile.yaml")

        with proj_yaml_fp.open("r") as stream:
            proj_mappings = yaml.safe_load(stream)
        with expt_yaml_fp.open("r") as stream:
            expt_mappings = yaml.safe_load(stream)
        with dset_yaml_fp.open("r") as stream:
            dset_mappings = yaml.safe_load(stream)
        with dfile_yaml_fp.open("r") as stream:
            dfile_mappings = yaml.safe_load(stream)

        dset_idntfr = DataclassIdentifier()
        logger.info(f"path = {path}")
        dclass_struct = dset_idntfr.identify_data_classes(path, out_man)

        logger.info(f"dclass_struct = {dclass_struct}")
        proj_miner = ProjectMiner()
        out_man = proj_miner.mine_project_metadata(
            path, dclass_struct, proj_mappings, out_man
        )

        expt_miner = ExperimentMiner()
        out_man = expt_miner.mine_experiment_metadata(
            path, dclass_struct, expt_mappings, out_man
        )

        dset_miner = DatasetMiner()
        out_man = dset_miner.mine_dataset_metadata(
            path, dclass_struct, dset_mappings, out_man
        )

        dfile_miner = DatafileMiner()
        out_man = dfile_miner.mine_datafile_metadata(
            path, dclass_struct, dfile_mappings, out_man
        )

        return out_man
