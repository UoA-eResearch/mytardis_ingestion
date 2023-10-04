# pylint: disable-all
# type: ignore
# noqa
# nosec

"""Determines the dataclass based on the path
"""


# ---Imports
import copy
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from src.extraction_output_manager import output_manager as om

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class DataclassIdentifier:
    """Determines the dataclass based on the path"""

    def __init__(
        self,
    ) -> None:
        return

    # Write rest of implementation here, use leading underscores for each method
    def identify_data_classes(
        self,
        path: str,
        out_man: om.OutputManager,
    ) -> dict[str, Any]:
        """Identify data class structure of a dataset at the given path.

        Args:
            self (object): The object instance.
            path (str): The path to the dataset.
            out_man (om.OutputManager): Output manager.

        Returns:
            dict[str, Any]: Dictionary containing identified data class structure.
        """
        dclass_struct: dict[str, Any] = {}
        new_out_man = copy.deepcopy(out_man)

        dir_rej_list = new_out_man.dirs_to_ignore
        rel_dir_rej_list = [os.path.relpath(x, path) for x in dir_rej_list]
        rel_dir_rej_lut = dict.fromkeys(rel_dir_rej_list)

        for root, dirs, files in os.walk(path):
            root_pth = Path(root)
            rel_path = root_pth.relative_to(path)
            if str(rel_path) not in rel_dir_rej_lut and str(rel_path) != ".":
                pth_components = rel_path.parts
                dir_levels = len(pth_components)
                if dir_levels < 4:
                    dclass_struct = self.add_dclass_struct_entry(
                        dclass_struct, dir_levels, list(pth_components)
                    )

        return dclass_struct

    def add_dclass_struct_entry(
        self,
        dclass_struct: Dict[str, Any],
        dir_levels: int,
        pth_components: List[str],
    ) -> Dict[str, Any]:
        new_struct: Dict[str, Any] = copy.deepcopy(dclass_struct)
        for i in range(dir_levels):
            if i == 0:
                if pth_components[i] not in dclass_struct:
                    new_struct[pth_components[i]] = {}
            elif i == 1:
                if pth_components[i - 1] not in dclass_struct:
                    new_struct[pth_components[i - 1]] = {}
                if pth_components[i] not in dclass_struct[pth_components[i - 1]]:
                    new_struct[pth_components[i - 1]][pth_components[i]] = {}
            elif i == 2:
                if pth_components[i - 2] not in dclass_struct:
                    new_struct[pth_components[i - 2]] = {}
                if pth_components[i - 1] not in dclass_struct[pth_components[i - 2]]:
                    new_struct[pth_components[i - 2]][pth_components[i - 1]] = {}
                if (
                    pth_components[i]
                    not in dclass_struct[pth_components[i - 2]][pth_components[i - 1]]
                ):
                    new_struct[pth_components[i - 2]][pth_components[i - 1]][
                        pth_components[i]
                    ] = {}

        return new_struct
