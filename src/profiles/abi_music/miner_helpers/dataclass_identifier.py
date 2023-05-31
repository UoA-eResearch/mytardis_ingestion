"""Determines the dataclass based on the path
"""


# ---Imports
import copy
import logging
import os

import yaml

from src.profiles import output_manager as om
from typing import Optional, Any

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class DataclassIdentifier:
    """Determines the dataclass based on the path
    """

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
            rel_path = os.path.relpath(root, path)

            if rel_path not in rel_dir_rej_lut and rel_path != ".":
                path_components = rel_path.split(os.sep)

                dir_levels = len(path_components)
                if dir_levels < 4:
                    for i in range(dir_levels):
                        if i == 0:
                            if path_components[i] not in dclass_struct:
                                dclass_struct[path_components[i]] = {}
                        elif i == 1:
                            if path_components[i - 1] not in dclass_struct:
                                dclass_struct[path_components[i - 1]] = {}
                            if (path_components[i] not in dclass_struct[path_components[i - 1]]):
                                dclass_struct[path_components[i - 1]][path_components[i]] = {}
                        elif i == 2:
                            if path_components[i - 2] not in dclass_struct:
                                dclass_struct[path_components[i - 2]] = {}
                            if (path_components[i - 1] not in dclass_struct[path_components[i - 2]]):
                                dclass_struct[path_components[i - 2]][path_components[i - 1]] = {}
                            if (path_components[i] not in dclass_struct[path_components[i - 2]][path_components[i - 1]]):
                                dclass_struct[path_components[i - 2]][path_components[i - 1]][path_components[i]] = {}

        return dclass_struct

    