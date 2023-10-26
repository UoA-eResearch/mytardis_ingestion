"""This is to convert all path from idw to pathlib
"""

# ---Imports
import logging
from pathlib import Path
from typing import Dict, List, Union

# from typing import Dict, List, Union

# ---Constants
logger = logging.getLogger(__name__)


def convert_to_pathlib(  # type: ignore
    self, path_or_dict: Union[str, Dict[str, str], List[str]]
) -> Union[str, Dict[str, str], List[str]]:
    """Convert a string, dictionary, or list to their Path equivalent.

    Args:
        path_or_dict (Union[str, Dict, List]): Input string, dictionary, or list.

    Returns:
        Union[str, Dict, List]: Path-converted equivalent of the input.
    """
    if isinstance(path_or_dict, str):
        return Path(path_or_dict)  # type: ignore
    if isinstance(path_or_dict, dict):
        return {
            key: self.convert_to_pathlib(value) for key, value in path_or_dict.items()
        }
    if isinstance(path_or_dict, list):
        return [self.convert_to_pathlib(item) for item in path_or_dict]

    return path_or_dict
