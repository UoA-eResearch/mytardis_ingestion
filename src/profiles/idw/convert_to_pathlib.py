# ---Imports
import logging
from pathlib import Path
from typing import Dict, List, Union

# ---Constants
logger = logging.getLogger(__name__)


def convert_to_pathlib(
    self, path_or_dict: Union[str, Dict, List]
) -> Union[str, Dict, List]:
    """Convert a string, dictionary, or list to their Path equivalent.

    Args:
        path_or_dict (Union[str, Dict, List]): Input string, dictionary, or list.

    Returns:
        Union[str, Dict, List]: Path-converted equivalent of the input.
    """
    if isinstance(path_or_dict, str):
        return Path(path_or_dict)
    if isinstance(path_or_dict, Dict):
        return {
            key: self.convert_to_pathlib(value) for key, value in path_or_dict.items()
        }
    elif isinstance(path_or_dict, List):
        return [self.convert_to_pathlib(item) for item in path_or_dict]
    else:
        return path_or_dict
