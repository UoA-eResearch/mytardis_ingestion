import logging
from pathlib import Path
from typing import Any, Dict, List, Union

import yaml

logger = logging.getLogger(__name__)


def convert_to_pathlib(
    self, path_or_dict: Union[str, Dict, List]
) -> Union[str, Dict, List]:
    if isinstance(path_or_dict, str):
        return Path(path_or_dict)
    elif isinstance(path_or_dict, Dict):
        return {
            key: self.convert_to_pathlib(value) for key, value in path_or_dict.items()
        }
    elif isinstance(path_or_dict, List):
        return [self.convert_to_pathlib(item) for item in path_or_dict]
    else:
        return path_or_dict
