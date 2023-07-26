import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def convert_to_pathlib(self, path_or_dict):
    if isinstance(path_or_dict, str):
        return Path(path_or_dict)
    elif isinstance(path_or_dict, dict):
        return {
            key: self.convert_to_pathlib(value) for key, value in path_or_dict.items()
        }
    elif isinstance(path_or_dict, list):
        return [self.convert_to_pathlib(item) for item in path_or_dict]
    else:
        return path_or_dict


with open("../../../tests/fixtures/fixtures_example.yaml") as f:
    data = yaml.safe_load(f)
    converted_data = convert_to_pathlib(data)
    yaml.dump(converted_data, "../../../tests/fixtures/fixtures_example_converted.yaml")
