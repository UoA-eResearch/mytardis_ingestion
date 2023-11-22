"""Helpers to determine required metadata fields for datafiles"""
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Union


def calculate_md5sum(
    fp: Path,
) -> str:
    """Calculate the MD5 checksum of a file.

    Args:
        fp (Path): The path to the file for which to calculate the MD5 checksum.

    Returns:
        str: The MD5 checksum as a hexadecimal string.
    """
    with open(fp, mode="rb") as f:
        d = hashlib.md5()
        for buf in iter(lambda: f.read(128 * 1024), b""):
            d.update(buf)
    return d.hexdigest()


def replace_micrometer_values(
    data: Union[Dict[Any, Any], List[Any]], replacement: str
) -> Union[Dict[Any, Any], List[Any]]:
    """Recursively replace micrometer values in a dictionary or list.

    This function searches for string values in dictionaries or lists that end with "µm"
    (micrometers) and replaces them with the specified replacement string.

    Args:
        data (Union[Dict, List]): The dictionary or list containing values to check and replace.
        replacement (str): The replacement string for micrometer values.

    Returns:
        Union[Dict, List]: The input data with micrometer values replaced.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = (
                replace_micrometer_values(value, replacement)
                if isinstance(value, (dict, list))
                else (
                    value[:-2] + replacement
                    if isinstance(value, str) and value.endswith("µm")
                    else value
                )
            )
    elif isinstance(data, list):
        return [
            replace_micrometer_values(item, replacement)
            if isinstance(item, (dict, list))
            else (
                item[:-2] + replacement
                if isinstance(item, str) and item.endswith("µm")
                else item
            )
            for item in data
        ]
    return data
