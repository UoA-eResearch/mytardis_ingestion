"""Helpers to determine required metadata fields for datafiles"""
import hashlib
import mimetypes
from pathlib import Path
from typing import Dict, List, Union


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


def determine_mimetype(
    fn: str,
) -> str | None:
    """Determine the MIME type of a file.

    Args:
        fn (str): The file name or path for which to determine the MIME type.

    Returns:
        str | None: The MIME type if it can be determined, or None if not.
    """
    mimetype,  _ = mimetypes.guess_type(fn)
    return mimetype


def replace_micrometer_values(
    data: Union[Dict, List], replacement: str
) -> Union[Dict, List]:
    """Recursively replace micrometer values in a dictionary or list.

    This function searches for string values in dictionaries or lists that end with "µm" (micrometers)
    and replaces them with the specified replacement string.

    Args:
        data (Union[Dict, List]): The dictionary or list containing values to check and replace.
        replacement (str): The replacement string for micrometer values.

    Returns:
        Union[Dict, List]: The input data with micrometer values replaced.
    """
    if isinstance(data, Dict):
        for key, value in data.items():
            if isinstance(value, str) and value.endswith("µm"):
                data[key] = value[:-2] + replacement
            elif isinstance(value, Union[Dict, List]):
                replace_micrometer_values(value, replacement)
    return data
