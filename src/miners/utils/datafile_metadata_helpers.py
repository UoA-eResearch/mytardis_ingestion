"""Helpers to determine required metadata fields for datafiles"""

import hashlib
import mimetypes
from pathlib import Path


def calculate_md5sum(  # pylint: disable=missing-function-docstring
    filepath: Path,
) -> str:
    with filepath.open(mode="rb") as input_file:
        chunk = hashlib.new("md5", usedforsecurity=False)
        for buffer in iter(lambda: input_file.read(128 * 1024), b""):
            chunk.update(buffer)
    return chunk.hexdigest()


def determine_mimetype(  # pylint: disable=missing-function-docstring
    filename: str,
) -> str:
    mimetype, _ = mimetypes.guess_type(filename)
    if not mimetype:
        mimetype = filename.split(".")[-1]
    return mimetype
