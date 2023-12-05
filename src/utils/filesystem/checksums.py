"""
Helpers for calculating checksums and hashes
"""

import hashlib
from pathlib import Path


def calculate_md5(file: Path) -> str:
    """
    Compute the MD5 hash of the file referenced by `file`.
    NOTE: a version already exists in datafile_metadata_helpers.py - should unify them
    """
    with file.open("rb") as f:
        calculator = hashlib.md5()

        chunk_size = 8192

        while chunk := f.read(chunk_size):
            calculator.update(chunk)

        return calculator.hexdigest()
