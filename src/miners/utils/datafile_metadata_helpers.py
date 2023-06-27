"""Helpers to determine required metadata fields for datafiles"""

import hashlib
import mimetypes
from pathlib import Path


def calculate_md5sum(
    fp: Path,
) -> str:
    with fp.open(mode="rb") as f:
        d = hashlib.md5()
        for buf in iter(lambda: f.read(128 * 1024), b""):
            d.update(buf)
    return d.hexdigest()


def determine_mimetype(
    fn: str,
) -> str:
    mimetype, encoding = mimetypes.guess_type(fn)
    if not mimetype:
        mimetype = fn.split(".")[-1]
    return mimetype
