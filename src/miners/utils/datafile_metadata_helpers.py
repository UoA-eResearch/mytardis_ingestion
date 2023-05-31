"""Helpers to determine required metadata fields for datafiles"""

import hashlib
import mimetypes

def calculate_md5sum(
    fp: str,
) -> str:
    with open(fp, mode="rb") as f:
        d = hashlib.md5()
        for buf in iter(lambda: f.read(128 * 1024), b""):
            d.update(buf)
    return d.hexdigest()


def determine_mimetype(
    fn: str,
) -> str | None:
    mimetype, encoding = mimetypes.guess_type(fn)
    return mimetype