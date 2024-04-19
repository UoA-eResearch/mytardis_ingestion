"""Creation time-related utilities."""

from pathlib import Path


def max_ctime(pths: list[Path]) -> float:
    """Given a list of file Paths, return the newest
    creation time from them.

    Args:
        pths (list[Path]): List of file Paths.

    Returns:
        float: The newest creation time from the files.
    """
    ret_ctime: float = 0
    for pth in pths:
        if pth.exists() and pth.is_file():
            # Find the newest file's age.
            ctime = pth.stat().st_ctime
            ret_ctime = max(ctime, ret_ctime)
    return ret_ctime
