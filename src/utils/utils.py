"""
utils.py - miscellaneous functions.
"""

from pathlib import Path


def file_size_to_str(size: float) -> str:
    """
    Given a file size, return a human-friendly string representation.

    Args:
        size (float): Size of the file in bytes.

    Returns:
        str: Human-friendly string representation of the file size.
    """
    for x in ["bytes", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f} {x}"
        size /= 1024.0
    # If size exceeds 1024 TB, return in terms of TB.
    return f"{size:.1f} TB"


def st_dev(path: Path) -> int:
    """Returns the device that the file
    `path`_ is stored on. This function uses
    os.Path.stat() to get the device id.

    Args:
        path (Path): The Path of the file.

    Returns:
        int: the device id.
    """
    return path.stat().st_dev
