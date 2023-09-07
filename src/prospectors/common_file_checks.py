"""
Provides common checks or filters for prospecting a directory tree to determine
whether the files and directory structure can be used for ingestion.
"""


# ---Imports
from pathlib import Path
from typing import List, Tuple

from src.helpers.constants import (
    COMMON_MACOS_SYS_FILES,
    COMMON_WIN_SYS_FILES,
    MACOS_PREFIXES_TO_REJECT,
    METADATA_FILE_SUFFIX,
)


def perform_common_file_checks(
    path: Path,
    recursive: bool = True,
    reject_hidden: bool = False,
) -> Tuple[List[Path], List[Path]]:
    """Performs the checking procedures and determines which files
    should be rejected based on common system file names and common
    file prefixes.
    Args:
        path (Path): the path to perform the check on.
        recursive (bool): whether to perform checks on child directories recursively.
    Returns:
        Tuple[[List[Path], List[Path]]]: lists of filepaths that are rejected or accepted.
    """

    common_filenames = [*COMMON_MACOS_SYS_FILES, *COMMON_WIN_SYS_FILES]
    # Can be constructed from multiple lists in future
    reject_prefixes = MACOS_PREFIXES_TO_REJECT

    rejection_list, ingestion_list = iterate_dir(
        path,
        common_filenames,
        reject_prefixes,
        reject_hidden,
        recursive,
    )

    return (rejection_list, ingestion_list)


def iterate_dir(
    directory: Path,
    common_filenames: List[str],
    reject_prefixes: List[str],
    reject_hidden: bool,
    recursive: bool,
) -> Tuple[List[Path], List[Path]]:
    """Iterates through a specified directory to perform common checks
    Args:
        dir (Path): directory to check
        common_filenames (List[str]): a list of system filenames
        reject_prefixes (List[str]): a list of known prefixes for system files
        reject_hidden (bool): a flag to indicate if dot files should automatically be
            rejected
        recursive (bool): a flag to indicate if subdirectories should be iterated over
            recursively
    Returns:
        Tuple([List[Path], List[Path]]): lists of filepaths that
        rejected or accepted in this directory.
    """
    glob_str = "**/*" if recursive else "*"
    paths = directory.glob(glob_str)
    dir_list = [item for item in paths if item.is_file()]
    rejection_list = [
        item
        for item in dir_list
        if (
            item.name in common_filenames
            or check_file_prefix(item, reject_prefixes, reject_hidden, dir_list)
            or METADATA_FILE_SUFFIX in item.stem
        )
    ]
    rejection_set = set(rejection_list)
    ingestion_list = [item for item in dir_list if item not in rejection_set]

    return (rejection_list, ingestion_list)


def check_file_prefix(
    test_file: Path,
    reject_prefixes: List[str],
    reject_hidden: bool,
    dir_list: List[Path],
) -> bool:
    """Checks a file against its residing folder by first determining
    whether the file has a common prefix, then checking if there is a file
    that already exists if the prefix was removed. If so, this indicates
    that the file was an operating-system-generated metafile.
    Args:
        test_file (Path): the file object to check
        reject_prefixes (List[str]): list of known file prefixes to reject
        reject_hidden (bool): a flag to indicate if dot files should automatically be
            rejected
        dir_list (List[Path]): The files in a directory containing the test_file
            so that the prefix can be checked to see if a data file also exists.
    Returns:
        bool: True if file is metafile, False otherwise
    """
    return any(
        (
            (
                test_file.stem.startswith(test_str)
                and Path(test_file.name.removeprefix(test_str)) in dir_list
            )
            or (test_file.stem.startswith(".") and reject_hidden)
        )
        for test_str in reject_prefixes
    )
