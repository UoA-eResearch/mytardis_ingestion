"""
Provides common checks or filters for prospecting a directory tree to determine
whether the files and directory structure can be used for ingestion.
"""

# ---Imports
import logging
import os
from typing import Any, List, Tuple

from src.profiles import profile_consts as pc
from src.utils.filesystem.filters import (
    get_excluded_system_filename_prefixes,
    get_excluded_system_suffixes,
)

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class CommonDirectoryTreeChecks:
    """Checks for common or known operating system files or file prefixes
    that are not normally intended for ingestion into MyTardis.
    """

    def __init__(
        self,
    ) -> None:
        """Instantiates look-up tables for common system files."""
        self.common_fnames_lut = dict.fromkeys(get_excluded_system_suffixes())
        self.reject_prefix_lut = dict.fromkeys(get_excluded_system_filename_prefixes())

    def perform_common_file_checks(
        self,
        path: str,
        recursive: bool = True,
    ) -> Tuple[List[str], List[str]]:
        """Performs the checking procedures and determines which files
        should be rejected based on common system file names and common
        file prefixes.
        Args:
            path (str): the path to perform the check on.
            recursive (bool): whether to perform checks on child directories recursively.
        Returns:
            tuple[[list[str], list[str]]]: lists of filepaths that are rejected or accepted.
        """
        rejection_list: list[str] = []
        ingestion_list: list[str] = []

        if recursive:
            for root, dirs, _ in os.walk(path):
                out = self._iterate_dir(
                    root,
                    self.common_fnames_lut,
                    self.reject_prefix_lut,
                )

                rejection_list = self._extend_list(rejection_list, out[0])
                ingestion_list = self._extend_list(ingestion_list, out[1])

                for directory in dirs:
                    dirlist = os.listdir(os.path.join(root, directory))
                    if len(dirlist) == 0:
                        logger.debug(  # pylint: disable=logging-fstring-interpolation
                            f"Empty dir {root} found in {directory}"
                        )
        else:
            out = self._iterate_dir(
                path, self.common_fnames_lut, self.reject_prefix_lut
            )
            rejectables = out[0]
            ingestables = out[1]
            rejection_list = self._extend_list(rejection_list, rejectables)
            ingestion_list = self._extend_list(ingestion_list, ingestables)

        return (rejection_list, ingestion_list)

    def _extend_list(
        self,
        main_list: list[str],
        ext_list: list[str],
    ) -> list[str]:
        extended_list = main_list.copy()
        extended_list.extend(ext_list)
        return extended_list

    def _iterate_dir(
        self,
        directory: str,
        cmn_fnames_lut: dict[str, Any],
        rej_prfx_lut: dict[str, Any],
    ) -> tuple[list[str], list[str]]:
        """Iterates through a specified directory to perform common checks
        Args:
            directory (str): directory to check
            cmn_fnames_lut (dict[str, str]): look-up table of common system filenames
            rej_prfx_lut (dict[str, str]): look-up table of file prefixes to reject
            chk_eqv_file (bool):
        Returns:
            tuple([list[str], list[str]]): lists of filepaths that
            rejected or accepted in this directory.
        """
        rejection_list: list[str] = []
        ingestion_list: list[str] = []

        dir_list = [
            item
            for item in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, item))
        ]
        dir_lut = dict.fromkeys(dir_list)
        for item in dir_list:
            test_fp = os.path.join(directory, item)
            if os.path.isfile(test_fp):
                if item in cmn_fnames_lut:
                    rejection_list.append(test_fp)
                elif self._check_for_equivalent_file_in_folder(
                    dir_lut, rej_prfx_lut, item
                ):
                    rejection_list.append(test_fp)
                elif self._check_for_leading_dot_underscore(
                    dir_lut, rej_prfx_lut, item
                ):
                    rejection_list.append(test_fp)
                elif pc.METADATA_FILE_SUFFIX in item:
                    rejection_list.append(test_fp)
                else:
                    ingestion_list.append(test_fp)

        return (rejection_list, ingestion_list)

    def _check_for_equivalent_file_in_folder(
        self,
        dir_lut: dict[str, Any],
        rej_prfx_lut: dict[str, Any],
        file: str,
    ) -> bool:
        """Checks a file against its residing folder by first determining
        whether the file has a common prefix, then checking if there is a file
        that already exists if the prefixed was removed. If so, this indicates
        that the file was an operating-system-generated metafile.
        Args:
            dir_lut (dict[str, Any]): lookup table of all items in the directory
            rej_prfx_lut (dict[str, Any]): lookup table of all file prefixes
            file (str): file to check
        Returns:
            bool: True if file is metafile, False otherwise
        """
        for search_str in rej_prfx_lut.keys():
            if file.find(search_str) == 0:
                search_file = file.replace(search_str, "")
                if search_file in dir_lut:
                    return True

        return False

    def _check_for_leading_dot_underscore(
        self,
        dir_lut: dict[str, Any],
        rej_prfx_lut: dict[str, Any],
        file: str,
    ) -> bool:
        """Checks a file for a leading '._' which is a strong indicator that
        the file is generated by the OS and hence a metafile.
        Please note that there may be a chance that a researcher may use '._'
        as a file (though unlikely). Should this incidence arise, then this
        function should be called/ignored accordingly.

        Args:
            dir_lut (dict[str, Any]): lookup table of all items in the directory
            rej_prfx_lut (dict[str, any]): lookup table of all file prefixes
            file (str): file to check
        Returns:
            bool: True if file is metafile, False otherwise
        """
        for search_str in rej_prfx_lut.keys():
            if file.find(search_str) == 0:
                search_file = file.replace(search_str, "")
                if search_file in dir_lut:
                    return True

        return False
