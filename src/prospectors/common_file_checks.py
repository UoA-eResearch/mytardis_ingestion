"""
Provides common checks or filters for prospecting a directory tree to determine
whether the files and directory structure can be used for ingestion.
"""


# ---Imports
import logging
import os

from src.profiles import profile_consts as pc

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
        csf = CommonSystemFiles()
        self.common_fnames_lut = csf.fnames_lut
        self.reject_prefix_lut = csf.reject_prefixes_lut


    def perform_common_file_checks(
        self,
        path: str,
        recursive: bool = True,
    ) -> tuple([list[str], list[str]]):
        """Performs the checking procedures and determines which files
        should be rejected based on common system file names and common
        file prefixes.
        Args:
            path (str): the path to perform the check on.
            recursive (bool): whether to perform checks on child directories recursively.
        Returns:
            tuple(rejection_list, ingestion_list): lists of filepaths that are rejected or accepted.
        """
        rejection_list = []
        ingestion_list = []

        if recursive:
            for root, dirs, files in os.walk(path):
                out = self._iterate_dir(
                    root, 
                    self.common_fnames_lut, 
                    self.reject_prefix_lut,
                )

                rejection_list = self._extend_list(rejection_list, out[0])
                ingestion_list = self._extend_list(ingestion_list, out[1])

                for dir in dirs:
                    dirlist = os.listdir(os.path.join(root, dir))
                    if len(dirlist) == 0:
                        logger.debug("Empty dir {0} found in {1}".format(root, dir))
        else:
            out = self._iterate_dir(
                path, 
                self.common_fnames_lut, 
                self.reject_prefix_lut
            )
            rejection_list = self._extend_list(rejection_list, out[0])
            ingestion_list = self._extend_list(ingestion_list, out[1])

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
        dir: str,
        cmn_fnames_lut: dict,
        rej_prfx_lut: dict,
    ) -> tuple([list[str], list[str]]):
        """Iterates through a specified directory to perform common checks
        Args:
            dir (str): directory to check
            cmn_fnames_lut (dict): look-up table of common system filenames
            rej_prfx_lut (dict): look-up table of file prefixes to reject
            chk_eqv_file (bool):
        Returns:
            tuple(rejection_list, ingestion_list): lists of filepaths that
            rejected or accepted in this directory.
        """
        rejection_list = []
        ingestion_list = []

        dir_list = [
            item for item in os.listdir(dir) 
            if os.path.isfile(os.path.join(dir, item))
        ]
        dir_lut = dict.fromkeys(dir_list)
        for item in dir_list:
            test_fp = os.path.join(dir, item)
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
        dir_lut: dict,
        rej_prfx_lut: dict,
        file: str,
    ) -> bool:
        """Checks a file against its residing folder by first determining 
        whether the file has a common prefix, then checking if there is a file 
        that already exists if the prefixed was removed. If so, this indicates 
        that the file was an operating-system-generated metafile.
        Args:
            dir_lut (dict): lookup table of all items in the directory
            rej_prfx_lut (dict): lookup table of all file prefixes
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
        dir_lut: dict,
        rej_prfx_lut: dict,
        file: str,
    ) -> bool:
        """Checks a file for a leading '._' which is a strong indicator that
        the file is generated by the OS and hence a metafile.
        Please note that there may be a chance that a researcher may use '._'
        as a file (though unlikely). Should this incidence arise, then this
        function should be called/ignored accordingly.

        Args:
            dir_lut (dict): lookup table of all items in the directory
            rej_prfx_lut (dict): lookup table of all file prefixes
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


class CommonSystemFiles:
    """Model class which stores common system files and
    system-generated files with prefixes that should be rejected
    """

    COMMON_MACOS_SYS_FILES = [
        ".DS_Store",
        "._.DS_Store",
        ".Trashes",
        ".Spotlight-V100",
        ".fseventsd",
        ".TemporaryItems",
        ".com.apple.timemachine.donotpresent",
        ".vol",
        ".AppleDouble",
        ".FileSync-lock",
        ".AppleDB",
    ]

    COMMON_WIN_SYS_FILES = [
        "thumbs.db"
    ]

    MACOS_PREFIXES_TO_REJECT = ["._"]


    def __init__(
        self,
    ) -> None:
        """Creates lookup tables based on the lists"""
        look_up_list = []
        look_up_list.extend(self.COMMON_MACOS_SYS_FILES)
        look_up_list.extend(self.COMMON_WIN_SYS_FILES)
        self.fnames_lut = dict.fromkeys(look_up_list)

        reject_prefixes = []
        reject_prefixes.extend(self.MACOS_PREFIXES_TO_REJECT)
        self.reject_prefixes_lut = dict.fromkeys(reject_prefixes)