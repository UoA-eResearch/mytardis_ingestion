"""Model class which stores common system files and
    system-generated files with prefixes that should be rejected
    """


# ---Imports
import logging
from pathlib import Path
from typing import Callable

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class CommonSystemFiles:  # pylint: disable=too-few-public-methods
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

    COMMON_WIN_SYS_FILES = ["thumbs.db"]

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



def is_mac_os_sys_file(path : Path) -> bool:
    extension = path.suffix

    if extension in CommonSystemFiles.COMMON_MACOS_SYS_FILES:
        return True
    
    if any(extension.startswith(p) for p in CommonSystemFiles.MACOS_PREFIXES_TO_REJECT):
        return True
    
    return False


def is_windows_sys_file(path : Path) -> bool:
    return path.name in CommonSystemFiles.COMMON_WIN_SYS_FILES


def is_system_file(path : Path) -> bool:
    # TODO: we don't have a definition of Linux system files at the moment,
    # but presumably there are some?
    return is_mac_os_sys_file(path) or is_windows_sys_file(path)


class FilesystemFilter:
    """
    A collection for holding filters to be applied to filesystem entries, to decide whether
    they are "valid" or not.
    """

    def __init__(self, system_files : bool = True):
        self._filter_system_files = system_files
        self._filters : list[Callable[[Path], bool]] = []

        if self._filter_system_files:
            self.add(is_system_file)

    def add(self, filter_func : Callable[[Path], bool]) -> None:
        """
        Add a filter predicate to the collection of filters
        """
        self._filters.append(filter_func)

    def is_valid(self, path : Path) -> bool:
        """
        Determine whether the entry referred to by 'path' should be retained by the filter or not.
        This is determined by applying the filter functions (predicates).
        """
        return any(func(path) for func in self._filters)
