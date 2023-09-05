"""Model class which stores common system files and
    system-generated files with prefixes that should be rejected
    """


# ---Imports
import logging

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
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
