"""Constants used in various places in the code base."""

# File types to be skipped

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

# Constants used throughout a profile in the Extraction Plant

PROFILE_NAME_DEFAULT = "default"

DEFAULT_KEY = "default"
NAME_KEY = "name"
NATIVE_KEY = "native"
REQUIRED_KEY = "required"
TYPE_KEY = "type_"
USEDEFAULT_KEY = "use_default_if_nonexistent"
DATACLASS_ENTRY_DICT_KEYS = [
    DEFAULT_KEY,
    NAME_KEY,
    NATIVE_KEY,
    REQUIRED_KEY,
    USEDEFAULT_KEY,
]

DATAFILE_NAME = "datafile"
DATASET_NAME = "dataset"
EXPERIMENT_NAME = "experiment"
PROJECT_NAME = "project"
DATACLASS_PROF_SUFFIX = ".yaml"
DATACLASSES_LIST = [PROJECT_NAME, EXPERIMENT_NAME, DATASET_NAME, DATAFILE_NAME]

OUTPUT_SUCCESS_KEY = "success"
OUTPUT_IGNORED_KEY = "ignored"
OUTPUT_ISSUES_KEY = "issues"
OUTPUT_DICT_KEYS = [OUTPUT_SUCCESS_KEY, OUTPUT_IGNORED_KEY, OUTPUT_ISSUES_KEY]

OUTPUT_VALUE_SUBKEY = "value"
OUTPUT_PROCESS_SUBKEY = "process"
OUTPUT_NOTES_SUBKEY = "notes"
OUTPUT_SUBDICT_KEYS = [OUTPUT_VALUE_SUBKEY, OUTPUT_PROCESS_SUBKEY, OUTPUT_NOTES_SUBKEY]

PROCESS_PROSPECTOR = "prospector"
PROCESS_MINER = "miner"

METADATA_FILE_SUFFIX = "_MyTardisMetadata"

KEY_LVL_SEP = "->"
KEY_IDX_OP = "_."
KEY_IDX_CL = "._"
