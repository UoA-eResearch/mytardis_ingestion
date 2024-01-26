"""Add profile-specific constants in this module
"""

CONFIG_FIELD = "config"
BASENAME_FIELD = "Basename"
PROJECT_FIELD = "Project"
SAMPLE_FIELD = "Sample"
SEQUENCE_FIELD = "Sequence"
DESCRIPTION_FIELD = "Description"
INSTRUMENT_FIELD = "Instrument"

FIELDS_WITH_DEFAULTS = [INSTRUMENT_FIELD]
FIELDS_WITH_DEFAULTS_LUT = dict.fromkeys(FIELDS_WITH_DEFAULTS)

RAW_FOLDER_SUFFIX = "-Raw"
DECONV_FOLDER_SUFFIX = "-Deconv"
FOLDER_SUFFIX_KEYS = [RAW_FOLDER_SUFFIX, DECONV_FOLDER_SUFFIX]
FOLDER_SUFFIX_LUT = dict.fromkeys(FOLDER_SUFFIX_KEYS)


OUTPUT_NOTE_JSON_MATCH_SUCCESS = "json matches folder path"
OUTPUT_NOTE_JSON_MATCH_FAIL = "json does not match folder path"

METADATA_FILE_TYPE = ".json"

ABI_SCHEMA = "https://abi.music"

DEFAULT_INSTITUTION = "University of Auckland"

ABI_MUSIC_MICROSCOPE_INSTRUMENT = "abi-music-microscope-v1"
ABI_MUSIC_POSTPROCESSING_INSTRUMENT = "abi-music-post-processing-v1"

ABI_MUSIC_DATASET_RAW_SCHEMA = "http://abi-music.com/dataset-raw/1"
ABI_MUSIC_DATASET_ZARR_SCHEMA = "http://abi-music.com/dataset-zarr/1"
