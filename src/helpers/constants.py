"""Constants used in the ingestion process"""

PROJECT_KEYS = [
    "name",
    "description",
    "locked",
    "public_access",
    "principal_investigator",
    "institution",
    "embargo_until",
    "start_time",
    "end_time",
    "created_by",
    "url",
    "users",
    "groups",
]

EXPERIMENT_KEYS = [
    "title",
    "description",
    "institution_name",
    "locked",
    "public_access",
    "embargo_until",
    "start_time",
    "end_time",
    "created_time",
    "update_time",
    "created_by",
    "url",
    "users",
    "groups",
    "projects",
]

DATASET_KEYS = [
    "experiments",
    "description",
    "directory",
    "created_time",
    "modified_time",
    "immutable",
    "instrument",
    "users",
    "groups",
]


DATAFILE_KEYS = [
    "dataset",
    "full_path",
    "relative_file_path",
    "filename",
    "md5sum",
    "directory",
    "mimetype",
    "size",
    "replicas",
    "users",
    "groups",
]
