"""Definition of MyTardis API endpoints, and related types."""

from typing import Literal

MyTardisEndpoint = Literal[
    "/dataset",
    "/datasetparameterset",
    "/dataset_file",
    "/experiment",
    "/experimentparameterset",
    "/facility",
    "/group",
    "/institution",
    "/instrument",
    "/introspection",
    "/project",
    "/projectparameterset",
    "/schema",
    "/storagebox",
    "/user",
]
