# from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from src.mytardis_client.endpoints import URI
from src.mytardis_client.enumerators import DataClassification


class IngestedProject(BaseModel):
    classification: DataClassification
    created_by: URI
    # datafile_count: int
    # dataset_count: int
    description: str
    embargo_until: Any
    end_time: Any
    # experiment_count: int
    id: int
    identifiers: list[str]
    institution: list[str]
    locked: bool
    name: str
    parameter_sets: list
    principal_investigator: str
    public_access: int
    resource_uri: str
    size: int
    start_time: str
    tags: Any
    url: Any


# TODO: write minimal dataclasses for each type, using just the fields that are needed. This include identifiers, match keys, and the fields used for display names.


class IngestedExperiment(BaseModel):
    created_by: URI
    description: str
    end_time: Any
    id: int
    identifier: str
    institution: list[str]
    locked: bool
    name: str
    parameter_sets: list
    public_access: int
    resource_uri: str
    start_time: str
    tags: Any
    url: Any
