"""Helper functions related to multiple dataclasses."""

from typing import Type

from src.blueprints import BaseDatafile, BaseDataset, BaseExperiment, BaseProject


def get_name(
    object_class: Type[
        BaseDatafile | BaseDataset | BaseExperiment | BaseProject
    ],
) -> str | None:
    """Generic helper function to get the name from a MyTardis  dataclass object"""
    if isinstance(object_class, BaseDatafile):
        return object_class.filename
    if isinstance(object_class, BaseDataset):
        return object_class.description
    if isinstance(object_class, BaseExperiment):
        return object_class.title
    if isinstance(object_class, BaseProject):
        return object_class.name
    return None
