"""Specialised Exceptions for MyTardis

This module provides Exceptions that are relevant to the MyTardis Ingestion scripts.
These exceptions allow for more nuanced understanding of errors in the ingestion process"""


class SanityCheckError(Exception):
    """Custom exception for when a sanity check has been failed.

    This exception is raised when a dictionary is missing keys that are
    necessary
    The error returns the object type, input dictionary and missing keys"""

    def __init__(self, dictionary_name, input_dict, missing_keys):
        super().__init__()
        self.message = f"{dictionary_name} is missing necessary keys"
        self.input_dict = input_dict
        self.missing_keys = missing_keys


class HierarchyError(Exception):
    """Custom esception for when the hierarchy in MyTardis is not correct.

    This exception is raised when the specified parent object of an object
    in the MyTardis database cannot be found."""

    def __init__(self, parent_obj, child_obj):
        super().__init__()
        self.message = f"The parent object, {parent_obj} for {child_obj} was not found."
        self.parent_obj = parent_obj
        self.child_obj = child_obj
