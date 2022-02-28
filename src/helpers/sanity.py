# pylint: disable=missing-module-docstring

from src.helpers.exceptions import SanityCheckError


def sanity_check(dictionary_name: str, input_dict: dict, required_keys: list) -> bool:
    """Checks a dictionary to make sure that necessary keys are present in it.

    This function reads in a dictionary and compares the keys present to a list of necessary keys.
    If there are missing keys, the function raises a SanityCheckError

    Args:
        dictionary_name: The human readable name of the dictionary for debugging
        input_dict: The python dictionary to be checked
        required_keys: A list of necessary keys

    Returns:
        True if the necessary keys are present in the dictionary

    Raises:
        SanityCheckError: if there are missing keys
    """
    if not required_keys - input_dict.keys():
        return True
    missing_keys = list(required_keys - input_dict.keys())
    raise SanityCheckError(dictionary_name, input_dict, missing_keys)
