"""Helpers for use in the IDW profile."""

from typing import Any, Dict, List, Union


def replace_micrometer_values(
    data: Union[Dict[Any, Any], List[Any]], replacement: str
) -> Union[Dict[Any, Any], List[Any]]:
    """Recursively replace micrometer values in a dictionary or list.

    This function searches for string values in dictionaries or lists that end with "µm"
    (micrometers) and replaces them with the specified replacement string.

    Args:
        data (Union[Dict, List]): The dictionary or list containing values to check and replace.
        replacement (str): The replacement string for micrometer values.

    Returns:
        Union[Dict, List]: The input data with micrometer values replaced.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = (
                replace_micrometer_values(value, replacement)
                if isinstance(value, (dict, list))
                else (
                    value[:-2] + replacement
                    if isinstance(value, str) and value.endswith("µm")
                    else value
                )
            )
    elif isinstance(data, list):
        return [
            (
                replace_micrometer_values(item, replacement)
                if isinstance(item, (dict, list))
                else (
                    item[:-2] + replacement
                    if isinstance(item, str) and item.endswith("µm")
                    else item
                )
            )
            for item in data
        ]
    return data
