"""Provides helper functions that allow for testing and manipulating MyTardis URIs"""

import re


def is_uri(uri_string: str, object_type: str) -> bool:
    """Does a simple assessment to see if the string is appropriately formatted as a
    URI for the object_type.

    Args:
        uri_string: the string to be tested as a URI
        object_type: the object type from which the URI should be generated

    Returns:
        True if the test string is formatted correctly for URIs, False otherwise
    """
    regex_pattern = rf"^/api/v1/{object_type}/\d+/$"
    try:
        if re.match(regex_pattern, uri_string):
            return True
    except TypeError:
        return False
    except Exception as error:
        raise error
    return False
