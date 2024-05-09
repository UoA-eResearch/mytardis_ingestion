"""Helper functions for the overseer module"""

from urllib.parse import urlparse

from src.blueprints.custom_data_types import URI


def resource_uri_to_id(uri: URI) -> int:
    """Gets the id from a resource URI

    Takes resource URI like: http://example.org/api/v1/experiment/998
    and returns just the id value (998).

    Args:
        uri: str - the URI from MyTardis

    Returns:
        The integer id that maps to the URI
    """
    uri_sep: str = "/"
    return int(urlparse(uri).path.rstrip(uri_sep).split(uri_sep).pop())
