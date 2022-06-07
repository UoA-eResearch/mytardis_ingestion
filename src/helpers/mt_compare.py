# pylint: disable=logging-fstring-interpolation
"""Helper function to compare MyTardis objects and their ingestion object_dictionaries

This module defines a match_object_with_dictionary function that does some basic comparison
for different objects to determine if they are the same as in MyTardis.
"""

import logging
from typing import Union

logger = logging.getLogger(__name__)


def match_object_with_dictionary(
    object_dict: dict, object_from_mytardis: dict, comparison_keys: list
) -> Union[bool, None]:
    """General function that takes a list of keys to compare against and checks to see if an
    object returned from MyTardis is the same as an object defined in the ingestion object_dict

    Args:
        object_dict: the dictionary prepared by the ingestion process to create the object in
            MyTardis
        object_from_mytardis: A dictionary returned from a successful search in MyTardis
        comparison_keys: The keys that should match in order for the object to be identified as a
            match

    Returns:
        True if the fields specified in comparison keys are an exact match,
        False if they do not match.
    """
    match = True
    for key in comparison_keys:
        try:
            ingestion_value = object_dict[key]
            mytardis_value = object_from_mytardis[key]
        except KeyError:
            if key in object_dict:
                logger.warning(
                    (
                        "Either the return from MyTardis is incomplete, or there are additional "
                        "keys in both the comparison_keys list and the object_dict generated for "
                        "ingestion.\n"
                        f"The object dictionary from ingestion is: {object_dict}\nThe object "
                        f"dictionary from MyTardis is: {object_from_mytardis}.\nThe keys used for "
                        f"comparison are: {comparison_keys}."
                    )
                )
                return None
            if key in object_from_mytardis:
                logger.warning(
                    (
                        "Incomplete ingestion dictionary prevents proper comparison.\n"
                        f"The object dictionary from ingestion is: {object_dict}\nThe object "
                        f"dictionary from MyTardis is: {object_from_mytardis}.\nThe keys used for "
                        f"comparison are: {comparison_keys}."
                    )
                )
                return None
            logger.warning(
                (
                    f"Unable to find the key, {key}, in either dictionaries for comparison. This "
                    "is likely due to the wrong set of comparison keys being handed over and may "
                    "be the result of an incorrect object specificiation.\n"
                    f"The object dictionary from ingestion is: {object_dict}\nThe object "
                    f"dictionary from MyTardis is: {object_from_mytardis}.\nThe keys used for "
                    f"comparison are: {comparison_keys}."
                )
            )
            return None
        except Exception as error:
            logger.error(
                "Error occured when attempting to compare dictionaries from mytardis and ingestion scripts.",  # pylint: disable=line-too-long
                exc_info=True,
            )
            raise error
        if ingestion_value != mytardis_value:
            match = False
    return match
