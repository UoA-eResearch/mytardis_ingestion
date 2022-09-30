# pylint: disable=logging-fstring-interpolation,unsubscriptable-object
"""Inspector class checks to ensure that the objects to be created don't
already exist in MyTardis.
If there is a potential match, then the inspector will assess if it is
a complete match or a partial matach and take appropriate actions then."""

import logging
from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError

from src.blueprints import URI, RawDatafile, RawDataset, RawExperiment, RawProject
from src.helpers import IntrospectionConfig
from src.helpers.dataclass import get_object_name, get_object_parents, get_object_type
from src.helpers.enumerators import ObjectDict
from src.helpers.project_aware import check_projects_enabled_and_log_if_not
from src.overseers.overseer import Overseer

logger = logging.getLogger(__name__)


class Inspector:
    """The Inspector class holds a record of blocked objects and blocks
    objects as needed."""

    def __init__(self, overseer: Overseer, mytardis_setup: IntrospectionConfig) -> None:
        """Class initalisation"""
        self.overseer = overseer
        self.blocked: Dict[str, List[Optional[str]]] = {}
        self.blocked["project"] = []
        self.blocked["experiment"] = []
        self.blocked["dataset"] = []
        self.blocked["datafile"] = []
        if mytardis_setup:
            self.projects_enabled = mytardis_setup.projects_enabled
            self.objects_with_ids = mytardis_setup.objects_with_ids

    def is_blocked(
        self,
        raw_object: RawProject | RawExperiment | RawDataset | RawDatafile,
    ) -> bool:
        """Helper function to look up the lists of blocked objects and see if an
        object is in the blocked lists"""
        object_type = get_object_type(raw_object)
        if not object_type:
            logger.warning(
                (
                    f"Attempting to see if object: {raw_object} is "
                    "blocked, but object is not of a type found in MyTardis."
                )
            )
            return True  # Default to blocking the object
        if object_type["name"] in self.blocked[object_type["type"]]:
            return True
        return False

    def is_blocked_by_parents(
        self, raw_object: RawExperiment | RawDataset | RawDatafile
    ) -> bool:
        """Helper funtion to look up the lists of blocked objects to see if an
        object's parents are in the blocked lists and thus should be blocked."""
        object_type = get_object_type(raw_object)
        if not object_type:
            logger.warning(
                (
                    f"Attempting to see if object: {raw_object} is "
                    "blocked by its parents, but object is not of a "
                    "type found in MyTardis."
                )
            )
            return True  # Default to blocking the object
        parents = get_object_parents(raw_object)
        if parents:
            for parent in parents:
                if (
                    object_type["parent"]
                    and parent in self.blocked[object_type["parent"]]
                ):
                    return True
        return False

    def __validate_objects_returned_from_mytardis(
        self, object_type: ObjectDict, objects: List[Any]
    ) -> List[Tuple[URI, List[str]]] | None:
        """Helper function to check that a sensible return comes back from
        the overseer and logs errors if not."""
        return_list = []
        for obj in objects:
            try:
                uri = URI(obj["resource_uri"])
            except ValidationError:
                logger.warning(
                    ("Malformed return from get_objects. Returned " f"value is {obj}"),
                    exc_info=True,
                )
                continue
            values = []
            for key in object_type["match_keys"]:
                values.append(str(obj[key]))
            return_list.append((uri, values))
        if not return_list:
            return None
        return return_list

    def __get_objects_that_may_match_from_mytardis(
        self,
        raw_object: RawProject | RawExperiment | RawDataset | RawDatafile,
    ) -> List[Tuple[URI, List[str]]] | None:
        """Function that attempts to match the IDs and name against
        objects in the MyTardis database"""
        return_list: List[Tuple[URI, List[str]]] = []
        object_type = get_object_type(raw_object)
        object_name = get_object_name(raw_object)
        if not object_type:
            logger.warning(
                (
                    "Unable to identify the type of object passed to be matched. "
                    f"Object passed = {raw_object}"
                )
            )
            return None
        if not object_name:
            logger.warning(
                (
                    "Unable to identify the name of the object passed to be "
                    f"matched. Object passed = {raw_object}"
                )
            )
            return None
        if object_type["type"] == "project":
            if not check_projects_enabled_and_log_if_not(self):
                return None
        search_list = []
        if not isinstance(raw_object, RawDatafile):
            if raw_object.persistent_id:
                search_set.add(raw_object.persistent_id)
            if raw_object.alternate_ids:
                search_list.append(*raw_object.alternate_ids)
        search_list.append(object_name)
        objects: List[Dict[Any, Any]] = []
        for search in search_list:
            found_objs = self.overseer.get_objects(
                object_type["search_type"].value, search
            )
            if found_objs:
                objects.append(*found_objs)
        objects = list(set(objects))
        if objects:
            validated_objs = self.__validate_objects_returned_from_mytardis(
                object_type,
                objects,
            )
            if validated_objs:
                return_list.append(*validated_objs)
        if return_list:
            return return_list
        return None

    def block_object(
        self, raw_object: RawDatafile | RawDataset | RawExperiment | RawProject
    ) -> None:
        """helper function to block an object by adding it's name and
        identifieres to the blocked dictionary"""
        object_type = get_object_type(raw_object)
        if not object_type:
            error_message = (
                "Unable to determine the object type for the object to be blocked. "
                f"Object passed is: {raw_object}"
            )
            logger.error(error_message)
            raise ValueError(error_message)
        self.blocked[object_type["type"]].append(get_object_name(raw_object))
        if not isinstance(raw_object, RawDatafile):
            if raw_object.persistent_id:
                self.blocked[object_type["type"]].append(raw_object.persistent_id)
            if raw_object.alternate_ids:
                for alt_id in raw_object.alternate_ids:
                    self.blocked[object_type["type"]].append(alt_id)

    def match_or_block_object(  # pylint: disable=too-many-return-statements
        self,
        raw_object: RawProject | RawExperiment | RawDataset | RawDatafile,
    ) -> URI | None:
        """Function to try to match a raw object with an existing object in
        MyTardis. If a full match is found, return the URI. If a partial match
        is found, block the object for furher investigation and log. If no
        match is foune return None.
        Later calls to the is_blocked function should be made to see if the
        object is available to be ingested."""
        object_type = get_object_type(raw_object)
        object_name = get_object_name(raw_object)
        if not object_type:
            logger.warning(
                (
                    "Unable to identify the type of object passed to be matched. "
                    f"Object passed = {raw_object}"
                )
            )
            return None
        if not object_name:
            logger.warning(
                (
                    "Unable to identify the name of the object passed to be "
                    f"matched. Object passed = {raw_object}"
                )
            )
            return None
        if object_type["type"] == "project":
            if not check_projects_enabled_and_log_if_not(self):
                return None
        if not isinstance(raw_object, RawProject) and self.is_blocked_by_parents(
            raw_object
        ):
            self.block_object(raw_object)
            return None
        potential_matches = self.__get_objects_that_may_match_from_mytardis(raw_object)
        if not potential_matches:
            return None
        for match in potential_matches:
            matched_keys = True
            raw_object_dict = raw_object.dict(by_alias=True)
            for key in object_type["match_keys"]:
                if raw_object_dict[key] not in match:
                    matched_keys = False
            if matched_keys:
                return match[0]  # The URI
        self.block_object(raw_object)
        return None
