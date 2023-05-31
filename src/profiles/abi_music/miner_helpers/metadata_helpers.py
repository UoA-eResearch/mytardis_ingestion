import json
import copy
from src.profiles.abi_music import abi_music_consts as amc
from typing import Optional, Any

def write_metadata_file(
    fp: str,
    metadata: dict[str, Any],
    ) -> None:
        with open(fp, "w") as outfile:
            json.dump(metadata, outfile, ensure_ascii=False)


def add_schema_to_metadata(
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Add ABI schema to the given metadata.

        Args:
            self (object): The object instance.
            metadata (dict[str, Any]): Input metadata.

        Returns:
            dict[str, Any]: Metadata with ABI schema added.
        """
        mdata_with_schema = copy.deepcopy(metadata)
        mdata_with_schema["object_schema"] = amc.ABI_SCHEMA
        return mdata_with_schema