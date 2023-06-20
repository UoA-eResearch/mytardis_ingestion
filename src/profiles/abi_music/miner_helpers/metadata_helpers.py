import json
import copy

from pathlib import Path
from src.profiles.abi_music import abi_music_consts as amc
from typing import Optional, Any, Dict

def write_metadata_file(
    fp: Path,
    metadata: Dict[str, Any],
    ) -> None:
        with fp.open("w") as outfile:
            json.dump(metadata, outfile, ensure_ascii=False)

def add_schema_to_metadata(
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
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