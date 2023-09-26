"""Used to store and track the progress and history of the processes in the
extraction plant.
"""

# ---Imports
import logging
from pathlib import Path
from typing import Any, Dict, List, Union

from src.profiles import profile_consts as pc
from src.profiles import profile_helpers as ph

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class OutputManager:
    """Manage output results, directories to ignore, files to ignore, and metadata files to ingest.

    In the output_dict, there are three keys - success, ignored, and issues. The key in the dict
    has a corresponding list of entries expressed in a dict where each entry has a file or folder
    as the value for the 'value' subkey, a process (prospecting/mining/beneficiating) where the entry
    was made for the 'process' subkey, and notes pertaining to the value in the 'notes' subkey.

    The dirs_to_ignore and files_to_ignore are used in mining and/or beneficiating where the files/folders
    have been tagged from the prospector.

    Attributes:
        output_dict (Dict): Stores the output results.
        dirs_to_ignore (List): List of dirs to ignore.
        files_to_ignore (List): List of files to ignore.
        metadata_files_to_ingest_dict (Dict): Stores the metadata files to ingest.
    """

    def __init__(
        self,
    ) -> None:
        self.output_dict = self._create_output_dict()
        self.dirs_to_ignore: List[Path] = []
        self.files_to_ignore: List[Path] = []
        self.metadata_files_to_ingest_dict = self._create_ingestion_dict()

    def add_success_entry_to_dict(
        self,
        value: Union[str, Path],
        process: str,
        note: str,
    ) -> None:
        if type(value) == Path:
            value = str(value)
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_SUCCESS_KEY].append(entry_subdict)

    def add_ignored_entry_to_dict(
        self,
        value: Union[str, Path],
        process: str,
        note: str,
    ) -> None:
        if type(value) == Path:
            value = str(value)
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_IGNORED_KEY].append(entry_subdict)

    def add_issues_entry_to_dict(
        self,
        value: Union[str, Path],
        process: str,
        note: str,
    ) -> None:
        if type(value) == Path:
            value = str(value)
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_ISSUES_KEY].append(entry_subdict)

    def add_dir_to_ignore(
        self,
        dir: Path,
    ) -> None:
        self.dirs_to_ignore.append(dir)

    def add_file_to_ignore(
        self,
        fp: Path,
    ) -> None:
        self.files_to_ignore.append(fp)

    def add_files_to_ignore(
        self,
        files: List[Path],
    ) -> None:
        self.files_to_ignore.extend(files)

    def add_metadata_file_to_ingest(
        self,
        file: Path,
        dataclass: str,
    ) -> None:
        self.metadata_files_to_ingest_dict[dataclass].append(file)

    def add_metadata_files_to_ingest(
        self,
        files: List[Path],
        dataclass: str,
    ) -> None:
        self.metadata_files_to_ingest_dict[dataclass].extend(files)

    def _create_output_dict(
        self,
    ) -> Dict[str, List[Dict[str, str]]]:
        out_dict = ph.create_output_dict()
        for key in out_dict.keys():
            out_dict[key] = []
        return out_dict

    def _create_output_subdict(
        self,
    ) -> Dict[str, Any]:
        return ph.create_output_subdict()

    def _create_ingestion_dict(
        self,
    ) -> Dict[str, List[Any]]:
        ingestion_dict = ph.create_ingestion_dict()
        for key in ingestion_dict.keys():
            ingestion_dict[key] = []
        return ingestion_dict
