"""Used to store and track the progress and history of the processes in the
extraction plant.
"""

# ---Imports
import logging
from pathlib import Path
from typing import Dict, List, Union

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class OutputManager:
    """Manage output results, directories to ignore, files to ignore, and metadata files to ingest.

    In the output_dict, there are three keys - success, ignored, and issues. The key in the dict
    has a corresponding list of entries expressed in a dict where each entry has a file or folder
    as the value for the 'value' subkey, a process (prospecting/mining/beneficiating) where the
    entry was made for the 'process' subkey, and notes pertaining to the value in the 'notes'
    subkey.

    The dirs_to_ignore and files_to_ignore are used in mining and/or beneficiating where the
    files/folders have been tagged from the prospector.

    Attributes:
        output_dict (Dict): Stores the output results.
        dirs_to_ignore (List): List of dirs to ignore.
        files_to_ignore (List): List of files to ignore.
        metadata_files_to_ingest_dict (Dict): Stores the metadata files to ingest.
    """

    def __init__(
        self,
    ) -> None:
        self.output_dict: Dict[str, List[Dict[str, str]]] = {
            "success": [],
            "ignored": [],
            "issues": [],
        }
        self.dirs_to_ignore: List[Path] = []
        self.files_to_ignore: List[Path] = []
        self.metadata_files_to_ingest_dict: Dict[str, List[Path]] = {
            "project": [],
            "experiment": [],
            "dataset": [],
            "datafile": [],
        }

    def add_success_entry_to_dict(  # pylint: disable=missing-function-docstring
        self,
        value: Union[str, Path],
        process: str,
        note: str,
    ) -> None:
        if isinstance(value, Path):
            value = value.as_posix()
        self.output_dict["success"].append(
            {
                "value": value,
                "process": process,
                "notes": note,
            }
        )

    def add_ignored_entry_to_dict(  # pylint: disable=missing-function-docstring
        self,
        value: Union[str, Path],
        process: str,
        note: str,
    ) -> None:
        if isinstance(value, Path):
            value = value.as_posix()
        self.output_dict["ignored"].append(
            {
                "value": value,
                "process": process,
                "notes": note,
            }
        )

    def add_issues_entry_to_dict(  # pylint: disable=missing-function-docstring
        self,
        value: Union[str, Path],
        process: str,
        note: str,
    ) -> None:
        if isinstance(value, Path):
            value = value.as_posix()
        self.output_dict["issues"].append(
            {
                "value": value,
                "process": process,
                "notes": note,
            }
        )

    def add_dir_to_ignore(  # pylint: disable=missing-function-docstring
        self,
        directory: Path,
    ) -> None:
        self.dirs_to_ignore.append(directory)

    def add_file_to_ignore(  # pylint: disable=missing-function-docstring
        self,
        file_path: Path,
    ) -> None:
        self.files_to_ignore.append(file_path)

    def add_files_to_ignore(  # pylint: disable=missing-function-docstring
        self,
        files: List[Path],
    ) -> None:
        self.files_to_ignore.extend(files)

    def add_metadata_file_to_ingest(  # pylint: disable=missing-function-docstring
        self,
        file_path: Path,
        dataclass: str,
    ) -> None:
        self.metadata_files_to_ingest_dict[dataclass].append(file_path)

    def add_metadata_files_to_ingest(  # pylint: disable=missing-function-docstring
        self,
        files: List[Path],
        dataclass: str,
    ) -> None:
        self.metadata_files_to_ingest_dict[dataclass].extend(files)
