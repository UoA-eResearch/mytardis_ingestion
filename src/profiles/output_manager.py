# pylint: disable=C0301

"""Used to store and track the progress and history of the processes in the
extraction plant.
"""

# ---Imports
import logging
from typing import Any

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

    The dirs_to_ignore and files_to_ignore are used in mining and/or beneficiating where files/folders
    from

    Attributes:
        output_dict (dict): Stores the output results.
        dirs_to_ignore (list): List of dirs to ignore.
        files_to_ignore (list): List of files to ignore.
        metadata_files_to_ingest_dict (dict): Stores the metadata files to ingest.
    """

    def __init__(
        self,
    ) -> None:
        """Initialize the OutputManager instance.

        Creates an `output_dict` to store output results, initializes empty lists for `dirs_to_ignore`
        and `files_to_ignore`, and sets up the `metadata_files_to_inggest_dict` with empty lists.

        Args:
            None

        Returns:
            None
        """
        self.output_dict = self._create_output_dict()
        self.dirs_to_ignore: list[str] = []
        self.files_to_ignore: list[str] = []
        self.metadata_files_to_ingest_dict = self._create_ingestion_dict()

    def add_success_entry_to_dict(
        self,
        value: str,
        process: str,
        note: str,
    ) -> None:
        """Add a success entry to the output dictionary.

        Args:
            value (str): The value to be added.
            process (str): The process associated with the entry.
            note (str): Additional notes pertaining to the entry.

        Returns:
            None
        """
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_SUCCESS_KEY].append(entry_subdict)

    def add_ignored_entry_to_dict(
        self,
        value: str,
        process: str,
        note: str,
    ) -> None:
        """Add an ignored entry to the output dictionary.

        Args:
            value (str): The value to be added.
            process (str): The process associated with the entry.
            note (str): Additional notes pertaining to the entry.

        Returns:
            None
        """
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_IGNORED_KEY].append(entry_subdict)

    def add_issues_entry_to_dict(
        self,
        value: str,
        process: str,
        note: str,
    ) -> None:
        """Add an issues entry to the output dictionary.

        Args:
            value (str): The value to be added.
            process (str): The process associated with the entry.
            note (str): Additional notes pertaining to the entry.

        Returns:
            None
        """
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_ISSUES_KEY].append(entry_subdict)

    def add_dir_to_ignore(
        self,
        directory: str,
    ) -> None:
        """
        Add a directory to the list of directories to ignore.

        This function appends the provided directory path to a list of directories
        that should be ignored in some processing.

        Args:
            directory (str): The directory path to be ignored.

        Returns:
            None
        """
        self.dirs_to_ignore.append(directory)

    def add_file_to_ignore(
        self,
        fp: str,
    ) -> None:
        """Add a file to the list of files to ignore.

        Args:
            fp (str): The file path to be ignored.

        Returns:
            None
        """
        self.files_to_ignore.append(fp)

    def add_files_to_ignore(
        self,
        files: list[str],
    ) -> None:
        """Add a list of files to the list of files to ignore.

        Args:
            files (list): List of file paths to be ignored.

        Returns:
            None
        """
        self.files_to_ignore.extend(files)

    def add_metadata_file_to_ingest(
        self,
        file: str,
        dataclass: str,
    ) -> None:
        """Add a metadata file to the list of files to ingest for a specific dataclass.

        Args:
            file (str): The file path to be added for ingestion.
            dataclass (str): The dataclass associated with the metadata file.

        Returns:
            None
        """
        self.metadata_files_to_ingest_dict[dataclass].append(file)

    def add_metadata_files_to_ingest(
        self,
        files: list[str],
        dataclass: str,
    ) -> None:
        """Add a list of metadata files to the list of files to ingest for a specific dataclass.

        Args:
            files (list): List of file paths to be added for ingestion.
            dataclass (str): The dataclass associated with the metadata files.

        Returns:
            None
        """
        self.metadata_files_to_ingest_dict[dataclass].extend(files)

    def _create_output_dict(
        self,
    ) -> dict[str, list[dict[str, str]]]:
        """Create an empty output dictionary.

        Returns:
            dict: An empty output dictionary with keys for 'success', 'ignored', and 'issues'.
        """
        out_dict = ph.create_output_dict()
        for key in out_dict.keys():
            out_dict[key] = []
        return out_dict

    def _create_output_subdict(
        self,
    ) -> dict[str, Any]:
        """Create an empty output sub-dictionary.

        Returns:
            dict: An empty output sub-dictionary.
        """
        return ph.create_output_subdict()

    def _create_ingestion_dict(
        self,
    ) -> dict[str, list[Any]]:
        """Create an empty metadata ingestion dictionary.

        Returns:
            dict: An empty metadata ingestion dictionary with keys for each dataclass.
        """
        ingestion_dict = ph.create_ingestion_dict()
        for key in ingestion_dict.keys():
            ingestion_dict[key] = []
        return ingestion_dict
