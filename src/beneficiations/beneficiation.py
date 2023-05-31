"""Part of the Extraction Plant where generated metadata files are parsed
into raw dataclasses. 
"""


# ---Imports
import logging

from src.beneficiations import beneficiation_consts as bc
from src.beneficiations.parsers import json_parser
from src.utils.ingestibles import IngestibleDataclasses

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Beneficiation:
    """
    This class provides a means of beneficiating project, experiment, dataset and datafile files.
    It takes a list of files in the specified format and parses them into a dictionary of objects.
    """

    def __init__(
        self,
    ) -> None:
        pass

    def beneficiate(
        self,
        proj_files: list[str],
        expt_files: list[str],
        dset_files: list[str],
        dfile_files: list[str],
        file_format: str,
    ) -> IngestibleDataclasses:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            proj_files (list[str]): List of project file paths.
            expt_files (list[str]): List of experiment file paths.
            dset_files (list[str]): List of dataset file paths.
            dfile_files (list[str]): List of datafile file paths.
            file_format (str): File format of the input files.

        Raises:
            Exception: Raised when file format not supported.

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.
        """
        ingestible_dataclasses = IngestibleDataclasses()
        if file_format == bc.JSON_FORMAT:
            parser = json_parser.Parser()
            ingestible_dataclasses = parser.parse(
                proj_files, expt_files, dset_files, dfile_files
            )
        elif file_format == bc.YAML_FORMAT:
            logger.error(
                "Beneficiation file format is recognised but yet to be implemented: {0}".format(
                    file_format
                )
            )
            raise Exception(
                "Beneficiation file format is recognised but yet to be implemented: {0}".format(
                    file_format
                )
            )
        else:
            logger.error(
                "Beneficiation file format not recognised: {0}".format(file_format)
            )
            raise Exception(
                "Beneficiation file format not recognised: {0}".format(file_format)
            )

        return ingestible_dataclasses
