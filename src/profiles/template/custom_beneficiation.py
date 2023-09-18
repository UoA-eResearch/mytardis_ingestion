# pylint: disable-all
# type: ignore
# This needs a lot of refactoring so disable checks

"""Metadata file parser module.

Main objective of this module is to map the metadata files to
a format accepted by the mytardis_ingestion
"""


# ---Imports
import copy
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.profiles import profile_consts as pc

# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class CustomBeneficiation(AbstractCustomBeneficiation):
    """A class to parse dataclass files into IngestibleDataclasses objects."""

    def __init__(
        self,
    ) -> None:
        pass

    def beneficiate(
        self,
        beneficiation_data: Any,
        ingestible_dclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        """Parse dataclass files into IngestibleDataclasses objects.

        Args:
            beneficiation_data (Any): Data that contains information about the dataclasses to parse
            ingestible_dclasses (IngestibleDataclasses): object that conatins parsed dataclasses

        Returns:
            IngestibleDataclasses: object containing parsed dataclass objects.
        """
        # Write rest of implementation here, use leading underscores for each method
        ing_dclasses: IngestibleDataclasses = copy.deepcopy(ingestible_dclasses)
        return ing_dclasses
