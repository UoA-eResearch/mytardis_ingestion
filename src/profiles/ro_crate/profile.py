"""
Ingestion profile for ingestions of RO-Crates
"""

from typing import Optional

from src.extraction.metadata_extractor import IMetadataExtractor
from src.profiles.profile_base import IProfile
from src.profiles.ro_crate._consts import PROFILE_VERSION as CRATE_PROFILE_VERSION
from src.profiles.ro_crate.ro_crate_parser import ROCrateExtractor


class ROCrateProfile(IProfile):
    """Profile defining the ingestion behaviour for metadata from RO-crates"""

    def __init__(self) -> None:
        pass

    @property
    def name(self) -> str:
        return "ro_crate"

    def get_extractor(self) -> IMetadataExtractor:
        return ROCrateExtractor()


def get_profile(version: Optional[str]) -> IProfile:
    """Entry point for the profile - returns the profile corresponding to the requested version"""
    if version == CRATE_PROFILE_VERSION or not version:
        return ROCrateProfile()
    raise ValueError(
        f"""Unknown version '{version}' for 'RO_Crate' profile
        \n Current RO_Crate Profile version is'{CRATE_PROFILE_VERSION}'."""
    )
