"""
Ingestion profile for the "ABI MuSIC" Microscope
"""

from src.extraction.metadata_extractor import IMetadataExtractor
from src.profiles.abi_music.parsing import ABIMusicExtractor
from src.profiles.profile_base import IProfile


class AbiMusicProfile(IProfile):
    """Profile defining the ingestion behaviour for the 'ABI MuSIC' Microscope"""

    def __init__(self) -> None:
        pass

    def get_extractor(self) -> IMetadataExtractor:
        return ABIMusicExtractor()


def load_profile(version: str) -> IProfile:
    """Entry point for the profile - returns the profile corresponding to the requested version"""
    match version:
        case "v1":
            return AbiMusicProfile()
        case _:
            raise ValueError(f"Unknown version '{version}' for 'ABI MuSIC' profile")
