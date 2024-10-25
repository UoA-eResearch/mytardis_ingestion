"""Module for managing the loading and discovery of profiles."""

import importlib
import importlib.util
import logging
from pathlib import Path

from src.profiles.profile_base import IProfile

PROFILE_FILE_NAME = "profile.py"

logger = logging.getLogger(__name__)


class ProfileManager:
    """Class for managing the loading and discovery of profiles."""

    def __init__(self) -> None:
        pass

    def discover_profiles(self, dirs: list[Path]) -> list[str]:
        """Search the given directories for profile modules and load them."""
        for d in dirs:
            candidate_files = d.rglob(PROFILE_FILE_NAME)
            for f in candidate_files:
                spec = importlib.util.spec_from_file_location(
                    name=PROFILE_FILE_NAME, location=f
                )

                if spec is None:
                    logger.debug("Unable to load profile from %s", f)
                    continue
                if spec.loader is None:
                    logger.debug("Unable to load profile from %s; invalid loader", f)
                    continue

                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

        return []

    def load_profile(self, profile_name: str) -> IProfile:
        """Load the profile with the given name."""
        if profile_type := IProfile.profiles.get(profile_name):
            return profile_type()

        raise ValueError(f"Profile {profile_name} not found")

    def list_profiles(self) -> list[str]:
        """List the available profiles."""
        return list(IProfile.profiles.keys())
