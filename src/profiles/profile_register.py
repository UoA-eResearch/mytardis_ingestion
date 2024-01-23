"""
A register for all ingestion profiles that can be loaded, and the entry point
for loading profiles.
"""

import importlib

from src.profiles.profile_base import IProfile

_PROFILE_ROOT = "src.profiles."

"""
This is the record of all profiles, and the location of the module
which is the profile's entry point. The module is expected to contain
a function called 'load_profile()' which takes a version, and returns
an IProfile object.

To register a new profile, add an entry here.
"""
_PROFILES = {
    "abi_music": "abi_music.profile",
    "idw": "idw.profile",
}


def get_profile_register() -> dict[str, str]:
    """Get a register containing all the profiles and their locations."""
    return _PROFILES


def get_profile_names() -> list[str]:
    """Get a list containing the name of every profile"""
    return list(get_profile_register().keys())


def load_profile(name: str, version: str) -> IProfile:
    """Load an ingestion profile corresponding to 'name' and 'version'"""
    profile_location = get_profile_register().get(name)
    if profile_location is None:
        raise ValueError(f"Unknown ingestion profile '{name}'")

    profile_path = _PROFILE_ROOT + profile_location

    try:
        module = importlib.import_module(profile_path)
    except ModuleNotFoundError as e:
        raise RuntimeError(
            f"Failed to load a profile module from {profile_path} while loading profile {name}"
        ) from e

    if not hasattr(module, "get_profile"):
        raise ImportError(
            f"The module for profile {name} has no 'get_profile()' function"
        )

    try:
        profile: IProfile = module.get_profile(version)
    except Exception as e:
        raise ImportError(
            f"An error occurred while loading the profile '{name}'"
        ) from e

    return profile
