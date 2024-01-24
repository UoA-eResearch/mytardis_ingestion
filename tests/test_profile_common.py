"""
Tests for common code related to profiles (not specific profiles)
"""

# pylint: disable=missing-function-docstring

# import pytest

import mock
import pytest

from src.profiles.profile_register import (
    get_profile_names,
    get_profile_register,
    load_profile,
)


def test_get_profile_register() -> None:
    profiles = get_profile_register()
    assert len(profiles) > 0, "Should return some profiles"


def test_get_profile_names() -> None:
    names = get_profile_names()
    assert len(names) > 0, "Should return some names"


@pytest.fixture(name="mock_profile_info")
def fixture_mock_profile_info() -> mock.MagicMock:
    return mock.MagicMock(return_value={"mock_profile": "mock_profile.module.path"})


@pytest.fixture(name="mock_profile_module")
def fixture_mock_profile_module() -> mock.MagicMock:
    mock_profile = mock.MagicMock()
    type(mock_profile).name = mock.PropertyMock(return_value="mock_profile")

    mock_module = mock.MagicMock()
    mock_module.configure_mock(**{"get_profile.return_value": mock_profile})

    return mock_module


def test_load_profile(
    mock_profile_info: mock.MagicMock, mock_profile_module: mock.MagicMock
) -> None:
    with mock.patch(
        "src.profiles.profile_register.get_profile_register", mock_profile_info
    ):
        with mock.patch(
            "importlib.import_module", mock.MagicMock(return_value=mock_profile_module)
        ):
            profile = load_profile("mock_profile", "v1")

            mock_profile_module.get_profile.assert_called_once_with("v1")
            assert profile.name == "mock_profile"
