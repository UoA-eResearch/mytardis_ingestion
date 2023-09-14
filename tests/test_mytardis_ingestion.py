# pylint: disable=missing-module-docstring,missing-function-docstring
# nosec assert_used
# flake8: noqa S101
from src import __version__


def test_version() -> None:
    assert __version__ == "0.8.0"  # nosec
