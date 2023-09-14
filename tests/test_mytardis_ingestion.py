# pylint: disable=missing-module-docstring,missing-function-docstring

from src import __version__


def test_version() -> None:
    assert __version__ == "0.8.0"
