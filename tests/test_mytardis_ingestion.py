# pylint: disable=missing-module-docstring,missing-function-docstring
from src import __version__


def test_version():
    assert __version__ == "0.2.0"
