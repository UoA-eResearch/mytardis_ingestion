"""Definitions of fundamental data types used when interacting with MyTardis.

Note that the specifications of the MyTardis objects are not included here; see
objects.py for that."""

from typing import Literal

HttpRequestMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]
