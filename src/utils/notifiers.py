"""Helpers for notifying the progress of operations"""

from typing import Callable, Optional


class ProgressUpdater:
    """Helper class used to communicate progress of long-running operations.

    Handling of progress updates is done by setting handlers for the init and update events.
    This class is used to decouple the progress update logic from the usage of the updates.
    e.g. the progress can be updated in a console, a GUI, or a web interface, without the
    sender of the progress updates needing to know about these endpoints.
    """

    InitHandler = Callable[[str, Optional[int]], None]
    UpdateHandler = Callable[[str, int], None]

    def __init__(self) -> None:
        self._init_handler: Optional[ProgressUpdater.InitHandler] = None
        self._update_handler: Optional[ProgressUpdater.UpdateHandler] = None

    def init(self, name: str, total: Optional[int] = None) -> None:
        """Initialise the progress updater with a name and a total number of steps, if known."""
        if self._init_handler is None:
            raise ValueError("No handler set for progress updater")
        self._init_handler(name, total)

    def on_init(self, handler: InitHandler) -> None:
        """Set the handler for the initialisation event."""
        self._init_handler = handler

    def update(self, name: str, increment: int = 1) -> None:
        """Update the progress with the given increment."""
        if self._update_handler is None:
            raise ValueError("No update handler set for progress updater")
        self._update_handler(name, increment)

    def on_update(self, handler: UpdateHandler) -> None:
        """Set the handler for the update event."""
        self._update_handler = handler
