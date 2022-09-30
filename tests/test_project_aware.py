# Pylint: disable=missing-function-docstring

import logging

from src.helpers.project_aware import log_if_projects_disabled


class ProjectAwareClass:
    """A minimal test class to assert that the project aware function
    is working correctly."""

    def __init__(self, projects_enabled: bool) -> None:
        self.projects_enabled = projects_enabled

    def test_function(self) -> bool:
        projects_enabled = log_if_projects_disabled(self)
        return projects_enabled


def test_project_aware_with_projects_enabled():
    test_class = ProjectAwareClass(projects_enabled=True)
    assert test_class.test_function() is True


def test_project_aware_with_projects_disabled(
    caplog,
):
    test_class = ProjectAwareClass(projects_enabled=False)
    caplog.set_level(logging.WARNING)
    warning_str = (
        "MyTardis is not currently set up to use projects. Please check settings.py "
        "and ensure that the 'projects' app is enabled. This may require rerunning "
        "migrations."
    )
    assert test_class.test_function() is False
    assert warning_str in caplog.text
