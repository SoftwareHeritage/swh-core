from hypothesis import settings
import pytest

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)

pytest_plugins = ["swh.core.github.pytest_plugin"]


@pytest.fixture
def swhmain():
    """Yield an instance of the main `swh` click command that cleans the added
    subcommands up on teardown."""
    from swh.core.cli import swh as _swhmain

    commands = _swhmain.commands.copy()
    aliases = _swhmain.aliases.copy()
    yield _swhmain
    _swhmain.commands = commands
    _swhmain.aliases = aliases
