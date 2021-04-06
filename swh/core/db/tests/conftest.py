import os

from hypothesis import HealthCheck

os.environ["LC_ALL"] = "C.UTF-8"

# we use getattr here to keep mypy happy regardless hypothesis version
function_scoped_fixture_check = (
    [getattr(HealthCheck, "function_scoped_fixture")]
    if hasattr(HealthCheck, "function_scoped_fixture")
    else []
)
