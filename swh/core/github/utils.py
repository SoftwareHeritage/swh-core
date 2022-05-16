# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import re
from typing import Optional

import requests

GITHUB_PATTERN = re.compile(r"https?://github.com/(?P<user_repo>.*)")


def _url_github_html(user_repo: str) -> str:
    """Given the user repo, returns the expected github html url."""
    return f"https://github.com/{user_repo}"


def _url_github_api(user_repo: str) -> str:
    """Given the user_repo, returns the expected github api url."""
    return f"https://api.github.com/repos/{user_repo}"


def _sanitize_github_url(url: str) -> str:
    """Sanitize github url."""
    return url.lower().rstrip("/").rstrip(".git").rstrip("/")


def get_canonical_github_origin_url(url: str) -> Optional[str]:
    """Retrieve canonical github url out of an url if any or None otherwise.

    This triggers an anonymous http request to the github api url to determine the
    canonical repository url.

    """
    url_ = url.lower()

    match = GITHUB_PATTERN.match(url_)
    if not match:
        return url

    user_repo = _sanitize_github_url(match.groupdict()["user_repo"])
    response = requests.get(_url_github_api(user_repo))
    if response.status_code != 200:
        return None
    data = response.json()
    return data["html_url"]
