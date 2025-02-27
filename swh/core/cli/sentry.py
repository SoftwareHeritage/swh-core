#!/usr/bin/env python3
# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Callable, Dict, List

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group


def common_options(func):
    import functools

    @click.option(
        "--sentry-url",
        "-u",
        default="https://sentry.softwareheritage.org",
        show_default=True,
        help="Sentry URL",
    )
    @click.option(
        "--sentry-token",
        "-t",
        default=None,
        envvar="SENTRY_TOKEN",
        help=(
            "Bearer token required to communicate with Sentry API (can also be provided "
            "in SENTRY_TOKEN environment variable)"
        ),
        required=True,
    )
    @click.option(
        "--sentry-issue-number",
        "-i",
        help="Sentry issue number to extract origin URLs from its events",
        required=True,
    )
    @click.option(
        "--environment",
        "-e",
        default="",
        help="Filter on environment: production or staging, both are selected by default",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@swh_cli_group.group(name="sentry", context_settings=CONTEXT_SETTINGS)
def sentry():
    """Software Heritage tools for extracting data from the events associated to
    a Sentry issue using Sentry REST API."""
    pass


def _process_sentry_events_pages(
    sentry_url,
    sentry_token,
    sentry_issue_number,
    events_page_process_callback: Callable[[List[Dict[str, Any]]], None],
    full_sentry_responses: bool = False,
):
    import requests

    sentry_api_base_url = f"{sentry_url.rstrip('/')}/api/0"
    sentry_issue_events_url = (
        f"{sentry_api_base_url}/issues/{sentry_issue_number}/events/"
    )
    if full_sentry_responses:
        sentry_issue_events_url += "?full=true"
    while sentry_issue_events_url:
        response = requests.get(
            sentry_issue_events_url, headers={"Authorization": f"Bearer {sentry_token}"}
        )
        events = response.json()
        if not events:
            break
        events_page_process_callback(events)
        sentry_issue_events_url = response.links.get("next", {}).get("url")


@sentry.command(name="extract-origin-urls", context_settings=CONTEXT_SETTINGS)
@common_options
def extract_origin_urls(sentry_url, sentry_token, sentry_issue_number, environment):
    """Extract origin URLs from events.

    This command allows to extract origin URLs from Sentry events related to
    a Software Heritage loader and dumps them to stdout."""

    origin_urls = set()

    def _extract_origin_urls(events: List[Dict[str, Any]]):
        for event in events:
            tags = {tag["key"]: tag["value"] for tag in event.get("tags", [])}
            env_match = environment in tags.get("environment", "")
            if "swh.loader.origin_url" in tags and env_match:
                origin_urls.add(tags["swh.loader.origin_url"])

    _process_sentry_events_pages(
        sentry_url,
        sentry_token,
        sentry_issue_number,
        _extract_origin_urls,
    )

    for origin_url in sorted(origin_urls):
        click.echo(origin_url)


@sentry.command(name="extract-scheduler-tasks", context_settings=CONTEXT_SETTINGS)
@common_options
@click.option(
    "--policy",
    "-p",
    default="oneshot",
    type=click.Choice(["oneshot", "recurring"]),
    help="The scheduling policy to be used for the tasks",
)
def extract_scheduler_tasks(
    sentry_url, sentry_token, sentry_issue_number, environment, policy
):
    """Extract scheduler task parameters from events.

    This command allows to extract scheduler task parameters from Sentry events related to
    a Software Heritage scheduler task and dumps a CSV file to stdout that can be consumed
    by the CLI command:

    $ swh scheduler task schedule --columns type --columns kwargs --columns policy <csv_file>.
    """
    import csv
    import json
    import sys

    task_params = {}

    def _extract_scheduler_tasks(events):
        for event in events:
            celery_job = event.get("context", {}).get("celery-job", {})
            task_name = celery_job.get("task_name")
            task_param = celery_job.get("kwargs")
            if task_param:
                key = tuple([task_name] + list(str(v) for v in task_param.values()))
                task_params[key] = (task_name, task_param)

    _process_sentry_events_pages(
        sentry_url,
        sentry_token,
        sentry_issue_number,
        _extract_scheduler_tasks,
        full_sentry_responses=True,
    )

    csv_writer = csv.writer(sys.stdout)
    for task_type, task_param in sorted(
        task_params.values(), key=lambda p: p[1].get("url", "")
    ):
        csv_writer.writerow([task_type, json.dumps(task_param), policy])
