# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from __future__ import annotations

import datetime
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Tuple

from systemd.journal import JournalHandler as _JournalHandler
from systemd.journal import send

try:
    from celery import current_task
except ImportError:
    current_task = None


EXTRA_LOGDATA_PREFIX = "swh_"
LOGGED_TASK_KWARGS = ("url", "instance")


def db_level_of_py_level(lvl):
    """convert a log level of the logging module to a log level suitable for the
    logging Postgres DB

    """
    return logging.getLevelName(lvl).lower()


def get_extra_data(record: logging.LogRecord) -> Dict[str, Any]:
    """Get the extra data to send to the log handler from the logging record.

    This gets the following data:
      - all fields in the record data starting with `EXTRA_LOGDATA_PREFIX`
      - arguments to the logging call (which can either be a tuple, or a dict
        if the arguments were named)
      - if this is called within a celery task, the following data:
        - the (uu)id of the task
        - the name of the task
        - any task keyword arguments named for values in `LOGGED_TASK_KWARGS`
    """
    log_data = record.__dict__

    extra_data = {
        k[len(EXTRA_LOGDATA_PREFIX) :]: v
        for k, v in log_data.items()
        if k.startswith(EXTRA_LOGDATA_PREFIX)
    }

    args = log_data.get("args")
    if args:
        extra_data["logging_args"] = args

    # Retrieve Celery task info
    if current_task and current_task.request:
        extra_data["task"] = {
            "id": current_task.request.id,
            "name": current_task.name,
        }

        for task_arg in LOGGED_TASK_KWARGS:
            if task_arg in current_task.request.kwargs:
                try:
                    value = stringify(current_task.request.kwargs[task_arg])
                except Exception:
                    continue

                extra_data["task"][f"kwarg_{task_arg}"] = value

    return extra_data


def flatten(data: Any, separator: str = "_") -> Generator[Tuple[str, Any], None, None]:
    """Flatten the data dictionary into a flat structure"""

    def inner_flatten(
        data: Any, prefix: List[str]
    ) -> Generator[Tuple[List[str], Any], None, None]:
        if isinstance(data, dict):
            if all(isinstance(key, str) for key in data):
                for key, value in data.items():
                    yield from inner_flatten(value, prefix + [key])
            else:
                yield prefix, str(data)
        elif isinstance(data, (list, tuple)):
            for key, value in enumerate(data):
                yield from inner_flatten(value, prefix + [str(key)])
        else:
            yield prefix, data

    for path, value in inner_flatten(data, []):
        yield separator.join(path), value


def stringify(value: Any) -> str:
    """Convert value to string"""
    if isinstance(value, datetime.datetime):
        return value.isoformat()

    return str(value)


class JournalHandler(_JournalHandler):
    def emit(self, record):
        """Write `record` as a journal event.

        MESSAGE is taken from the message provided by the user, and PRIORITY,
        LOGGER, THREAD_NAME, CODE_{FILE,LINE,FUNC} fields are appended
        automatically. In addition, record.MESSAGE_ID will be used if present.

        This also records all the extra data fetched by `get_extra_data`.
        """
        try:
            extra_data = flatten(get_extra_data(record))
            extra_data = {
                (EXTRA_LOGDATA_PREFIX + key).upper(): stringify(value)
                for key, value in extra_data
            }
            msg = self.format(record)
            pri = self.mapPriority(record.levelno)
            send(
                msg,
                PRIORITY=format(pri),
                LOGGER=record.name,
                THREAD_NAME=record.threadName,
                CODE_FILE=record.pathname,
                CODE_LINE=record.lineno,
                CODE_FUNC=record.funcName,
                **extra_data,
            )
        except Exception:
            self.handleError(record)


try:
    from aiohttp.web_log import AccessLogger
except ImportError:
    pass
else:

    if TYPE_CHECKING:
        from aiohttp.web_request import BaseRequest
        from aiohttp.web_response import StreamResponse

    class FilteredIPAccessLogger(AccessLogger):
        """Don't log successful requests from the set of IP addresses set in
        :envvar:``SWH_AIOHTTP_ACCESSLOG_IGNORE_IPS`` (comma-separated)"""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self.ignored_ips: List[str] = []
            env_value = os.environ.get("SWH_AIOHTTP_ACCESSLOG_IGNORE_IPS")
            if env_value:
                self.ignored_ips = env_value.split(",")

        def log(
            self, request: BaseRequest, response: StreamResponse, time: float
        ) -> None:
            if (
                request
                and request.remote in self.ignored_ips
                and response
                and response.status < 400
            ):
                return
            super().log(request, response, time)
