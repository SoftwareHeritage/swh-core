# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging

from systemd.journal import JournalHandler as _JournalHandler, send

try:
    from celery import current_task
except ImportError:
    current_task = None


EXTRA_LOGDATA_PREFIX = 'swh_'


def db_level_of_py_level(lvl):
    """convert a log level of the logging module to a log level suitable for the
    logging Postgres DB

    """
    return logging.getLevelName(lvl).lower()


def get_extra_data(record, task_args=True):
    """Get the extra data to insert to the database from the logging record"""
    log_data = record.__dict__

    extra_data = {k[len(EXTRA_LOGDATA_PREFIX):]: v
                  for k, v in log_data.items()
                  if k.startswith(EXTRA_LOGDATA_PREFIX)}

    args = log_data.get('args')
    if args:
        extra_data['logging_args'] = args

    # Retrieve Celery task info
    if current_task and current_task.request:
        extra_data['task'] = {
            'id': current_task.request.id,
            'name': current_task.name,
        }
        if task_args:
            extra_data['task'].update({
                'kwargs': current_task.request.kwargs,
                'args': current_task.request.args,
            })

    return extra_data


def flatten(data, separator='_'):
    """Flatten the data dictionary into a flat structure"""
    def inner_flatten(data, prefix):
        if isinstance(data, dict):
            for key, value in data.items():
                yield from inner_flatten(value, prefix + [key])
        elif isinstance(data, (list, tuple)):
            for key, value in enumerate(data):
                yield from inner_flatten(value, prefix + [str(key)])
        else:
            yield prefix, data

    for path, value in inner_flatten(data, []):
        yield separator.join(path), value


def stringify(value):
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
        """
        try:
            extra_data = flatten(get_extra_data(record, task_args=False))
            extra_data = {
                (EXTRA_LOGDATA_PREFIX + key).upper(): stringify(value)
                for key, value in extra_data
            }
            msg = self.format(record)
            pri = self.mapPriority(record.levelno)
            send(msg,
                 PRIORITY=format(pri),
                 LOGGER=record.name,
                 THREAD_NAME=record.threadName,
                 CODE_FILE=record.pathname,
                 CODE_LINE=record.lineno,
                 CODE_FUNC=record.funcName,
                 **extra_data)
        except Exception:
            self.handleError(record)
