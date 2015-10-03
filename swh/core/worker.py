# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from celery import Celery
from celery.signals import setup_logging
from kombu import Exchange, Queue

from swh.core.config import load_named_config
from swh.core.logger import PostgresHandler

CONFIG_NAME = 'worker.ini'
DEFAULT_CONFIG = {
    'task_broker': ('str', 'amqp://guest@localhost//'),
    'task_modules': ('list[str]', []),
    'task_queues': ('list[str]', []),
    'task_soft_time_limit': ('int', 0),
}


@setup_logging.connect
def setup_log_handler(loglevel=None, logfile=None, format=None,
                      colorize=None):
    """Setup logging according to Software Heritage preferences"""

    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)

    pg = PostgresHandler(CONFIG['log_db'])
    pg.setFormatter(logging.Formatter(format))
    pg.setLevel(logging.DEBUG)

    root_logger.addHandler(console)
    root_logger.addHandler(pg)

    celery_logger = logging.getLogger('celery')
    celery_logger.setLevel(logging.INFO)

    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.CRITICAL)

    swh_logger = logging.getLogger('swh')
    swh_logger.setLevel(logging.DEBUG)

# Load the Celery config
CONFIG = load_named_config(CONFIG_NAME, DEFAULT_CONFIG)

# Celery Queues
CELERY_QUEUES = [Queue('celery', Exchange('celery'), routing_key='celery')]

for queue in CONFIG['task_queues']:
    CELERY_QUEUES.append(Queue(queue, Exchange(queue), routing_key=queue))

# Instantiate the Celery app
app = Celery()
app.conf.update(
    # The broker
    BROKER_URL=CONFIG['task_broker'],
    # Timezone configuration: all in UTC
    CELERY_ENABLE_UTC=True,
    CELERY_TIMEZONE='UTC',
    # Imported modules
    CELERY_IMPORTS=CONFIG['task_modules'],
    # Time (in seconds, or a timedelta object) for when after stored task
    # tombstones will be deleted.
    CELERY_TASK_RESULT_EXPIRES=3600,
    # Late ack means the task messages will be acknowledged after the task has
    # been executed, not just before, which is the default behavior.
    CELERY_ACKS_LATE=True,
    # A string identifying the default serialization method to use.
    # Can be pickle (default), json, yaml, msgpack or any custom serialization
    # methods that have been registered with kombu.serialization.registry
    CELERY_ACCEPT_CONTENT=['msgpack', 'pickle', 'json'],
    # If True the task will report its status as “started”
    # when the task is executed by a worker.
    CELERY_TRACK_STARTED=True,
    # Default compression used for task messages. Can be gzip, bzip2
    # (if available), or any custom compression schemes registered
    # in the Kombu compression registry.
    # CELERY_MESSAGE_COMPRESSION='bzip2',
    # Disable all rate limits, even if tasks has explicit rate limits set.
    # (Disabling rate limits altogether is recommended if you don’t have any
    # tasks using them.)
    CELERY_DISABLE_RATE_LIMITS=True,
    # Task hard time limit in seconds. The worker processing the task will be
    # killed and replaced with a new one when this is exceeded.
    # CELERYD_TASK_TIME_LIMIT=3600,
    # Task soft time limit in seconds.
    # The SoftTimeLimitExceeded exception will be raised when this is exceeded.
    # The task can catch this to e.g. clean up before the hard time limit
    # comes.
    CELERYD_TASK_SOFT_TIME_LIMIT=CONFIG['task_soft_time_limit'],
    # Task routing
    CELERY_ROUTES={
        'swh.loader.git.tasks.LoadGitRepository': {
            'queue': 'swh_loader_git',
        },
        'swh.loader.git.tasks.LoadGitHubRepository': {
            'queue': 'swh_loader_git',
        },
        'swh.cloner.git.worker.tasks.execute_with_measure': {
            'queue': 'swh_cloner_git',
        },
    },
    # Task queues this worker will consume from
    CELERY_QUEUES=CELERY_QUEUES,
    # Allow pool restarts from remote
    CELERYD_POOL_RESTARTS=True,
)
