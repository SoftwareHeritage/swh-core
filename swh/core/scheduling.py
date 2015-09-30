# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import celery


class Task(celery.Task):
    """a schedulable task (abstract class)

    Sub-classes must implement the run() method

    Current implementation is based on Celery. See
    http://docs.celeryproject.org/en/latest/reference/celery.app.task.html for
    how to use tasks once instantiated

    """

    abstract = True

    def run(self, *args, **kwargs):
        raise NotImplementedError('tasks must implement the run() method')
