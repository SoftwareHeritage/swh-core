swh-core
========

core library for swh's modules:
- config parser
- hash computations
- serialization
- logging mechanism

Defines also a celery application to run concurrency tasks

Celery use
----------

### configuration file

worker.ini file which looks like:

    [main]
    task_broker = amqp://guest@localhost//
    task_modules = swh.loader.dir.tasks, swh.loader.tar.tasks, swh.loader.git.tasks
    task_queues = swh_loader_tar, swh_loader_git, swh_loader_dir
    task_soft_time_limit = 0

This file can be set in the following location:
- ~/.swh
- ~/.config/swh
- /etc/softwareheritage


### run celery worker

Sample command:

    celery worker --app=swh.core.worker \
                  --pool=prefork \
                  --autoscale=2,2 \
                  -Ofair \
                  --loglevel=info 2>&1 | tee -a swh-core-worker.log
