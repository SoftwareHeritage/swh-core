.. _swh-core-db:

Common database utilities
=========================

The ``swh.core.db`` module offers a set of common (postgresql) database
handling utilities and features for other swh packages implementing a
`datastore`, aka a service responsible for providing a data store via a common
interface which can use a postgresql database as backend. Examples are
:mod:`swh.storage` or :mod:`swh.scheduler`.

Most of the time, this database-based data storage facility will depend on a data
schema (may be based on :mod:`swh.model` or not) and provide a unified interface
based on an Python class to abstract access to this datastore.

Some packages may implement only a postgresql backend, some may provide more
backends.

This :mod:`swh.core.db` only deals with the postgresql part and provides common
features and tooling to manage the database lifecycle in a consistent and
unified way among all the :mod:`swh` packages.

Command line tools
------------------

It comes with a few command line tools to manage the specific :mod:`swh`
package database.

As such, most of the database management cli commands require a configuration
file holding the database connection information, but database creation and
superuser-level initialization steps can be executed without this configuration
file, giving db connection parameters as command line arguments (this is
helpful because you generally don't want to use superuser-level credentials in
you configuration file for regular db access; these should only be used for the
database creation -- if any -- and parts of its initialization).


Database initialization
~~~~~~~~~~~~~~~~~~~~~~~

We are using the :mod:`swh.storage` package in this documentation as example of
a swh package providing database backends. One will be able to create,
initialize and upgrade databases for the :mod:`swh.storage` package using
simple commands. In this case, the default backend for this package is the main
storage postgresql backend (but this package provides more backends, which will
be illustrated later).

Creating and initializing the database can be done with or without any
configuration file.

To create the database and perform superuser initialization
steps (see below):

.. code-block:: bash

   $ swh db create storage --dbname=postgresql://superuser:passwd@localhost:5433/test-storage

This will create the database and run the superuser-level initialization steps.

If the database already exists, superuser level initialization steps can be executed with:

.. code-block:: bash

   $ swh db init-admin storage --dbname=postgresql://superuser:passwd@localhost:5433/test-storage

The non-superuser level initialization can be done with:

.. code-block:: bash

   $ swh db init storage --dbname=postgresql://user:passwd@localhost:5433/test-storage


.. hint::
   A simple way of testing these commands is to use `pifpaf <https://github.com/jd/pifpaf>`_.

   .. code-block:: bash

      $ eval `pifpaf run -- postgresql`
      $ swh db init-admin storage --dbname=$PIFPAF_URL
      storage:postgresql Database postgresql://localhost/postgres[...] initialized (admin)
      $ swh db init storage --dbname=$PIFPAF_URL
      storage:postgresql Database initialized (flavor default) at version 195
      $ pifpaf_stop


Configuration file
~~~~~~~~~~~~~~~~~~

All the `swh db` commands can use (or need) a configuration file.

Assuming the ``config.yml`` file existence:

.. code-block:: yaml

   storage:
     cls: postgresql
	 db: host=localhost, port=5433, dbname=test-storage, username=normal-user, password=pwd
	 objstorage:
	   cls: memory

then you can run:

.. code-block:: bash

   $ swh --config-file=config.yml db init storage
   DONE database for storage initialized (flavor default) at version 182

Note: you can define the ``SWH_CONFIG_FILENAME`` environment variable instead
of using the ``--config-name`` command line option shown above.

.. code-block:: bash

   $ export SWH_CONFIG_FILENAME=$PWD/config.yml
   $ swh db init storage
   DONE database for storage initialized (flavor default) at version 182


You can check the actual data model version of this database:

.. code-block:: bash

   $ export SWH_CONFIG_FILENAME=$PWD/config.yml
   $ swh db version storage
   module: storage
   flavor: default
   version: 182

as well as the migration history for the database:

.. code-block:: bash

   $ swh db version --all storage
   module: storage
   flavor: default
   182 [2022-02-11 15:08:31.806070+01:00] Work In Progress
   181 [2022-02-11 14:06:27.435010+01:00] Work In Progress


Database migration
~~~~~~~~~~~~~~~~~~

The database migration is done using the ``swh db upgrade`` command:

.. code-block:: bash

   $ swh db version storage

   module: storage:postgresql
   flavor: default
   current code version: 195
   version: 192

   $ swh db upgrade storage
   Migration to version 195 done


Multiple backends in a configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A configuration file can store more than one database backend entries.

.. code-block:: yaml

   storage:
     cls: pipeline
     steps:
       - cls: record_references
       - cls: blocking
         db: postgresql:///?service=blocking-proxy
       - cls: masking
         db: postgresql:///?service=masking-proxy-ro
         max_pool_conns: 10
         storage:
           cls: postgresql
           db: postgresql:///?service=storage
           objstorage:
             cls: remote
             url: http://nginx/rpc/objstorage/

   storage_masking_admin:
     pkg: storage
     cls: postgresql
     db: postgresql:///?service=masking-proxy-rw

In this configuration we have 3 database backends, the standard postgresql
based storage, plus 2 databases for the blocking and masking proxy. The
configuration for this later comes in 2 parts, because we want the connection
used for the masking proxy to be read-only, but we may also need to be able to
perform admin tasks from this configuration file.

Initializing the databases:

.. code-block:: bash

   $ swh db init-admin storage
   storage:postgresql Created database postgresql:///?service=storage
   $ swh db init-admin storage:blocking
   storage:postgresql Created database postgresql:///?service=blocking-proxy
   $ swh db init-admin storage_masking_admin
   storage:postgresql Created database postgresql:///?service=swh-masking-proxy

In the fist command, we do not specify which backend we want to initialize the
database for, so it will (recursively) pick the last one in the ``storage``
configuration structure.

We initialize the ``blocking`` database using the ``storage::blocking`` syntax.
The first part is both the name of the configuration section and the swh
package concerned. The second part is the backend ``cls`` registered for this
package. The ``swh db`` command will look for the configuration section in the
``storage`` structure which ``cls`` matches the given one.

Note: the backend ``cls`` entries are registered in the
``swh.<package>.classes`` entrypoint. Each ``swh`` package implementing
database backends will register them. See below for more details.

The third command is using an "aliased" configuration entry. In this form, the
configuration section is only meant to be used by `swh db` command to perform
administrative tasks. The name of the section can be arbitrary, but it must
explicitly have a ``pkg`` entry in addition to the ``cls`` and ``db`` ones.
This ``pkg`` is the name of the ``swh`` package to be used to look for the
``cls`` backend.


Implementation of a swh.core.db datastore
-----------------------------------------

To use this database management tooling, in a :mod:`swh` package, the following
conditions are expected:

- the package should provide an ``sql`` directory in its root namespace
  providing initialization sql scripts. Scripts should be named like
  ``nn-xxx.sql`` and are executed in order according to the ``nn`` integer
  value. Scripts having ``-superuser-`` in their name will be executed by the
  ``init-admin`` tool and are expected to require superuser access level,
  whereas scripts without ``-superuser-`` in their name will be executed by the
  ``swh db init`` command and are expected to require write access
  level (with no need for superuser access level).

- the package should provide a ``sql/upgrade`` directory with SQL migration
  scripts in its root namespace. Script names are expected to be of the form
  ``nnn.sql`` where `nnn` is the version to which this script does the
  migration from a database at version `nnn - 1`.

- the initialization and migration scripts should not create nor fill the
  metadata related tables (``dbversion`` and ``dbmodule``).

- the package should provide a ``get_datastore`` function in its root namespace
  returning an instance of the datastore object. Normally, this datastore
  object uses ``swh.core.db.BaseDb`` to interact with the actual database.

- The datastore object should provide a ``current_version`` attribute returning the
  database version expected by the code.

See existing ``swh`` packages like ``swh.storage`` or ``swh.scheduler`` for
usage examples.

Writing tests
-------------

The ``swh.core.db.pytest_plugin`` provides a few helper tools to write unit
tests for postgresql based datastores.

By default, when using these fixtures, a postgresql server will be started (by
the pytest_postgresql fixture) and a template database will be created using
the ``postgresql_proc`` fixture factory provided by ``pytest_postgresql``.

Then a dedicated fixture must be declared to use the ``postgresql_proc``
fixture generated by the fixture factory function.

This template database will then be used to create a new database for test
using this dedicated fixture.

In order to help the database initialization process and make it consistent
with the database initialization tools from the ``swh db`` cli, an
``initialize_database_for_module()`` function is provided to be used with the
fixture factory described above.

Typically, writing tests for a ``swh`` package ``swh.example`` would look like:

.. code-block:: python

   from functools import partial

   from pytest_postgresql import factories
   from swh.core.db.pytest_plugin import postgresql_fact
   from swh.core.db.pytest_plugin import initialize_database_for_module

   example_postgresql_proc = factories.postgresql_proc(
     dbname="example",
     load=[partial(initialize_database_for_module,
                   modname="example", version=1)]
     )

   postgresql_example = postgresql_fact("example_postgresql_proc")

   def test_example(postgresql_example):
       with postgresql_example.cursor() as c:
           c.execute("select version from dbversion limit 1")
           assert c.fecthone()[0] == 1


Note: most of the time, you will want to put the scaffolding part of the code
above in a ``conftest.py`` file.


The ``load`` argument of the ``factories.postgresql_proc`` will be used to
initialize the template database that will be used to create a new database for
each test, while the ``load`` argument of the ``postgresql_fact`` fixture will
be executed before each test (in the database created from the template
database and dedicated to the test being executed).
