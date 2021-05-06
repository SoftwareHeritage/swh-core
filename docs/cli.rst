.. _swh-core-cli:

Command-line interface
======================

Shared command-line interface
-----------------------------

.. click:: swh.core.cli:swh
  :prog: swh

Database initialization utilities
---------------------------------

.. click:: swh.core.cli.db:db_init
  :prog: swh db init
  :nested: full

.. click:: swh.core.cli.db:db_init
  :prog: swh db-init
  :nested: full
