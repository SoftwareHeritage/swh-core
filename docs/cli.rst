.. _swh-core-cli:

Command-line interface
======================

Shared command-line interface
-----------------------------

.. click:: swh.core.cli:swh
  :prog: swh

Shell Completion
----------------

You may activate the command line completion mechanism for your shell. The
`swh` tool is using the `click`_ package, so activating command completion is a
simple matter of:

.. tab-set::

   .. tab-item:: Bash

      Add this to your `.bashrc`

      .. code-block:: bash

         eval "$(_SWH_COMPLETE=bash_source swh)"

   .. tab-item:: Zsh

      Add this to your `.zshrc`

      .. code-block:: zsh

         eval "$(_SWH_COMPLETE=zsh_source swh)"


See `click documentation`_ for more details and options.

.. _`click`: https://click.palletsprojects.com
.. _`click documentation`: https://click.palletsprojects.com/en/8.1.x/shell-completion


Database initialization utilities
---------------------------------

.. click:: swh.core.cli.db:db_init
  :prog: swh db init
  :nested: full

.. click:: swh.core.cli.db:db_init
  :prog: swh db-init
  :nested: full
