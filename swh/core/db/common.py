# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import inspect


def remove_kwargs(names):
    def decorator(f):
        sig = inspect.signature(f)
        params = sig.parameters
        params = [param for param in params.values() if param.name not in names]
        sig = sig.replace(parameters=params)
        f.__signature__ = sig
        return f

    return decorator


def apply_options(cursor, options):
    """Applies the given postgresql client options to the given cursor.

    Returns a dictionary with the old values if they changed."""
    old_options = {}
    for option, value in options.items():
        cursor.execute("SHOW %s" % option)
        old_value = cursor.fetchall()[0][0]
        if old_value != value:
            # We could also pre-format the option and value using:
            #
            #      (str(option), str(value))
            #
            # However using %s::text is going through the psycopg adapter
            # system and is likely more robust.
            cursor.execute(
                "SELECT set_config(%s::text, %s::text, true)",
                (option, value),
            )
            old_options[option] = old_value
    return old_options


def db_transaction(**client_options):
    """decorator to execute Backend methods within DB transactions

    The decorated method must accept a ``cur`` and ``db`` keyword argument

    Client options are passed as ``set`` options to the postgresql server. If
    available, decorated ``self.query_options`` can be defined as a dict which
    keys are (decorated) method names and values are dicts. These later dicts
    are merged with the given ``client_options``. So it's possible to define
    default client_options as decorator arguments and overload them from e.g. a
    configuration file (e.g. making is the ``self.query_options`` attribute filled
    from a config file).
    """

    def decorator(meth, __client_options=client_options):
        if inspect.isgeneratorfunction(meth):
            raise ValueError("Use db_transaction_generator for generator functions.")

        @remove_kwargs(["cur", "db"])
        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            options = getattr(self, "query_options", None) or {}
            if meth.__name__ in options:
                client_options = {**__client_options, **options[meth.__name__]}
            else:
                client_options = __client_options
            if "cur" in kwargs and kwargs["cur"]:
                cur = kwargs["cur"]
                old_options = apply_options(cur, client_options)
                ret = meth(self, *args, **kwargs)
                apply_options(cur, old_options)
                return ret
            else:
                db = self.get_db()
                try:
                    with db.transaction() as cur:
                        apply_options(cur, client_options)
                        return meth(self, *args, db=db, cur=cur, **kwargs)
                finally:
                    self.put_db(db)

        return _meth

    return decorator


def db_transaction_generator(**client_options):
    """decorator to execute Backend methods within DB transactions, while
    returning a generator

    The decorated method must accept a ``cur`` and ``db`` keyword argument

    Client options are passed as ``set`` options to the postgresql server. If
    available, decorated ``self.query_options`` can be defined as a dict which
    keys are (decorated) method names and values are dicts. These later dicts
    are merged with the given ``client_options``. So it's possible to define
    default client_options as decorator arguments and overload them from e.g. a
    configuration file (e.g. making is the ``self.query_options`` attribute filled
    from a config file).
    """

    def decorator(meth, __client_options=client_options):
        if not inspect.isgeneratorfunction(meth):
            raise ValueError("Use db_transaction for non-generator functions.")

        @remove_kwargs(["cur", "db"])
        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            options = getattr(self, "query_options", None) or {}
            if meth.__name__ in options:
                client_options = {**__client_options, **options[meth.__name__]}
            else:
                client_options = __client_options
            if "cur" in kwargs and kwargs["cur"]:
                cur = kwargs["cur"]
                old_options = apply_options(cur, client_options)
                yield from meth(self, *args, **kwargs)
                apply_options(cur, old_options)
            else:
                db = self.get_db()
                try:
                    with db.transaction() as cur:
                        apply_options(cur, client_options)
                        yield from meth(self, *args, db=db, cur=cur, **kwargs)
                finally:
                    self.put_db(db)

        return _meth

    return decorator
