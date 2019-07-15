swh-core
========

core library for swh's modules:
- config parser
- hash computations
- serialization
- logging mechanism
- database connection
- http-based RPC client/server

Development
-----------

We strongly recommend you to use a [virtualenv][1] if you want to run tests or
hack the code.

To set up your development environment:

```
(swh) user@host:~/swh-environment/swh-core$ pip install -e .[testing]
```

This will install every Python package needed to run this package's tests.

Unit tests can be executed using [pytest][2] or [tox][3].

```
(swh) user@host:~/swh-environment/swh-core$ pytest
============================== test session starts ==============================
platform linux -- Python 3.7.3, pytest-3.10.1, py-1.8.0, pluggy-0.12.0
hypothesis profile 'default' -> database=DirectoryBasedExampleDatabase('/home/ddouard/src/swh-environment/swh-core/.hypothesis/examples')
rootdir: /home/ddouard/src/swh-environment/swh-core, inifile: pytest.ini
plugins: requests-mock-1.6.0, hypothesis-4.26.4, celery-4.3.0, postgresql-1.4.1
collected 89 items

swh/core/api/tests/test_api.py ..                                         [  2%]
swh/core/api/tests/test_async.py ....                                     [  6%]
swh/core/api/tests/test_serializers.py .....                              [ 12%]
swh/core/db/tests/test_db.py ....                                         [ 16%]
swh/core/tests/test_cli.py ......                                         [ 23%]
swh/core/tests/test_config.py ..............                              [ 39%]
swh/core/tests/test_statsd.py ........................................... [ 87%]
....                                                                      [ 92%]
swh/core/tests/test_utils.py .......                                      [100%]
===================== 89 passed, 9 warnings in 6.94 seconds =====================
```

Note: this git repository uses [pre-commit][4] hooks to ensure better and more
consistent code. It should already be installed in your virtualenv (if not,
just type `pip install pre-commit`). Make sure to activate it in your local
copy of the git repository:

```
(swh) user@host:~/swh-environment/swh-core$ pre-commit install
pre-commit installed at .git/hooks/pre-commit
```

Please read the [developer setup manual][5] for more information on how to hack
on Software Heritage.

[1]: https://virtualenv.pypa.io
[2]: https://docs.pytest.org
[3]: https://tox.readthedocs.io
[4]: https://pre-commit.com
[5]: https://docs.softwareheritage.org/devel/developer-setup.html
