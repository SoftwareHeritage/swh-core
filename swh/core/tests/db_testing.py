# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import subprocess


def pg_restore(dbname, dumpfile, dumptype='pg_dump'):
    """
    Args:
        dbname: name of the DB to restore into
        dumpfile: path fo the dump file
        dumptype: one of 'pg_dump' (for binary dumps), 'psql' (for SQL dumps)
    """
    assert dumptype in ['pg_dump', 'psql']
    if dumptype == 'pg_dump':
        subprocess.check_call(['pg_restore', '--no-owner', '--no-privileges',
                               '--dbname', dbname, dumpfile])
    elif dumptype == 'psql':
        subprocess.check_call(['psql', '--quiet',
                                       '--no-psqlrc',
                                       '-v', 'ON_ERROR_STOP=1',
                                       '-f', dumpfile,
                                       dbname])


def pg_dump(dbname, dumpfile):
    subprocess.check_call(['pg_dump', '--no-owner', '--no-privileges', '-Fc',
                           '-f', dumpfile, dbname])


def pg_dropdb(dbname):
    subprocess.check_call(['dropdb', dbname])


def pg_createdb(dbname):
    subprocess.check_call(['createdb', dbname])


def db_create(dbname, dump=None, dumptype='pg_dump'):
    """create the test DB and load the test data dump into it

    context: setUpClass

    """
    try:
        pg_createdb(dbname)
    except subprocess.CalledProcessError:  # try recovering once, in case
        pg_dropdb(dbname)                  # the db already existed
        pg_createdb(dbname)
    if dump:
        pg_restore(dbname, dump, dumptype)
    return dbname


def db_destroy(dbname):
    """destroy the test DB

    context: tearDownClass

    """
    pg_dropdb(dbname)


def db_connect(dbname):
    """connect to the test DB and open a cursor

    context: setUp

    """
    conn = psycopg2.connect('dbname=' + dbname)
    return {
        'conn': conn,
        'cursor': conn.cursor()
    }


def db_close(conn):
    """rollback current transaction and disconnect from the test DB

    context: tearDown

    """
    if not conn.closed:
        conn.rollback()
        conn.close()


class DbTestConn:
    def __init__(self, dbname):
        self.dbname = dbname

    def __enter__(self):
        self.db_setup = db_connect(self.dbname)
        self.conn = self.db_setup['conn']
        self.cursor = self.db_setup['cursor']
        return self

    def __exit__(self, *_):
        db_close(self.conn)


class DbTestContext:
    def __init__(self, name='softwareheritage-test', dump=None,
                 dump_type='pg_dump'):
        self.dbname = name
        self.dump = dump
        self.dump_type = dump_type

    def __enter__(self):
        db_create(dbname=self.dbname,
                  dump=self.dump,
                  dumptype=self.dump_type)
        return self

    def __exit__(self, *_):
        db_destroy(self.dbname)


class DbTestFixture:
    """Mix this in a test subject class to get DB testing support.

    Use the class method add_db() to add a new database to be tested.
    Using this will create a DbTestConn entry in the `test_db` dictionary for
    all the tests, indexed by the name of the database.

    Example:

    class TestDb(DbTestFixture, unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            super().setUpClass()
            cls.add_db('db_name', DUMP)

        def setUp(self):
            db = self.test_db['db_name']
            print('conn: {}, cursor: {}'.format(db.conn, db.cursor))

    To ensure test isolation, each test method of the test case class will
    execute in its own connection, cursor, and transaction.

    Note that if you want to define setup/teardown methods, you need to
    explicitly call super() to ensure that the fixture setup/teardown methods
    are invoked. Here is an example where all setup/teardown methods are
    defined in a test case:

        class TestDb(DbTestFixture, unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                # your add_db() calls here
                super().setUpClass()
                # your class setup code here

            def setUp(self):
                super().setUp()
                # your instance setup code here

            def tearDown(self):
                # your instance teardown code here
                super().tearDown()

            @classmethod
            def tearDownClass(cls):
                # your class teardown code here
                super().tearDownClass()

    """

    _DB_DUMP_LIST = {}
    _DB_LIST = {}
    DB_TEST_FIXTURE_IMPORTED = True

    @classmethod
    def add_db(cls, name='softwareheritage-test', dump=None,
               dump_type='pg_dump'):
        cls._DB_DUMP_LIST[name] = (dump, dump_type)

    @classmethod
    def setUpClass(cls):
        for name, (dump, dump_type) in cls._DB_DUMP_LIST.items():
            cls._DB_LIST[name] = DbTestContext(name, dump, dump_type)
            cls._DB_LIST[name].__enter__()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        for name, context in cls._DB_LIST.items():
            context.__exit__()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_db = {}

    def setUp(self):
        self.test_db = {}
        for name in self._DB_LIST.keys():
            self.test_db[name] = DbTestConn(name)
            self.test_db[name].__enter__()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        for name in self._DB_LIST.keys():
            self.test_db[name].__exit__()

    def reset_db_tables(self, name, excluded=None):
        db = self.test_db[name]
        conn = db.conn
        cursor = db.cursor

        cursor.execute("""SELECT table_name FROM information_schema.tables
                          WHERE table_schema = %s""", ('public',))

        tables = set(table for (table,) in cursor.fetchall())
        if excluded is not None:
            tables -= set(excluded)

        for table in tables:
            cursor.execute('truncate table %s cascade' % table)

        conn.commit()


class SingleDbTestFixture(DbTestFixture):
    """Simplified fixture like DbTest but that can only handle a single DB.

    Gives access to shortcuts like self.cursor and self.conn.

    DO NOT use this with other fixtures that need to access databases, like
    StorageTestFixture.

    The class can override the following class attributes:
        TEST_DB_NAME: name of the DB used for testing
        TEST_DB_DUMP: DB dump to be restored before running test methods; can
            be set to None if no restore from dump is required
        TEST_DB_DUMP_TYPE: one of 'pg_dump' (binary dump) or 'psql' (SQL dump)

    The test case class will then have the following attributes, accessible via
    self:

        dbname: name of the test database
        conn: psycopg2 connection object
        cursor: open psycopg2 cursor to the DB
    """

    TEST_DB_NAME = 'softwareheritage-test'
    TEST_DB_DUMP = None
    TEST_DB_DUMP_TYPE = 'pg_dump'

    @classmethod
    def setUpClass(cls):
        cls.dbname = cls.TEST_DB_NAME
        cls.add_db(name=cls.TEST_DB_NAME,
                   dump=cls.TEST_DB_DUMP,
                   dump_type=cls.TEST_DB_DUMP_TYPE)
        super().setUpClass()

    def setUp(self):
        super().setUp()

        db = self.test_db[self.TEST_DB_NAME]
        self.conn = db.conn
        self.cursor = db.cursor
