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
        subprocess.check_call(['psql', '--quiet', '-f', dumpfile, dbname])


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
    """rollback current transaction and disconnet from the test DB

    context: tearDown

    """
    if not conn.closed:
        conn.rollback()
        conn.close()


class DbTestFixture():
    """Mix this in a test subject class to get DB testing support.

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

    To ensure test isolation, each test method of the test case class will
    execute in its own connection, cursor, and transaction.

    To ensure setup/teardown methods are called, in case of multiple
    inheritance DbTestFixture should be the first class in the inheritance
    hierarchy.

    Note that if you want to define setup/teardown methods, you need to
    explicitly call super() to ensure that the fixture setup/teardown methods
    are invoked. Here is an example where all setup/teardown methods are
    defined in a test case:

        class TestDb(DbTestFixture, unittest.TestCase):

            @classmethod
            def setUpClass(cls):
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

    TEST_DB_NAME = 'softwareheritage-test'
    TEST_DB_DUMP = None
    TEST_DB_DUMP_TYPE = 'pg_dump'

    @classmethod
    def setUpClass(cls):
        cls.dbname = db_create(dbname=cls.TEST_DB_NAME,
                               dump=cls.TEST_DB_DUMP,
                               dumptype=cls.TEST_DB_DUMP_TYPE)
        super().setUpClass()

    def setUp(self):
        db_setup = db_connect(self.dbname)
        self.conn = db_setup['conn']
        self.cursor = db_setup['cursor']
        super().setUp()

    def tearDown(self):
        super().tearDown()
        db_close(self.conn)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        db_destroy(cls.dbname)


class DbsTestFixture():
    """Mix this in a test subject class to get DB testing support with
    multiple databases.

    The class can override the following class attributes:
        TEST_DB_NAMES: names of the DB used for testing
        TEST_DB_DUMPS: DB dumps to be restored before running test methods; can
            be set to [] if no restore from dump is required
        TEST_DB_DUMP_TYPES: List of one of 'pg_dump' (binary dump) or 'psql'
            (SQL dump)

    The test case class will then have the following attributes, accessible via
    self:

        dbnames: name of the test database
        conns: psycopg2 connection object
        cursors: open psycopg2 cursor to the DB

    To ensure test isolation, each test method of the test case class will
    execute in its own connection, cursor, and transaction.

    To ensure setup/teardown methods are called, in case of multiple
    inheritance DbTestFixture should be the first class in the inheritance
    hierarchy.

    Note that if you want to define setup/teardown methods, you need to
    explicitly call super() to ensure that the fixture setup/teardown methods
    are invoked. Here is an example where all setup/teardown methods are
    defined in a test case:

        class TestDb(DbTestFixture, unittest.TestCase):

            @classmethod
            def setUpClass(cls):
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

    TEST_DB_NAMES = ['softwareheritage-test']
    TEST_DB_DUMPS = []
    TEST_DB_DUMP_TYPES = ['pg_dump']

    @classmethod
    def setUpClass(cls):
        dbnames = []
        for i, dbname in enumerate(cls.TEST_DB_NAMES):
            try:
                dbname = db_create(dbname,
                                   dump=cls.TEST_DB_DUMPS[i],
                                   dumptype=cls.TEST_DB_DUMP_TYPES[i])
            finally:
                print(dbname)
                dbnames.append(dbname)
                print(dbnames)

        cls.dbnames = dbnames
        super().setUpClass()

    def setUp(self):
        conns = []
        cursors = []
        for i, dbname in enumerate(self.dbnames):
            db_setup = db_connect(dbname)
            conns.append(db_setup['conn'])
            cursors.append(db_setup['cursor'])

        self.conns = conns
        self.cursors = cursors
        super().setUp()

    def tearDown(self):
        super().tearDown()
        for conn in self.conns:
            db_close(conn)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        for dbname in cls.dbnames:
            db_destroy(dbname)
