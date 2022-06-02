# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass
import datetime
from enum import IntEnum
import inspect
from string import printable
from typing import Any
from unittest.mock import MagicMock, Mock
import uuid

from hypothesis import given, settings, strategies
from hypothesis.extra.pytz import timezones
import psycopg2
import pytest
from pytest_postgresql import factories
from typing_extensions import Protocol

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction, db_transaction_generator
from swh.core.db.tests.conftest import function_scoped_fixture_check


# workaround mypy bug https://github.com/python/mypy/issues/5485
class Converter(Protocol):
    def __call__(self, x: Any) -> Any:
        ...


@dataclass
class Field:
    name: str
    """Column name"""
    pg_type: str
    """Type of the PostgreSQL column"""
    example: Any
    """Example value for the static tests"""
    strategy: strategies.SearchStrategy
    """Hypothesis strategy to generate these values"""
    in_wrapper: Converter = lambda x: x
    """Wrapper to convert this data type for the static tests"""
    out_converter: Converter = lambda x: x
    """Converter from the raw PostgreSQL column value to this data type"""


# Limit PostgreSQL integer values
pg_int = strategies.integers(-2147483648, +2147483647)

pg_text = strategies.text(
    alphabet=strategies.characters(
        blacklist_categories=["Cs"],  # surrogates
        blacklist_characters=[
            "\x00",  # pgsql does not support the null codepoint
            "\r",  # pgsql normalizes those
        ],
    ),
)

pg_bytea = strategies.binary()


def pg_bytea_a(min_size: int, max_size: int) -> strategies.SearchStrategy:
    """Generate a PostgreSQL bytea[]"""
    return strategies.lists(pg_bytea, min_size=min_size, max_size=max_size)


def pg_bytea_a_a(min_size: int, max_size: int) -> strategies.SearchStrategy:
    """Generate a PostgreSQL bytea[][]. The inner lists must all have the same size."""
    return strategies.integers(min_value=max(1, min_size), max_value=max_size).flatmap(
        lambda n: strategies.lists(
            pg_bytea_a(min_size=n, max_size=n), min_size=min_size, max_size=max_size
        )
    )


def pg_tstz() -> strategies.SearchStrategy:
    """Generate values that fit in a PostgreSQL timestamptz.

    Notes:
      We're forbidding old datetimes, because until 1956, many timezones had
      seconds in their "UTC offsets" (see
      <https://en.wikipedia.org/wiki/Time_zone Worldwide_time_zones>), which is
      not representable by PostgreSQL.

    """
    min_value = datetime.datetime(1960, 1, 1, 0, 0, 0)
    return strategies.datetimes(min_value=min_value, timezones=timezones())


def pg_jsonb(min_size: int, max_size: int) -> strategies.SearchStrategy:
    """Generate values representable as a PostgreSQL jsonb object (dict)."""
    return strategies.dictionaries(
        strategies.text(printable),
        strategies.recursive(
            # should use floats() instead of integers(), but PostgreSQL
            # coerces large integers into floats, making the tests fail. We
            # only store ints in our generated data anyway.
            strategies.none()
            | strategies.booleans()
            | strategies.integers(-2147483648, +2147483647)
            | strategies.text(printable),
            lambda children: strategies.lists(children, max_size=max_size)
            | strategies.dictionaries(
                strategies.text(printable), children, max_size=max_size
            ),
        ),
        min_size=min_size,
        max_size=max_size,
    )


def tuple_2d_to_list_2d(v):
    """Convert a 2D tuple to a 2D list"""
    return [list(inner) for inner in v]


def list_2d_to_tuple_2d(v):
    """Convert a 2D list to a 2D tuple"""
    return tuple(tuple(inner) for inner in v)


class TestIntEnum(IntEnum):
    foo = 1
    bar = 2


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


FIELDS = (
    Field("i", "int", 1, pg_int),
    Field("txt", "text", "foo", pg_text),
    Field("bytes", "bytea", b"bar", strategies.binary()),
    Field(
        "bytes_array",
        "bytea[]",
        [b"baz1", b"baz2"],
        pg_bytea_a(min_size=0, max_size=5),
    ),
    Field(
        "bytes_tuple",
        "bytea[]",
        (b"baz1", b"baz2"),
        pg_bytea_a(min_size=0, max_size=5).map(tuple),
        in_wrapper=list,
        out_converter=tuple,
    ),
    Field(
        "bytes_2d",
        "bytea[][]",
        [[b"quux1"], [b"quux2"]],
        pg_bytea_a_a(min_size=0, max_size=5),
    ),
    Field(
        "bytes_2d_tuple",
        "bytea[][]",
        ((b"quux1",), (b"quux2",)),
        pg_bytea_a_a(min_size=0, max_size=5).map(list_2d_to_tuple_2d),
        in_wrapper=tuple_2d_to_list_2d,
        out_converter=list_2d_to_tuple_2d,
    ),
    Field(
        "ts",
        "timestamptz",
        now(),
        pg_tstz(),
    ),
    Field(
        "dict",
        "jsonb",
        {"str": "bar", "int": 1, "list": ["a", "b"], "nested": {"a": "b"}},
        pg_jsonb(min_size=0, max_size=5),
        in_wrapper=psycopg2.extras.Json,
    ),
    Field(
        "intenum",
        "int",
        TestIntEnum.foo,
        strategies.sampled_from(TestIntEnum),
        in_wrapper=int,
        out_converter=lambda x: TestIntEnum(x),  # lambda needed by mypy
    ),
    Field("uuid", "uuid", uuid.uuid4(), strategies.uuids()),
    Field(
        "text_list",
        "text[]",
        # All the funky corner cases
        ["null", "NULL", None, "\\", "\t", "\n", "\r", " ", "'", ",", '"', "{", "}"],
        strategies.lists(pg_text, min_size=0, max_size=5),
    ),
    Field(
        "tstz_list",
        "timestamptz[]",
        [now(), now() + datetime.timedelta(days=1)],
        strategies.lists(pg_tstz(), min_size=0, max_size=5),
    ),
    Field(
        "tstz_range",
        "tstzrange",
        psycopg2.extras.DateTimeTZRange(
            lower=now(),
            upper=now() + datetime.timedelta(days=1),
            bounds="[)",
        ),
        strategies.tuples(
            # generate two sorted timestamptzs for use as bounds
            strategies.tuples(pg_tstz(), pg_tstz()).map(sorted),
            # and a set of bounds
            strategies.sampled_from(["[]", "()", "[)", "(]"]),
        ).map(
            # and build the actual DateTimeTZRange object from these args
            lambda args: psycopg2.extras.DateTimeTZRange(
                lower=args[0][0],
                upper=args[0][1],
                bounds=args[1],
            )
        ),
    ),
)

INIT_SQL = "create table test_table (%s)" % ", ".join(
    f"{field.name} {field.pg_type}" for field in FIELDS
)

COLUMNS = tuple(field.name for field in FIELDS)
INSERT_SQL = "insert into test_table (%s) values (%s)" % (
    ", ".join(COLUMNS),
    ", ".join("%s" for i in range(len(COLUMNS))),
)

STATIC_ROW_IN = tuple(field.in_wrapper(field.example) for field in FIELDS)
EXPECTED_ROW_OUT = tuple(field.example for field in FIELDS)

db_rows = strategies.lists(strategies.tuples(*(field.strategy for field in FIELDS)))


def convert_lines(cur):
    return [
        tuple(field.out_converter(x) for x, field in zip(line, FIELDS)) for line in cur
    ]


test_db = factories.postgresql("postgresql_proc", dbname="test-db2")


@pytest.fixture
def db_with_data(test_db, request):
    """Fixture to initialize a db with some data out of the "INIT_SQL above"""
    db = BaseDb.connect(test_db.dsn)
    with db.cursor() as cur:
        psycopg2.extras.register_default_jsonb(cur)
        cur.execute(INIT_SQL)
    yield db
    db.conn.rollback()
    db.conn.close()


@pytest.mark.db
def test_db_connect(db_with_data):
    with db_with_data.cursor() as cur:
        psycopg2.extras.register_default_jsonb(cur)
        cur.execute(INSERT_SQL, STATIC_ROW_IN)
        cur.execute("select * from test_table;")
        output = convert_lines(cur)
        assert len(output) == 1
        assert EXPECTED_ROW_OUT == output[0]


def test_db_initialized(db_with_data):
    with db_with_data.cursor() as cur:
        psycopg2.extras.register_default_jsonb(cur)
        cur.execute(INSERT_SQL, STATIC_ROW_IN)
        cur.execute("select * from test_table;")
        output = convert_lines(cur)
        assert len(output) == 1
        assert EXPECTED_ROW_OUT == output[0]


def test_db_copy_to_static(db_with_data):
    items = [{field.name: field.example for field in FIELDS}]
    db_with_data.copy_to(items, "test_table", COLUMNS)
    with db_with_data.cursor() as cur:
        cur.execute("select * from test_table;")
        output = convert_lines(cur)
        assert len(output) == 1
        assert EXPECTED_ROW_OUT == output[0]


@settings(suppress_health_check=function_scoped_fixture_check, max_examples=5)
@given(db_rows)
def test_db_copy_to(db_with_data, data):
    items = [dict(zip(COLUMNS, item)) for item in data]
    with db_with_data.cursor() as cur:
        cur.execute("TRUNCATE TABLE test_table CASCADE")

    db_with_data.copy_to(items, "test_table", COLUMNS)

    with db_with_data.cursor() as cur:
        cur.execute("select * from test_table;")
        converted_lines = convert_lines(cur)
        assert converted_lines == data


def test_db_copy_to_thread_exception(db_with_data):
    data = [(2**65, "foo", b"bar")]

    items = [dict(zip(COLUMNS, item)) for item in data]
    with pytest.raises(psycopg2.errors.NumericValueOutOfRange):
        db_with_data.copy_to(items, "test_table", COLUMNS)


def test_db_transaction(mocker):
    expected_cur = object()

    called = False

    class Storage:
        @db_transaction()
        def endpoint(self, cur=None, db=None):
            nonlocal called
            called = True
            assert cur is expected_cur

    storage = Storage()

    # 'with storage.get_db().transaction() as cur:' should cause
    # 'cur' to be 'expected_cur'
    db_mock = Mock()
    db_mock.transaction.return_value = MagicMock()
    db_mock.transaction.return_value.__enter__.return_value = expected_cur
    mocker.patch.object(storage, "get_db", return_value=db_mock, create=True)

    put_db_mock = mocker.patch.object(storage, "put_db", create=True)

    storage.endpoint()

    assert called
    put_db_mock.assert_called_once_with(db_mock)


def test_db_transaction__with_generator():
    with pytest.raises(ValueError, match="generator"):

        class Storage:
            @db_transaction()
            def endpoint(self, cur=None, db=None):
                yield None


def test_db_transaction_signature():
    """Checks db_transaction removes the 'cur' and 'db' arguments."""

    def f(self, foo, *, bar=None):
        pass

    expected_sig = inspect.signature(f)

    @db_transaction()
    def g(self, foo, *, bar=None, db=None, cur=None):
        pass

    actual_sig = inspect.signature(g)

    assert actual_sig == expected_sig


def test_db_transaction_generator(mocker):
    expected_cur = object()

    called = False

    class Storage:
        @db_transaction_generator()
        def endpoint(self, cur=None, db=None):
            nonlocal called
            called = True
            assert cur is expected_cur
            yield None

    storage = Storage()

    # 'with storage.get_db().transaction() as cur:' should cause
    # 'cur' to be 'expected_cur'
    db_mock = Mock()
    db_mock.transaction.return_value = MagicMock()
    db_mock.transaction.return_value.__enter__.return_value = expected_cur
    mocker.patch.object(storage, "get_db", return_value=db_mock, create=True)

    put_db_mock = mocker.patch.object(storage, "put_db", create=True)

    list(storage.endpoint())

    assert called
    put_db_mock.assert_called_once_with(db_mock)


def test_db_transaction_generator__with_nongenerator():
    with pytest.raises(ValueError, match="generator"):

        class Storage:
            @db_transaction_generator()
            def endpoint(self, cur=None, db=None):
                pass


def test_db_transaction_generator_signature():
    """Checks db_transaction removes the 'cur' and 'db' arguments."""

    def f(self, foo, *, bar=None):
        pass

    expected_sig = inspect.signature(f)

    @db_transaction_generator()
    def g(self, foo, *, bar=None, db=None, cur=None):
        yield None

    actual_sig = inspect.signature(g)

    assert actual_sig == expected_sig


@pytest.mark.parametrize(
    "query_options", (None, {"something": 42, "statement_timeout": 200})
)
@pytest.mark.parametrize("use_generator", (True, False))
def test_db_transaction_query_options(mocker, use_generator, query_options):
    class Storage:
        @db_transaction(statement_timeout=100)
        def endpoint(self, cur=None, db=None):
            return [None]

        @db_transaction_generator(statement_timeout=100)
        def gen_endpoint(self, cur=None, db=None):
            yield None

    storage = Storage()

    # mockers
    mocked_apply = mocker.patch("swh.core.db.common.apply_options")
    # 'with storage.get_db().transaction() as cur:' should cause
    # 'cur' to be 'expected_cur'
    expected_cur = object()
    db_mock = MagicMock()
    db_mock.transaction.return_value.__enter__.return_value = expected_cur
    mocker.patch.object(storage, "get_db", return_value=db_mock, create=True)
    mocker.patch.object(storage, "put_db", create=True)

    if query_options:
        storage.query_options = {
            "endpoint": query_options,
            "gen_endpoint": query_options,
        }
    if use_generator:
        list(storage.gen_endpoint())
    else:
        list(storage.endpoint())

    mocked_apply.assert_called_once_with(
        expected_cur,
        query_options if query_options is not None else {"statement_timeout": 100},
    )
