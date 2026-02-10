"""Tests that polars and pandas correctly infer schemas from cursor.description type codes."""

import datetime
import inspect
import os
import pytest

import mssql_python

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


@pytest.fixture(scope="module")
def db_connection():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING not set")
    conn = mssql_python.connect(conn_str)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def cursor(db_connection):
    cur = db_connection.cursor()
    yield cur
    cur.close()


# ── cursor.description type_code verification ─────────────────────────────


class TestCursorDescriptionTypeCodes:
    """Verify cursor.description returns isclass-compatible Python types."""

    def test_date_type_code_is_datetime_date(self, cursor):
        """DATE columns must report datetime.date, not str."""
        cursor.execute("SELECT CAST('2024-01-15' AS DATE) AS d")
        type_code = cursor.description[0][1]
        assert type_code is datetime.date
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_time_type_code_is_datetime_time(self, cursor):
        """TIME columns must report datetime.time."""
        cursor.execute("SELECT CAST('13:45:30' AS TIME) AS t")
        type_code = cursor.description[0][1]
        assert type_code is datetime.time
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_datetime_type_code_is_datetime_datetime(self, cursor):
        """DATETIME columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:30' AS DATETIME) AS dt")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_datetime2_type_code_is_datetime_datetime(self, cursor):
        """DATETIME2 columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:30.1234567' AS DATETIME2) AS dt2")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_smalldatetime_type_code_is_datetime_datetime(self, cursor):
        """SMALLDATETIME columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_datetimeoffset_type_code_is_datetime_datetime(self, cursor):
        """DATETIMEOFFSET columns must report datetime.datetime."""
        cursor.execute("SELECT CAST('2024-01-15 13:45:30.123 +05:30' AS DATETIMEOFFSET) AS dto")
        type_code = cursor.description[0][1]
        assert type_code is datetime.datetime
        assert inspect.isclass(type_code)
        cursor.fetchall()

    def test_all_types_are_isclass(self, cursor):
        """Every type_code in cursor.description must pass inspect.isclass()."""
        cursor.execute("""
            SELECT
                CAST(1 AS INT) AS i,
                CAST('x' AS NVARCHAR(10)) AS s,
                CAST('2024-01-15' AS DATE) AS d,
                CAST('13:45:30' AS TIME) AS t,
                CAST('2024-01-15 13:45:30' AS DATETIME2) AS dt2,
                CAST(1.5 AS DECIMAL(10,2)) AS dec,
                CAST(1 AS BIT) AS b,
                CAST(0x01 AS VARBINARY(10)) AS bin
            """)
        for desc in cursor.description:
            col_name = desc[0]
            type_code = desc[1]
            assert inspect.isclass(
                type_code
            ), f"Column '{col_name}': type_code={type_code!r} fails isclass()"
        cursor.fetchall()


# ── Polars integration ────────────────────────────────────────────────────


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestPolarsIntegration:
    """Polars read_database must infer correct dtypes from cursor.description."""

    def test_polars_date_column(self, db_connection):
        """Issue #352: DATE columns caused ComputeError in polars."""
        df = pl.read_database(
            query="SELECT CAST('2024-01-15' AS DATE) AS d",
            connection=db_connection,
        )
        assert df.schema["d"] == pl.Date
        assert df["d"][0] == datetime.date(2024, 1, 15)

    def test_polars_all_datetime_types(self, db_connection):
        """All date/time types must produce correct polars dtypes."""
        df = pl.read_database(
            query="""
                SELECT
                    CAST('2024-01-15' AS DATE) AS d,
                    CAST('2024-01-15 13:45:30' AS DATETIME) AS dt,
                    CAST('2024-01-15 13:45:30.123' AS DATETIME2) AS dt2,
                    CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt
            """,
            connection=db_connection,
        )
        assert df.schema["d"] == pl.Date
        assert df.schema["dt"] == pl.Datetime
        assert df.schema["dt2"] == pl.Datetime
        assert df.schema["sdt"] == pl.Datetime

    def test_polars_mixed_types(self, db_connection):
        """Mixed column types with DATE must not cause schema mismatch."""
        df = pl.read_database(
            query="""
                SELECT
                    CAST(42 AS INT) AS i,
                    CAST('hello' AS NVARCHAR(50)) AS s,
                    CAST('2024-06-15' AS DATE) AS d,
                    CAST(99.95 AS DECIMAL(10,2)) AS amount
            """,
            connection=db_connection,
        )
        assert df["i"][0] == 42
        assert df["s"][0] == "hello"
        assert df["d"][0] == datetime.date(2024, 6, 15)
        assert df.schema["d"] == pl.Date

    def test_polars_date_with_nulls(self, db_connection):
        """DATE columns with NULLs must still infer Date dtype."""
        cursor = db_connection.cursor()
        cursor.execute("""
            CREATE TABLE #polars_null_test (
                id INT,
                d DATE
            )
            """)
        cursor.execute("""
            INSERT INTO #polars_null_test VALUES
            (1, '2024-01-15'),
            (2, NULL),
            (3, '2024-03-20')
            """)
        db_connection.commit()

        df = pl.read_database(
            query="SELECT * FROM #polars_null_test ORDER BY id",
            connection=db_connection,
        )
        assert df.schema["d"] == pl.Date
        assert df["d"][0] == datetime.date(2024, 1, 15)
        assert df["d"][1] is None
        assert df["d"][2] == datetime.date(2024, 3, 20)

        cursor.execute("DROP TABLE IF EXISTS #polars_null_test")
        db_connection.commit()
        cursor.close()


# ── Pandas integration ────────────────────────────────────────────────────


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@pytest.mark.filterwarnings("ignore::UserWarning")
class TestPandasIntegration:
    """Pandas read_sql must handle date/time columns correctly."""

    def test_pandas_date_column(self, db_connection):
        """DATE columns must be readable by pandas without error."""
        df = pd.read_sql(
            "SELECT CAST('2024-01-15' AS DATE) AS d",
            db_connection,
        )
        assert len(df) == 1
        val = df["d"].iloc[0]
        # pandas may return datetime or date depending on version
        if isinstance(val, datetime.datetime):
            assert val.date() == datetime.date(2024, 1, 15)
        else:
            assert val == datetime.date(2024, 1, 15)

    def test_pandas_all_datetime_types(self, db_connection):
        """All date/time types must be readable by pandas."""
        df = pd.read_sql(
            """
            SELECT
                CAST('2024-01-15' AS DATE) AS d,
                CAST('2024-01-15 13:45:30' AS DATETIME) AS dt,
                CAST('2024-01-15 13:45:30.123' AS DATETIME2) AS dt2,
                CAST('2024-01-15 13:45:00' AS SMALLDATETIME) AS sdt
            """,
            db_connection,
        )
        assert len(df) == 1
        assert len(df.columns) == 4

    def test_pandas_mixed_types_with_date(self, db_connection):
        """Mixed column types including DATE must work correctly."""
        df = pd.read_sql(
            """
            SELECT
                CAST(42 AS INT) AS i,
                CAST('hello' AS NVARCHAR(50)) AS s,
                CAST('2024-06-15' AS DATE) AS d,
                CAST(99.95 AS DECIMAL(10,2)) AS amount
            """,
            db_connection,
        )
        assert df["i"].iloc[0] == 42
        assert df["s"].iloc[0] == "hello"
        val = df["d"].iloc[0]
        if isinstance(val, datetime.datetime):
            assert val.date() == datetime.date(2024, 6, 15)
        else:
            assert val == datetime.date(2024, 6, 15)
