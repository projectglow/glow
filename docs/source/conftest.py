from sybil import Sybil
from sybil.parsers.codeblock import CodeBlockParser
from pandas.testing import assert_series_equal
import pytest
import pandas as pd


@pytest.fixture(scope="session")
def assert_rows_equal():
    def _assert_rows_equal(r1, r2):
        d1 = r1.asDict(recursive=True)
        s1 = pd.Series(d1, index = sorted(d1.keys()))
        d2 = r2.asDict(recursive=True)
        s2 = pd.Series(d2, index = sorted(d2.keys()))
        # Permissive to floating-point error
        assert_series_equal(s1, s2)
    return _assert_rows_equal

pytest_collect_file = Sybil(
    parsers=[
        CodeBlockParser(future_imports=['print_function']),
    ],
    pattern='*.rst',
    fixtures=['assert_rows_equal', 'spark'],
).pytest()
