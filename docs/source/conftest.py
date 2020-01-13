from doctest import ELLIPSIS
from sybil import Sybil
from sybil.parsers.codeblock import CodeBlockParser
from sybil.parsers.doctest import DocTestParser, FIX_BYTE_UNICODE_REPR
from sybil.parsers.skip import skip
import pytest


@pytest.fixture(scope="session")
def assert_rows_equal():
    def _do_test(r1, r2):
        assert r1.asDict(recursive=True) == r2.asDict(recursive=True)
    return _do_test

pytest_collect_file = Sybil(
    parsers=[
        CodeBlockParser(future_imports=['print_function']),
    ],
    pattern='*.rst',
    fixtures=['assert_rows_equal', 'spark'],
).pytest()
