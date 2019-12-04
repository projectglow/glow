from doctest import ELLIPSIS
from sybil import Sybil
from sybil.parsers.codeblock import CodeBlockParser
from sybil.parsers.doctest import DocTestParser, FIX_BYTE_UNICODE_REPR
from sybil.parsers.skip import skip
import pytest


@pytest.fixture(scope="session")
def rows_equal():
    def _rows_equal(r1, r2):
        return r1.asDict(recursive=True) == r2.asDict(recursive=True)
    return _rows_equal

pytest_collect_file = Sybil(
    parsers=[
        CodeBlockParser(future_imports=['print_function']),
    ],
    pattern='*.rst',
    fixtures=['rows_equal', 'spark'],
).pytest()
