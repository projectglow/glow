import pytest
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
import glow as glow


def test_no_register(spark):
    sess = spark.newSession()
    row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    df = sess.createDataFrame([row_one, row_two], schema=['base_col'])
    with pytest.raises(AnalysisException):
        df.selectExpr("add_struct_fields(base_col, 'float_col', 3.14, 'rev_str_col', reverse(base_col.str_col))").head()


def test_register(spark):
    sess = spark.newSession()
    glow.register(sess)
    row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    df = sess.createDataFrame([row_one, row_two], schema=['base_col'])
    added_col_row = df.selectExpr("add_struct_fields(base_col, 'float_col', 3.14, 'rev_str_col', reverse(base_col.str_col)) as added_col") \
                      .filter("added_col.str_col = 'foo'") \
                      .head()
    assert added_col_row.added_col.rev_str_col == 'oof'
