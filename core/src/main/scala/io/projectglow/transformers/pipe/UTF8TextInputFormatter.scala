package io.projectglow.transformers.pipe

import java.io.{OutputStream, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLUtils.dataTypesEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType

/**
 * A simple input formatter that writes each row as a string.
 */
class UTF8TextInputFormatter() extends InputFormatter {

  private var writer: PrintWriter = _

  override def init(stream: OutputStream): Unit = {
    writer = new PrintWriter(stream)
  }

  override def write(record: InternalRow): Unit = {
    if (!record.isNullAt(0)) {
      writer.println(record.getUTF8String(0)) // scalastyle:ignore
    }
  }

  override def close(): Unit = {
    writer.close()
  }
}

class UTF8TextInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "text"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter = {
    require(df.schema.length == 1, "Input dataframe must have one column,")
    require(
      dataTypesEqualExceptNullability(df.schema.head.dataType, StringType),
      "Input dataframe must have one string column.")
    new UTF8TextInputFormatter
  }
}
