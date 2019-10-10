package io.projectglow.transformers.pipe

import java.io.{OutputStream, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, SGUnivocityGenerator}
import org.apache.spark.sql.types.StructType

class CSVInputFormatter(schema: StructType, parsedOptions: CSVOptions) extends InputFormatter {

  private var writer: PrintWriter = _
  private var univocityGenerator: SGUnivocityGenerator = _

  override def init(stream: OutputStream): Unit = {
    writer = new PrintWriter(stream)
    univocityGenerator = new SGUnivocityGenerator(schema, writer, parsedOptions)
    if (parsedOptions.headerFlag) {
      univocityGenerator.writeHeaders()
    }
  }

  override def write(record: InternalRow): Unit = {
    univocityGenerator.write(record)
  }

  override def close(): Unit = {
    writer.close()
    univocityGenerator.close()
  }
}

class CSVInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "csv"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter = {
    val sqlConf = df.sparkSession.sessionState.conf
    val parsedOptions =
      new CSVOptions(options, sqlConf.csvColumnPruning, sqlConf.sessionLocalTimeZone)
    new CSVInputFormatter(df.schema, parsedOptions)
  }
}
