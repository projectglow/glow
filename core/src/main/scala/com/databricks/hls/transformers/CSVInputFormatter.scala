package com.databricks.hls.transformers

import java.io.{OutputStream, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, UnivocityGeneratorWrapper}
import org.apache.spark.sql.types.StructType

class CSVInputFormatter(schema: StructType, parsedOptions: CSVOptions) extends InputFormatter {

  private var writer: PrintWriter = _
  private var univocityGenerator: UnivocityGeneratorWrapper = _

  override def init(stream: OutputStream): Unit = {
    writer = new PrintWriter(stream)
    univocityGenerator = new UnivocityGeneratorWrapper(schema, writer, parsedOptions)
  }

  override def write(record: InternalRow): Unit = {
    univocityGenerator.write(record)
  }

  override def writeDummyDataset(): Unit = {
    // Writes the header at least once (if header flag is set, twice)
    univocityGenerator.write(InternalRow.fromSeq(schema.fieldNames))
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
