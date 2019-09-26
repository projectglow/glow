package com.databricks.sql

import org.apache.spark.sql.sources.DataSourceRegister

// For backward compatibility, prefixes the Datasource alias with "com.databricks.".
// For example, the aliases "com.databricks.vcf" and "vcf" refer to the same Datasource.
@deprecated("Remove the prefix com.databricks.")
trait ComDatabricksDataSource extends DataSourceRegister {
  abstract override def shortName(): String = "com.databricks." + super.shortName()
}
