package org.projectglow.core.vcf

import java.lang.{Boolean => JBoolean, Iterable => JIterable}
import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{Allele, Genotype => HtsjdkGenotype, VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf.{VCFConstants, VCFHeader}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.projectglow.core.common.HLSLogging

// HTSJDK VariantContext -> VCFRow
// Based on the HTSJDK classes VCFEncoder and CommonInfo
private[databricks] object VariantContextToVCFRowConverter {

  def parseObjectAsString(obj: Object): String = {
    obj match {
      case null => ""
      case VCFConstants.MISSING_VALUE_v4 => ""
      case _: JBoolean => ""
      case intArray: Array[Int] => intArray.mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case doubleArray: Array[Double] =>
        doubleArray
          .mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case objArray: Array[Object] =>
        val length = objArray.length
        val strSeq = new mutable.ArraySeq[String](length)
        var arrayIdx = 0
        while (arrayIdx < length) {
          strSeq.update(arrayIdx, parseObjectAsString(objArray(arrayIdx)))
          arrayIdx += 1
        }
        strSeq.mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case objIter if objIter.isInstanceOf[JIterable[_]] =>
        val iterator = objIter.asInstanceOf[JIterable[Object]].iterator
        val listBuffer = new mutable.ListBuffer[String]()
        while (iterator.hasNext) {
          listBuffer.append(parseObjectAsString(iterator.next))
        }
        listBuffer.mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case _ => obj.toString
    }
  }
}

// HTSJDK VariantContext -> VCFRow
private[databricks] class VariantContextToVCFRowConverter(
    vcfHeader: VCFHeader,
    stringency: ValidationStringency = ValidationStringency.LENIENT,
    includeSampleIds: Boolean = true)
    extends HLSLogging
    with Serializable {

  private val converter = new VariantContextToInternalRowConverter(
    vcfHeader,
    VCFRow.schema,
    stringency,
    writeSampleIds = includeSampleIds
  )

  private val rowToVCFRowConverter = VCFRow.encoder.resolveAndBind()

  def convert(variantContext: HtsjdkVariantContext): VCFRow = {
    val internalRow = converter.convertRow(variantContext, isSplit = false)
    rowToVCFRowConverter.fromRow(internalRow)
  }
}
