/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql.expressions

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, Generator, GenericInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.sql.util.CodegenUtils

/**
 * Bins the given half-open interval [start, end) using the specified binSize
 * and outputs the bins that overlap with this interval
 *
 * @param startCol the start column expression
 * @param endCol the end column expression
 * @param binSize
 */
case class BinIdGenerator(startCol: Expression, endCol: Expression, binSize: Int)
    extends Generator
    with CodegenFallback {

  override def children: Seq[Expression] = Seq(startCol, endCol)

  override def elementSchema: StructType = {
    new StructType()
      .add("binId", IntegerType, nullable = false)
      .add("isLastBin", BooleanType, nullable = false)
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val startValue = startCol.eval(input)
    val endValue = endCol.eval(input)
    if (startValue == null || endValue == null) {
      Nil
    } else {
      val start = startValue.asInstanceOf[Number].longValue()
      val end = endValue.asInstanceOf[Number].longValue()
      val sbin = Math.toIntExact(start / binSize)
      val ebin = Math.toIntExact((end - 1) / binSize)

      new Iterator[InternalRow]() {

        private var current = sbin
        private val row = new GenericInternalRow(2)

        override def hasNext: Boolean = {
          current <= ebin
        }

        override def next(): InternalRow = {
          row.setInt(0, current)
          row.setBoolean(1, current == ebin)
          current += 1
          row
        }
      }
    }
  }
}

case class StringTransformExpression(override val child: Expression, transformer: Char => Char)
    extends UnaryExpression
    with CodegenFallback {

  override def dataType: DataType = StringType

  override protected def nullSafeEval(input: Any): Any = {
    val s = input.asInstanceOf[UTF8String].toString
    val length = s.length
    val sb = new StringBuilder
    var i = 0
    while (i < length) {
      val score = s.charAt(i)
      val transformed = transformer(score)
      sb.append(transformed)
      i += 1
    }
    UTF8String.fromString(sb.toString())
  }
}

case class AsciiCharSplit(str: Expression, split: Expression)
    extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = ArrayType(StringType)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def left: Expression = str
  override def right: Expression = split

  override protected def nullSafeEval(str: Any, split: Any): Any = {
    CodegenUtils.asciiCharSplit(str.asInstanceOf[UTF8String], split.asInstanceOf[UTF8String])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (str, split) => {
      s"""
         |${ev.value} = com.databricks.hls.sql.util.CodegenUtils.asciiCharSplit($str, $split);
       """.stripMargin
    })
  }

  private def byteSub(bytes: Array[Byte], start: Int, end: Int): UTF8String = {
    val len = end - start
    UTF8String.fromBytes(bytes, start, len)
  }
}
