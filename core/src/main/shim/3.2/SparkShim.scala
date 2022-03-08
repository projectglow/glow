/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

import org.apache.spark.sql.types.{DataType}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException}

// Spark 3.2 APIs that are not inter-version compatible
object SparkShim extends SparkShimBase {
  // [SPARK-25393][SQL] Adding new function from_csv()
  // Refactors classes from [[org.apache.spark.sql.execution.datasources.csv]] to [[org.apache.spark.sql.catalyst.csv]]
  override type CSVOptions = org.apache.spark.sql.catalyst.csv.CSVOptions
  override type UnivocityParser = org.apache.spark.sql.catalyst.csv.UnivocityParser

  override def wrapUnivocityParse(parser: UnivocityParser)(input: String): Option[InternalRow] = {
    parser.parse(input)
  }

  // [SPARK-27328][SQL] Add 'deprecated' in ExpressionDescription for extended usage and SQL doc
  // Adds 'deprecated' argument to the ExpressionInfo constructor
  override def createExpressionInfo(
      className: String,
      db: String,
      name: String,
      usage: String,
      arguments: String,
      examples: String,
      note: String,
      since: String): ExpressionInfo = {
    // TODO fix this up later.
    new ExpressionInfo(
      className,
      db,
      name,
      usage,
      arguments
    )
  }

  // [SPARK-28077][SQL] Support ANSI SQL OVERLAY function.
  // Adds QuaternaryExpression
  abstract class QuaternaryExpression
      extends org.apache.spark.sql.catalyst.expressions.QuaternaryExpression

  override def children: Seq[Expression] = arguments ++ functions

  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override def first: Expression = genotypes
  override def second: Expression = phenotypes
  override def third: Expression = covariates

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): WrappedAggregateByIndex =
    copy(
      arr = newChildren(0),
      update = newChildren(1),
      merge = newChildren(2),
      evaluate = newChildren(3))

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): LiftOverCoordinatesExpr =
    copy(
      contigName = newChildren(0),
      start = newChildren(1),
      end = newChildren(2),
      chainFile = newChildren(3),
      minMatchRatio = newChildren(4))

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): LinearRegressionExpr =
    copy(genotypes = newFirst, phenotypes = newSecond, covariates = newThird)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): LogisticRegressionExpr = {
    if (newChildren.size == 5) {
      copy(
        genotypes = newChildren(0),
        phenotypes = newChildren(1),
        covariates = newChildren(2),
        test = newChildren(3),
        offsetOption = Option(newChildren(4))
      )
    } else {
      copy(
        genotypes = newChildren(0),
        phenotypes = newChildren(1),
        covariates = newChildren(2),
        test = newChildren(3)
      )
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): MeanSubstitute =
    copy(array = newChildren.head, missingValue = newChildren.last)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): NormalizeVariantExpr =
    copy(
      contigName = newChildren(0),
      start = newChildren(1),
      end = newChildren(2),
      refAllele = newChildren(3),
      altAlleles = newChildren(4),
      refGenomePathString = newChildren(5)
    )

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): PerSampleSummaryStatistics =
    copy(genotypes = newChildren.head)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): SampleDpSummaryStatistics =
    copy(child = newChildren.head)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): SampleGqSummaryStatistics =
    copy(child = newChildren.head)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CallSummaryStats =
    copy(genotypes = newChildren(0), refAllele = newChildren(1), altAlleles = newChildren(2))

  override def withNewChildInternal(newChild: Expression): HardyWeinberg = {
    copy(genotypes = newChild)
  }

  override protected def withNewChildInternal(newChild: Expression): CallStats =
    copy(genotypes = newChild)

  override protected def withNewChildInternal(newChild: Expression): ArrayStatsSummary =
    copy(array = newChild)

  override protected def withNewChildrenInternal(
      children: IndexedSeq[Expression]): DpSummaryStats = {
    copy(child = children.head)
  }

  override protected def withNewChildrenInternal(
      children: IndexedSeq[Expression]): GqSummaryStats = {
    copy(child = children.head)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): AssertTrueOrError = {
    copy(child = newLeft, errMsg = newRight)
  }

  override protected def withNewChildInternal(newChild: Expression): GenotypeStates = {
    copy(genotypes = newChild)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): HardCalls =
    copy(
      probabilities = newChildren(0),
      numAlts = newChildren(1),
      phased = newChildren(2),
      threshold = newChildren(3))

  override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): ExpandStruct = {
    copy(struct = children.head)
  }

  override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): SubsetStruct = {
    copy(struct = children.head)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): AddStructFields =
    copy(struct = newChildren(0), newFields = newChildren.drop(1))

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ExplodeMatrix =
    copy(matrixExpr = newChildren.head)

  override protected def withNewChildInternal(newChild: Expression): ArrayToSparseVector = {
    copy(child = newChild)
  }

  override protected def withNewChildInternal(newChild: Expression): ArrayToDenseVector = {
    copy(child = newChild)
  }

  override protected def withNewChildInternal(newChild: Expression): VectorToArray = {
    copy(child = newChild)
  }

  override def withNewChildInternal(newChild: Expression): OneArgExpr =
    copy(child = newChild)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): TwoArgExpr =
    copy(left = newLeft, right = newRight)

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): VarArgsExpr =
    copy(arg = newChildren.head, varArgs = newChildren.drop(1))

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): OptionalArgExpr =
    copy(required = newChildren.head, optional = newChildren.last)

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): testQuinaryExpr =
    copy(
      child1 = newChildren.head,
      child2 = newChildren(1),
      child3 = newChildren(2),
      child4 = newChildren(4),
      child5 = Option(newChildren(5)))

  val path = new Path(BigFileDatasource.checkPath(options))
  val outputStream = Option(codec.getCodec(path))

}
