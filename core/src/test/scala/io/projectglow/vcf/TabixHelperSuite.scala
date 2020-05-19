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

package io.projectglow.vcf

import io.projectglow.common.{GlowLogging, SimpleInterval, VCFRow}
import io.projectglow.sql.GlowBaseTest
import io.projectglow.vcf.TabixIndexHelper._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources._

class TabixHelperSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val tabixTestVcf: String = s"$testDataHome/tabix-test-vcf"
  lazy val testVcf = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val testBigVcf = s"$tabixTestVcf/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf.gz"
  lazy val multiAllelicVcf = s"$tabixTestVcf/combined.chr20_18210071_18210093.g.vcf.gz"
  lazy val testNoTbiVcf = s"$tabixTestVcf/NA12878_21_10002403NoTbi.vcf.gz"
  lazy val oneRowGzipVcf = s"$testDataHome/vcf/1row_not_bgz.vcf.gz"

  def printFilterContig(filterContig: FilterContig): Unit = {
    filterContig.getContigName.foreach(i => logger.debug(s"$i"))
  }

  def printFilterInterval(filterInterval: FilterInterval): Unit = {
    filterInterval
      .getSimpleInterval
      .foreach(i => logger.debug(s"${i.getContig}, ${i.getStart}, ${i.getEnd}"))
  }

  def printParsedFilterResult(parsedFilterResult: ParsedFilterResult): Unit = {
    printFilterContig(parsedFilterResult.contig)
    printFilterInterval(parsedFilterResult.startInterval)
    printFilterInterval(parsedFilterResult.endInterval)
  }

  def isSameOptionSimpleInterval(i: Option[SimpleInterval], j: Option[SimpleInterval]): Boolean = {
    (i, j) match {
      case (Some(i), Some(j)) =>
        if (i.getContig == "") {
          i.getContig == j.getContig
        } else {
          i.getContig == j.getContig && i.getStart == j.getStart && i.getEnd == j.getEnd
        }
      case (None, _) => j.isEmpty
      case (_, None) => i.isEmpty
    }
  }

  def isSameParsedFilterResult(i: ParsedFilterResult, j: ParsedFilterResult): Boolean = {
    i.contig.isSame(j.contig) &&
    i.startInterval.isSame(j.startInterval) && i.endInterval.isSame(j.endInterval)
  }

  /**
   * Tests to ensure getSmallestQueryInterval produces the correct interval given different
   * start and end interval situations
   */
  def testGetSmallestQueryInterval(
      ss: Long,
      se: Long,
      es: Long,
      ee: Long,
      xs: Long,
      xe: Long): Unit = {
    val actual = getSmallestQueryInterval(new FilterInterval(ss, se), new FilterInterval(es, ee))
    val expected = new FilterInterval(xs, xe)
    actual
      .getSimpleInterval
      .foreach(i => logger.debug(s"${i.getContig}, ${i.getStart}, ${i.getEnd}"))
    printFilterInterval(actual)
    printFilterInterval(expected)
    assert(actual.isSame(expected))
  }

  test("getSmallestQueryInterval: non-overlapping start and end intervals") {
    testGetSmallestQueryInterval(1000, 2000, 3000, 4000, 3000, 3000)
  }

  test("getSmallestQueryInterval: non-overlapping touching") {
    // note that start will be incremented by one before overlapping
    testGetSmallestQueryInterval(1000, 2000, 2001, 4000, 2001, 2001)
  }

  test("getSmallestQueryInterval: overlapping by one") {
    // Note that start will be incremented by one before overlapping)
    testGetSmallestQueryInterval(1000, 2001, 2001, 4000, 2001, 2001)
  }

  test("getSmallestQueryInterval: overlapping by more than 1") {
    testGetSmallestQueryInterval(1000, 2000, 1500, 4000, 1500, 2000)
  }

  test("getSmallestQueryInterval: start after end") {
    testGetSmallestQueryInterval(2000, 4000, 1000, 1999, 2, 1)
  }

  test("getSmallestQueryInterval: start almost after end") {
    testGetSmallestQueryInterval(2000, 4000, 1000, 2000, 2000, 2000)
  }

  test("getSmallestQueryInterval: no start") {
    testGetSmallestQueryInterval(2, 1, 1000, 2001, 2, 1)
  }

  test("getSmallestQueryInterval: no end") {
    testGetSmallestQueryInterval(2000, 4000, 2, 1, 2, 1)
  }

  /**
   * Tests to ensure parseFilter returns the correct ParsedFilterResult given different
   * filter situations.
   */
  def testParseFilter(
      filters: Seq[Filter],
      contigName: String,
      ss: Long,
      se: Long,
      es: Long,
      ee: Long): Unit = {
    val actual = parseFilter(filters)
    val expected = ParsedFilterResult(
      new FilterContig(contigName),
      new FilterInterval(ss, se),
      new FilterInterval(es, ee))
    printParsedFilterResult(actual)
    printParsedFilterResult(expected)
    assert(isSameParsedFilterResult(actual, expected))
  }

  test("parseFilter: contig, start <, end >") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThan("start", 10004770L),
        GreaterThan("end", 10004775L)
      ),
      "20",
      1,
      10004770,
      10004776,
      Int.MaxValue
    )
  }

  test("parseFilter: contig, start <=, end >=") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThanOrEqual("start", 10004770L),
        GreaterThanOrEqual("end", 10004775L)
      ),
      "20",
      1,
      10004771,
      10004775,
      Int.MaxValue
    )
  }

  test("parseFilter: contig, start >=, end <=") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        GreaterThanOrEqual("start", 10004770L),
        LessThanOrEqual("end", 10004775L)
      ),
      "20",
      10004771,
      Int.MaxValue,
      1,
      10004775
    )
  }

  test("parseFilter: contig, start >, end <") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        GreaterThan("start", 10004770L),
        LessThan("end", 10004775L)
      ),
      "20",
      10004772,
      Int.MaxValue,
      1,
      10004774
    )
  }

  test("parseFilter: contig, start =, end =") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        EqualTo("start", 10004770L),
        EqualTo("end", 10004775L)
      ),
      "20",
      10004771,
      10004771,
      10004775,
      10004775
    )
  }

  test("parseFilter: No contig, No Start, No End") {
    testParseFilter(Seq(), "", 1, Int.MaxValue, 1, Int.MaxValue)
  }

  test("parseFilter: inconsistent contig") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        EqualTo("start", 10004770L),
        EqualTo("end", 10004775L),
        EqualTo("contigName", "12")
      ),
      null,
      10004771,
      10004771,
      10004775,
      10004775
    )
  }

  test("parseFilter: unsupported conditions on contig") {
    // Note: The detection of empty result set is deferred to Spark filtering.
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        GreaterThanOrEqual("contigName", "20"),
        EqualTo("start", 10004770L),
        EqualTo("end", 10004775L),
        EqualTo("contigName", "12")
      ),
      "12",
      10004771,
      10004771,
      10004775,
      10004775
    )
  }

  test("parseFilter: And with equals") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        And(EqualTo("start", 10004770L), EqualTo("end", 10004775L))
      ),
      "20",
      10004771,
      10004771,
      10004775,
      10004775
    )
  }

  test("parseFilter: And with inequalities non-overlapping") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        And(LessThanOrEqual("start", 10004770L), GreaterThanOrEqual("end", 10004775L))
      ),
      "20",
      1,
      10004771,
      10004775,
      Int.MaxValue
    )
  }

  test("parseFilter: Or with equals") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        Or(EqualTo("start", 10004770L), EqualTo("end", 10004775L))
      ),
      "20",
      10004771,
      10004775,
      10004771,
      10004775
    )
  }

  test("parseFilter: Or with inequalities non-overlapping") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        Or(LessThanOrEqual("start", 10004770L), GreaterThanOrEqual("end", 10004775L))
      ),
      "20",
      1,
      Int.MaxValue,
      1,
      Int.MaxValue
    )
  }

  test("parseFilter: Or with inequalities non-overlapping touching") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        Or(LessThanOrEqual("start", 10004774L), GreaterThanOrEqual("end", 10004775L))),
      "20",
      1,
      Int.MaxValue,
      1,
      Int.MaxValue
    )
  }

  test("parseFilter: Or with inequalities overlapping") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        Or(LessThanOrEqual("start", 10004775L), GreaterThanOrEqual("end", 10004775L))
      ),
      "20",
      1,
      Int.MaxValue,
      1,
      Int.MaxValue
    )
  }

  test("parseFilter: Or with inequalities overlapping reverse") {
    testParseFilter(
      Seq(
        EqualTo("contigName", "20"),
        Or(GreaterThanOrEqual("start", 10004775L), LessThanOrEqual("end", 10004775L))
      ),
      "20",
      1,
      Int.MaxValue,
      1,
      Int.MaxValue)
  }

  test("parseFilter: And nested in Or") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        EqualTo("contigName", "20"),
        Or(
          And(GreaterThan("start", 10004223L), LessThan("end", 10004500L)),
          And(GreaterThan("start", 10003500L), LessThan("end", 10004000L))
        )
      ),
      "20",
      10003502,
      10004499,
      10003502,
      10004499
    )
  }

  test("parseFilter: And nested in Or nested in Or") {
    testParseFilter(
      Seq(
        IsNotNull("contigName"),
        EqualTo("contigName", "20"),
        Or(
          Or(
            And(GreaterThan("start", 10004223L), LessThan("end", 10004500L)),
            And(GreaterThan("start", 10003500L), LessThan("end", 10004000L))
          ),
          EqualTo("end", 10004725L)
        )
      ),
      "20",
      10003502,
      10004725,
      10003502,
      10004725
    )
  }

  /**
   * Tests to ensure makeFilteredInterval returns the correct Option[SimpleInterval]
   * given different filter situations
   */
  def testMakeFilteredInterval(
      filters: Seq[Filter],
      useFilterParser: Boolean,
      useIndex: Boolean,
      contigName: String,
      s: Int,
      e: Int): Unit = {
    val actual = makeFilteredInterval(filters, useFilterParser, useIndex)
    val expected = Option(SimpleInterval(contigName, s, e))
    actual.foreach(i => logger.debug(s"${i.getContig}, ${i.getStart}, ${i.getEnd}"))
    expected.foreach(i => logger.debug(s"${i.getContig}, ${i.getStart}, ${i.getEnd}"))
    assert(isSameOptionSimpleInterval(actual, expected))
  }

  def testMakeFilteredInterval(
      filters: Seq[Filter],
      useFilterParser: Boolean,
      useIndex: Boolean): Unit = {
    val actual = makeFilteredInterval(filters, useFilterParser, useIndex)
    actual.foreach(i => logger.debug(s"${i.getContig}, ${i.getStart}, ${i.getEnd}"))
    assert(isSameOptionSimpleInterval(actual, None))
  }

  test("makeFilteredInterval: start and end intervals non-overlapping") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThan("start", 10004770L),
        GreaterThan("end", 10004775L)
      ),
      true,
      true,
      "20",
      10004776,
      10004776
    )
  }

  test("makeFilteredInterval: start and end intervals non-overlapping touching ") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThanOrEqual("start", 10004769L),
        GreaterThanOrEqual("end", 10004771L)
      ),
      true,
      true,
      "20",
      10004771,
      10004771
    )
  }

  test("makeFilteredInterval: start and end intervals non-overlapping by 1 ") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThanOrEqual("start", 10004770L),
        GreaterThanOrEqual("end", 10004771L)
      ),
      true,
      true,
      "20",
      10004771,
      10004771
    )
  }

  test("makeFilteredInterval: start and end intervals overlapping more than 1") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        GreaterThanOrEqual("start", 10004770L),
        LessThanOrEqual("end", 10004775L)
      ),
      true,
      true,
      "20",
      10004771,
      10004775
    )
  }

  test("makeFilteredInterval: start after end") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        GreaterThanOrEqual("start", 10004770L),
        LessThanOrEqual("end", 10004770L)
      ),
      true,
      true
    )
  }

  test("makeFilteredInterval: start almost after end") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        GreaterThanOrEqual("start", 10004770L),
        LessThanOrEqual("end", 10004771L)
      ),
      true,
      true,
      "20",
      10004771,
      10004771
    )
  }

  test("makeFilteredInterval: no contig") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        GreaterThanOrEqual("start", 10004770L),
        LessThanOrEqual("end", 10004771L)
      ),
      true,
      true,
      "",
      1,
      1
    )
  }

  test("makeFilteredInterval: inconsistent contig") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        GreaterThanOrEqual("start", 10004770L),
        LessThanOrEqual("end", 10004771L),
        EqualTo("contigName", "21")
      ),
      true,
      true
    )
  }

  test("makeFilteredInterval: no start") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThanOrEqual("end", 10004771L)
      ),
      true,
      true,
      "20",
      1,
      10004771
    )
  }

  test("makeFilteredInterval: no end") {
    testMakeFilteredInterval(
      Seq(
        IsNotNull("contigName"),
        IsNotNull("end"),
        IsNotNull("start"),
        EqualTo("contigName", "20"),
        LessThanOrEqual("start", 10004771L)
      ),
      true,
      true,
      "20",
      1,
      10004772
    )
  }

  // Tests to ensure simultaneously setting useTabixIndex to true and useFilterParser to false results in an exception.
  test("useFilterParser = false while useTabixIndex = true") {
    try {
      val dfWithTabix = spark
        .read
        .format(sourceName)
        .option("useTabixIndex", true)
        .option("useFilterParser", false)
        .load(testBigVcf)
        .filter("contigName= '20' and start < 10004770 and end > 10004775")

      dfWithTabix.rdd.count()
      assert(false)
    } catch {
      case e: IllegalArgumentException => assert(true)
      case _: Throwable => assert(false)
    }
  }

  // Test to ensure no filter does not cause errors
  test("no filter") {

    val sess = spark
    import sess.implicits._

    val dfEmptyFilter = spark
      .read
      .format(sourceName)
      .schema(VCFRow.schema)
      .load(testVcf)
      .filter("contigName >= 20 ")

    dfEmptyFilter.rdd.count()

    val withEmptyFilter = dfEmptyFilter.orderBy("contigName", "start").as[VCFRow].collect().toSeq

    val dfNoFilter = spark
      .read
      .format(sourceName)
      .schema(VCFRow.schema)
      .load(testVcf)

    dfNoFilter.rdd.count()

    val withNoFilter = dfNoFilter.orderBy("contigName", "start").as[VCFRow].collect().toSeq

    if (dfEmptyFilter.count() == dfNoFilter.count()) {
      withEmptyFilter.zip(withNoFilter).foreach {
        case (ef, nf) =>
          assert(ef.contigName == nf.contigName)
          assert(ef.start == nf.start)
      }
    } else {
      fail()
    }

  }

  // Tests to ensure invalid BGZ and absence of tbi does not cause errors
  test("invalid BGZ") {
    val df = spark
      .read
      .format(sourceName)
      .load(testVcf)
      .filter("contigName= '20' and start < 10004770 and end > 10004775")

    df.rdd.count()

  }

  test("No index file found") {
    val df = spark
      .read
      .format(sourceName)
      .load(testNoTbiVcf)
      .filter("contigName= '21' and start = 10002435")
    df.rdd.count()
  }

  test("gzip files") {
    val path = new Path(oneRowGzipVcf)
    val conf = sparkContext.hadoopConfiguration
    val fs = path.getFileSystem(conf)
    val fileLength = fs.getFileStatus(path).getLen
    val partitionedFile = PartitionedFile(InternalRow.empty, oneRowGzipVcf, 0, 2)
    val interval = Some(SimpleInterval("0", 1, 2))
    assert(
      TabixIndexHelper
        .getFileRangeToRead(fs, partitionedFile, conf, false, false, interval)
        .contains((0L, fileLength)))

    val partitionedFileWithoutStart = partitionedFile.copy(start = 1)
    assert(
      TabixIndexHelper
        .getFileRangeToRead(fs, partitionedFileWithoutStart, conf, false, false, interval)
        .isEmpty)
  }

  /**
   * Tests that the variants returned for different filter statements are the same in
   * the three following cases:
   * 1. Filter parser and Tabix index are both used.
   * 2. Filter parser is used but tabix index is not.
   * 3. Neither is used.
   */
  def testParserAndTabix(fileName: String, condition: String): Unit = {

    val sess = spark
    import sess.implicits._

    val dfFT = spark
      .read
      .format(sourceName)
      .schema(VCFRow.schema)
      .load(fileName)
      .filter(condition)
    dfFT.rdd.count()
    val withFT = dfFT.orderBy("contigName", "start").as[VCFRow].collect().toSeq

    val dfFN = spark
      .read
      .format(sourceName)
      .option("useTabixIndex", false)
      .schema(VCFRow.schema)
      .load(fileName)
      .filter(condition)
    dfFN.rdd.count()
    val withFN = dfFN.orderBy("contigName", "start").as[VCFRow].collect().toSeq

    val dfNN = spark
      .read
      .format(sourceName)
      .option("useTabixIndex", false)
      .option("useFilterParser", false)
      .schema(VCFRow.schema)
      .load(fileName)
      .filter(condition)
    dfNN.rdd.count()
    val withNN = dfNN.orderBy("contigName", "start").as[VCFRow].collect().toSeq

    if (dfNN.count() == dfFT.count() && dfNN.count() == dfFN.count()) {
      withFT.zip(withFN).zip(withNN).foreach {
        case ((ft, fn), nn) =>
          logger.debug(s"${ft.contigName}, ${ft.start}")
          logger.debug(s"${fn.contigName}, ${fn.start}")
          logger.debug(s"${nn.contigName}, ${nn.start}")
          assert(ft.contigName == nn.contigName && fn.contigName == nn.contigName)
          assert(ft.start == nn.start && fn.start == nn.start)
      }
    } else {
      fail()
    }
  }

  gridTest("Parser/Tabix vs Not")(
    Seq(
      "contigName= '20' and start > 0",
      "contigName= '20' and start >= 0",
      // Filter parser skips negative parameters and defers to spark
      "contigName= '20' and start > -1",
      "contigName= '20' and start = 10004193 and end > -12",
      // Some corner cases
      "contigName= '20' and start > 10004193",
      "contigName= '20' and start >= 10004193",
      "contigName= '20' and start <= 10004768 and end >= 10004779",
      "contigName= '20' and start <= 10004768 and end > 10004779",
      "contigName= '20' and start < 10004768 and end >= 10004779",
      "contigName= '20' and start > 10001433 and end < 10001445",
      "contigName = '20' and ((start>10004223 and end <10004500) or " +
      "(start > 10003500 and end < 10004000))",
      "contigName= '20' and ((start>10004223 and end <10004500) or " +
      "(start > 10003500 and end < 10004000) or (end= 10004725))",
      "contigName= '20' and (start=10000211 or end=10003817)",
      "contigName= '20' and ((start>10004223 and end <10004500) or " +
      "(start > 10003500 and end < 10004000)) and contigName='20'",
      // FilterParser unsupported logical operators must be handled correctly as well.
      "contigName= '20' and (not(start>10004223 and end <10004500) " +
      "or not(start > 10003500 and end < 10004000))"
    )
  ) { condition =>
    testParserAndTabix(testBigVcf, condition)
  }

  test("Do not try to read index files") {
    val tbi = testBigVcf + ".tbi"
    val path = new Path(tbi)
    val conf = sparkContext.hadoopConfiguration
    val fs = path.getFileSystem(conf)
    val partitionedFile = PartitionedFile(InternalRow.empty, tbi, 0, 2)
    val interval = Some(SimpleInterval("0", 1, 2))
    assert(
      TabixIndexHelper
        .getFileRangeToRead(fs, partitionedFile, conf, false, false, interval)
        .isEmpty)
  }
}
