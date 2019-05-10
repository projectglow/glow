import Dependencies._
import Tests._

val sparkVersion = "2.4.1"
val testConcurrency = 2

ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.databricks"
ThisBuild / organizationName := "DB / RGC"
Compile / compileOrder := CompileOrder.JavaThenScala
Test / fork := true
concurrentRestrictions in Global := Seq(
  Tags.limit(Tags.ForkedTestGroup, testConcurrency)
)

def groupByHash(tests: Seq[TestDefinition]) = {
  tests.groupBy(_.name.hashCode % testConcurrency).map { case (i, tests) =>
    val options = ForkOptions()
    new Group(i.toString, tests, SubProcess(options))
  }.toSeq
}

lazy val core = (project in file("core"))
  .settings(
    name := "spark-genomics",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "com.github.samtools" % "htsjdk" % "2.16.1",
      "org.seqdoop" % "hadoop-bam" % "7.9.1",
      "org.bdgenomics.adam" %% "adam-core-spark2" % "0.26.0",
      "org.bdgenomics.adam" %% "adam-apis-spark2" % "0.26.0",
      "log4j" % "log4j" % "1.2.17",
      "org.slf4j" % "slf4j-api" % "1.7.16",
      "org.slf4j" % "slf4j-log4j12" % "1.7.16",
      "org.jdbi" % "jdbi" % "2.63.1",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      // Exclude xgboost-predictor since it's not in maven central
      "org.broadinstitute" % "gatk" % "4.0.11.0" exclude("biz.k11i", "xgboost-predictor"),

      // Test dependencies
      "org.scalatest" %% "scalatest" % "3.0.3" % "test",
      "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests"
    ),
    testGrouping in Test := groupByHash((definedTests in Test).value)
  )

// Uncomment the following for publishing to Sonatype.
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for more detail.

// ThisBuild / description := "Some descripiton about your project."
// ThisBuild / licenses    := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
// ThisBuild / homepage    := Some(url("https://github.com/example/project"))
// ThisBuild / scmInfo := Some(
//   ScmInfo(
//     url("https://github.com/your-account/your-project"),
//     "scm:git@github.com:your-account/your-project.git"
//   )
// )
// ThisBuild / developers := List(
//   Developer(
//     id    = "Your identifier",
//     name  = "Your Name",
//     email = "your@email",
//     url   = url("http://your.url")
//   )
// )
// ThisBuild / pomIncludeRepository := { _ => false }
// ThisBuild / publishTo := {
//   val nexus = "https://oss.sonatype.org/"
//   if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
//   else Some("releases" at nexus + "service/local/staging/deploy/maven2")
// }
// ThisBuild / publishMavenStyle := true
