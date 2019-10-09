import scala.sys.process._

import sbt.Tests._

val sparkVersion = "2.4.3"
val scalaMajorMinor = "2.11"

ThisBuild / scalaVersion := s"$scalaMajorMinor.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.databricks"
ThisBuild / organizationName := "DB / RGC"
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Compile Java sources before Scala sources, so Scala code can depend on Java
// but not vice versa
Compile / compileOrder := CompileOrder.JavaThenScala

// Test concurrency settings
// Tests are run serially in one or more forked JVMs. This setup is necessary because the shared
// Spark session used by many tasks cannot be used concurrently.
val testConcurrency = 1
Test / fork := true
concurrentRestrictions in Global := Seq(
  Tags.limit(Tags.ForkedTestGroup, testConcurrency)
)

def groupByHash(tests: Seq[TestDefinition]) = {
  tests
    .groupBy(_.name.hashCode % testConcurrency)
    .map {
      case (i, tests) =>
        val options = ForkOptions()
          .withRunJVMOptions(Vector("-Xmx1024m"))
        new Group(i.toString, tests, SubProcess(options))
    }
    .toSeq
}

lazy val mainScalastyle = taskKey[Unit]("mainScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
// testGrouping cannot be set globally using the `Test /` syntax since it's not a pure value
lazy val commonSettings = Seq(
  mainScalastyle := scalastyle.in(Compile).toTask("").value,
  testScalastyle := scalastyle.in(Test).toTask("").value,
  testGrouping in Test := groupByHash((definedTests in Test).value),
  test in Test := ((test in Test) dependsOn mainScalastyle).value,
  test in Test := ((test in Test) dependsOn testScalastyle).value,
  test in Test := ((test in Test) dependsOn scalafmtCheckAll).value,
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    // Assembly jar is not executable
    case p if p.toLowerCase.contains("manifest.mf") =>
      MergeStrategy.discard
    case _ =>
      // Be permissive for other files
      MergeStrategy.first
  },
  scalacOptions += "-target:jvm-1.8"
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "spark-genomics",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "com.github.samtools" % "htsjdk" % "2.20.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
      "org.seqdoop" % "hadoop-bam" % "7.9.1",
      "log4j" % "log4j" % "1.2.17",
      "org.slf4j" % "slf4j-api" % "1.7.16",
      "org.slf4j" % "slf4j-log4j12" % "1.7.16",
      "org.jdbi" % "jdbi" % "2.63.1",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      // Exclude extraneous GATK dependencies
      ("org.broadinstitute" % "gatk" % "4.0.11.0")
        .exclude("biz.k11i", "xgboost-predictor")
        .exclude("com.google.cloud.bigdataoss", "gcs-connector")
        .exclude("com.intel", "genomicsdb")
        .exclude("org.apache.spark", s"spark-mllib_$scalaMajorMinor")
        .exclude("org.bdgenomics.adam", s"adam-core-spark2_$scalaMajorMinor")
        .exclude("com.google.cloud", "google-cloud-nio"),
      // Test dependencies
      "org.scalatest" %% "scalatest" % "3.0.3" % "test",
      "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
      "org.bdgenomics.adam" %% "adam-apis-spark2" % "0.28.0" % "test",
      "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.11.3" % "test"
    ),
    // Fix versions of libraries that are depended on multiple times
    dependencyOverrides ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "2.7.3",
      "io.netty" % "netty" % "3.9.9.Final",
      "io.netty" % "netty-all" % "4.1.17.Final"
    )
  )

lazy val python =
  (project in file("python"))
    .dependsOn(core % "test->test")
    .settings(
      unmanagedSourceDirectories in Compile := {
        Seq(baseDirectory.value / "glow")
      },
      test in Test := {
        // Pass the test classpath to pyspark so that we run the same bits as the Scala tests
        val classpath = (fullClasspath in Test)
          .value
          .files
          .map(_.getCanonicalPath)
          .mkString(":")
        val ret = Process(
          Seq("pytest", "python"),
          None,
          "SPARK_CLASSPATH" -> classpath,
          "SPARK_HOME" -> (ThisBuild / baseDirectory).value.absolutePath
        ).!
        require(ret == 0, "Python tests failed")
      }
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
