import scala.sys.process._

import sbt.Tests._

val sparkVersion = "2.4.3"
val scalaMajorMinor = "2.11"

ThisBuild / scalaVersion := s"$scalaMajorMinor.12"
ThisBuild / organization := "io.projectglow"
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"
ThisBuild / publish / skip := true

ThisBuild / organizationName := "The Glow Authors"
ThisBuild / startYear := Some(2019)
ThisBuild / licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

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
          .withRunJVMOptions(Vector("-Dspark.ui.enabled=false", "-Xmx1024m"))
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
  test in Test := ((test in Test) dependsOn (headerCheck in Compile)).value,
  test in Test := ((test in Test) dependsOn (headerCheck in Test)).value,
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

lazy val dependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.seqdoop" % "hadoop-bam" % "7.9.1",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.16",
  "org.slf4j" % "slf4j-log4j12" % "1.7.16",
  "org.jdbi" % "jdbi" % "2.63.1",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  // Exclude extraneous GATK dependencies
  ("org.broadinstitute" % "gatk" % "4.0.11.0")
    .exclude("biz.k11i", "xgboost-predictor")
    .exclude("com.esotericsoftware", "kryo")
    .exclude("com.esotericsoftware", "reflectasm")
    .exclude("com.github.jsr203hadoop", "jsr203hadoop")
    .exclude("com.google.cloud", "google-cloud-nio")
    .exclude("com.google.cloud.bigdataoss", "gcs-connector")
    .exclude("com.intel", "genomicsdb")
    .exclude("com.intel.gkl", "gkl")
    .exclude("com.opencsv", "opencsv")
    .exclude("commons-io", "commons-io")
    .exclude("gov.nist.math.jama", "gov.nist.math.jama")
    .exclude("it.unimi.dsi", "fastutil")
    .exclude("org.aeonbits.owner", "owner")
    .exclude("org.apache.commons", "commons-lang3")
    .exclude("org.apache.commons", "commons-math3")
    .exclude("org.apache.commons", "commons-collections4")
    .exclude("org.apache.commons", "commons-vfs2")
    .exclude("org.apache.hadoop", "hadoop-client")
    .exclude("org.apache.spark", s"spark-mllib_$scalaMajorMinor")
    .exclude("org.bdgenomics.adam", s"adam-core-spark2_$scalaMajorMinor")
    .exclude("org.broadinstitute", "barclay")
    .exclude("org.broadinstitute", "hdf5-java-bindings")
    .exclude("org.broadinstitute", "gatk-native-bindings")
    .exclude("org.broadinstitute", "gatk-bwamem-jni")
    .exclude("org.broadinstitute", "gatk-fermilite-jni")
    .exclude("org.jgrapht", "jgrapht-core")
    .exclude("org.objenesis", "objenesis")
    .exclude("org.ojalgo", "ojalgo")
    .exclude("org.ojalgo", "ojalgo-commons-math3")
    .exclude("org.reflections", "reflections")
    .exclude("org.seqdoop", "hadoop-bam")
    .exclude("org.xerial", "sqlite-jdbc"),
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.bdgenomics.adam" %% "adam-apis-spark2" % "0.28.0" % "test",
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.11.3" % "test",
  "org.xerial" % "sqlite-jdbc" % "3.20.1" % "test"
).map(_.exclude("com.google.code.findbugs", "jsr305"))

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "glow",
    publish / skip := false,
    bintrayRepository := "glow",
    libraryDependencies ++= dependencies,
    // Fix versions of libraries that are depended on multiple times
    dependencyOverrides ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "2.7.3",
      "io.netty" % "netty" % "3.9.9.Final",
      "io.netty" % "netty-all" % "4.1.17.Final",
      "com.github.samtools" % "htsjdk" % "2.20.1"
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
      },
      publish / skip := true
    )

// Publish to Bintray
ThisBuild / description := "An open-source toolkit for large-scale genomic analysis"
ThisBuild / homepage := Some(url("https://projectglow.io"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/projectglow/glow"),
    "scm:git@github.com:projectglow/glow.git"
  )
)
ThisBuild / developers := List(
  Developer(
    "henrydavidge",
    "Henry Davidge",
    "hhd@databricks.com",
    url("https://github.com/henrydavidge")),
  Developer(
    "karenfeng",
    "Karen Feng",
    "karen.feng@databricks.com",
    url("https://github.com/karenfeng"))
)
ThisBuild / pomIncludeRepository := { _ =>
  false
}
ThisBuild / publishMavenStyle := true

ThisBuild / bintrayOrganization := Some("projectglow")
ThisBuild / bintrayRepository := "glow"

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)
