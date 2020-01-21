import scala.sys.process._

import sbt.Tests._

val sparkVersion = "2.4.3"

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

ThisBuild / scalaVersion := scala212
ThisBuild / organization := "io.projectglow"
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"
ThisBuild / crossScalaVersions := Nil
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

def groupByHash(tests: Seq[TestDefinition]): Seq[Tests.Group] = {
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

lazy val scalaLogging = SettingKey[String]("scalaLogging")
scalaLogging := scalaBinaryVersion.value match {
  case "2.11" => "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
  case _ => "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
}

lazy val dependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.seqdoop" % "hadoop-bam" % "7.9.2",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.jdbi" % "jdbi" % "2.63.1",
  scalaLogging,
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
    .exclude("org.apache.spark", s"spark-mllib_2.11")
    .exclude("org.bdgenomics.adam", s"adam-core-spark2_2.11")
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
    .exclude("org.xerial", "sqlite-jdbc")
    .exclude("com.github.fommil.netlib", "*"),
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.bdgenomics.adam" %% "adam-apis-spark2" % "0.29.0" % "test",
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.14.0" % "test",
  "org.xerial" % "sqlite-jdbc" % "3.20.1" % "test"
).map(_.exclude("com.google.code.findbugs", "jsr305"))

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "glow",
    crossScalaVersions := supportedScalaVersions,
    publish / skip := false,
    // Adds the Git hash to the MANIFEST file. We set it here instead of relying on sbt-release to
    // do so.
    packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Git-Release-Hash" -> currentGitHash(baseDirectory.value)),
    bintrayRepository := "glow",
    libraryDependencies ++= dependencies,
    // Fix versions of libraries that are depended on multiple times
    dependencyOverrides ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "2.7.3",
      "io.netty" % "netty" % "3.9.9.Final",
      "io.netty" % "netty-all" % "4.1.17.Final",
      "com.github.samtools" % "htsjdk" % "2.20.3"
    )
  )

/**
 * @param dir The base directory of the Git project
 * @return The commit of HEAD
 */
def currentGitHash(dir: File): String = {
  Process(
    Seq("git", "rev-parse", "HEAD"),
    // Set the working directory for Git to the passed in directory
    Some(dir)
  ).!!.trim
}

lazy val sparkClasspath = taskKey[String]("sparkClasspath")
lazy val sparkHome = taskKey[String]("sparkHome")

lazy val pythonSettings = Seq(
  sparkClasspath := (fullClasspath in Test).value.files.map(_.getCanonicalPath).mkString(":"),
  sparkHome := (ThisBuild / baseDirectory).value.absolutePath,
  publish / skip := true
)

lazy val python =
  (project in file("python"))
    .dependsOn(core % "test->test")
    .settings(
      pythonSettings,
      unmanagedSourceDirectories in Compile := {
        Seq(baseDirectory.value / "glow")
      },
      test in Test := {
        // Pass the test classpath to pyspark so that we run the same bits as the Scala tests
        val ret = Process(
          Seq("pytest", "python"),
          None,
          "SPARK_CLASSPATH" -> sparkClasspath.value,
          "SPARK_HOME" -> sparkHome.value
        ).!
        require(ret == 0, "Python tests failed")
      }
    )

lazy val docs =
  (project in file("docs"))
    .dependsOn(core % "test->test")
    .settings(
      pythonSettings,
      test in Test := {
        // Pass the test classpath to pyspark so that we run the same bits as the Scala tests
        val ret = Process(
          Seq("pytest", "docs"),
          None,
          "SPARK_CLASSPATH" -> sparkClasspath.value,
          "SPARK_HOME" -> sparkHome.value,
          "PYTHONPATH" -> ((ThisBuild / baseDirectory).value / "python" / "glow").absolutePath
        ).!
        require(ret == 0, "Docs tests failed")
      }
    )

// List tests to parallelize on CircleCI
lazy val printTests =
  taskKey[Unit]("Print full class names of Scala tests to core-test-names.log.")

printTests := {
  IO.writeLines(
    baseDirectory.value / "core-test-names.log",
    (definedTestNames in Test in core).value.sorted)
}

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

// don't use sbt-release's cross facility
releaseCrossBuild := false
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  releaseStepCommandAndRemaining("+test"),
  setReleaseVersion,
  updateStableVersion,
  commitReleaseVersion,
  commitStableVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion
)
