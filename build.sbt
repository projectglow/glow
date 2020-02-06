import scala.sys.process._

import sbt.Tests._

val sparkVersion = "2.4.3"

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"

ThisBuild / scalaVersion := scala211
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

// Script to generate function definitions from YAML file
lazy val generatorScript = taskKey[File]("generatorScript")
ThisBuild / generatorScript := (ThisBuild / baseDirectory).value / "python" / "render_template.py"
def runCmd(args: File*): Unit = {
  args.map(_.getPath).!!
}

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

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

lazy val providedSparkDependencies = sparkDependencies.map(_ % "provided")
lazy val testSparkDependencies = sparkDependencies.map(_ % "test")

lazy val testCoreDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.xerial" % "sqlite-jdbc" % "3.20.1" % "test"
)

lazy val coreDependencies = (providedSparkDependencies ++ testCoreDependencies ++ Seq(
  "org.seqdoop" % "hadoop-bam" % "7.9.2",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.jdbi" % "jdbi" % "2.63.1",
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
  // Fix versions of libraries that are depended on multiple times
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "io.netty" % "netty" % "3.9.9.Final",
  "io.netty" % "netty-all" % "4.1.17.Final",
  "com.github.samtools" % "htsjdk" % "2.20.3"
)).map(_.exclude("com.google.code.findbugs", "jsr305"))

lazy val root = (project in file("."))
  .aggregate(core_2_11, python_2_11, docs_2_11, core_2_12, python_2_12, docs_2_12)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "glow",
    publish / skip := false,
    // Adds the Git hash to the MANIFEST file. We set it here instead of relying on sbt-release to
    // do so.
    packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Git-Release-Hash" -> currentGitHash(baseDirectory.value)),
    bintrayRepository := "glow",
    libraryDependencies ++= coreDependencies,
    sourceGenerators in Compile += Def.task {
      val file = baseDirectory.value / "functions.scala.TEMPLATE"
      val output = (Compile / scalaSource).value / "io" / "projectglow" / "functions.scala"
      runCmd(generatorScript.value, file, output)
      Seq(output)
    }.taskValue
  )
  .cross

lazy val core_2_11 = core(scala211)
  .settings(
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
  )

lazy val core_2_12 = core(scala212)
  .settings(
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
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
lazy val pythonPath = taskKey[String]("pythonPath")

lazy val pythonSettings = Seq(
  libraryDependencies ++= testSparkDependencies,
  sparkClasspath := (fullClasspath in Test).value.files.map(_.getCanonicalPath).mkString(":"),
  sparkHome := (ThisBuild / baseDirectory).value.absolutePath,
  pythonPath := ((ThisBuild / baseDirectory).value / "python" / "glow").absolutePath,
  publish / skip := true
)

lazy val python =
  (project in file("python"))
    .settings(
      pythonSettings,
      test in Test := {
        // Pass the test classpath to pyspark so that we run the same bits as the Scala tests
        val ret = Process(
          Seq("pytest", "python"),
          None,
          "SPARK_CLASSPATH" -> sparkClasspath.value,
          "SPARK_HOME" -> sparkHome.value
        ).!
        require(ret == 0, "Python tests failed")
      },
      sourceGenerators in Compile += Def.task {
        val file = baseDirectory.value / "glow" / "functions.py.TEMPLATE"
        val output = baseDirectory.value / "glow" / "functions.py"
        runCmd(generatorScript.value, file, output)
        Seq.empty[File]
      }.taskValue
    )
    .cross
    .dependsOn(core % "test->test")

lazy val python_2_11 = python(scala211)
lazy val python_2_12 = python(scala212)

lazy val docs = (project in file("docs"))
  .settings(
    pythonSettings,
    test in Test := {
      // Pass the test classpath to pyspark so that we run the same bits as the Scala tests
      val ret = Process(
        Seq("pytest", "docs"),
        None,
        "SPARK_CLASSPATH" -> sparkClasspath.value,
        "SPARK_HOME" -> sparkHome.value,
        "PYTHONPATH" -> pythonPath.value
      ).!
      require(ret == 0, "Docs tests failed")
    }
  )
  .cross
  .dependsOn(core % "test->test")

lazy val docs_2_11 = docs(scala211)
lazy val docs_2_12 = docs(scala212)

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

lazy val stableVersion = settingKey[String]("Stable version")
ThisBuild / stableVersion := IO
  .read((ThisBuild / baseDirectory).value / "stable-version.txt")
  .trim()

lazy val stagedRelease = (project in file("core/src/test"))
  .settings(
    commonSettings,
    resourceDirectory in Test := baseDirectory.value / "resources",
    scalaSource in Test := baseDirectory.value / "scala",
    libraryDependencies ++= testSparkDependencies ++ testCoreDependencies :+
    "io.projectglow" %% "glow" % stableVersion.value % "test",
    resolvers := Seq("bintray-staging" at "https://dl.bintray.com/projectglow/glow"),
    org.jetbrains.sbt.extractors.SettingKeys.sbtIdeaIgnoreModule := true // Do not import this SBT project into IDEA
  )
  .cross

lazy val stagedRelease_2_11 = stagedRelease(scala211)
lazy val stagedRelease_2_12 = stagedRelease(scala212)

import ReleaseTransformations._

// Don't use sbt-release's cross facility	
releaseCrossBuild := false

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  updateStableVersion,
  commitReleaseVersion,
  commitStableVersion,
  tagRelease,
  publishArtifacts,
  releaseStepCommandAndRemaining("stagedRelease_2_11/test"),
  releaseStepCommandAndRemaining("stagedRelease_2_12/test"),
  setNextVersion,
  commitNextVersion
)
