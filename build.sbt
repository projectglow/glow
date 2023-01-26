import scala.sys.process._

import complete.DefaultParsers._
import org.apache.commons.lang3.StringUtils
import sbt.Tests._
import sbt.Keys._
import sbt.librarymanagement.ModuleID
import sbt.nio.Keys._

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"

lazy val spark3 = "3.3.1"

lazy val hailOnSpark3 = "0.2.89"

lazy val sparkVersion = settingKey[String]("sparkVersion")
ThisBuild / sparkVersion := sys.env.getOrElse("SPARK_VERSION", spark3)

lazy val hailVersion = settingKey[String]("hailVersion")
ThisBuild / hailVersion := sys.env.getOrElse("HAIL_VERSION", hailOnSpark3)

// Paths containing Hail tests
lazy val hailTestPaths = Seq("python/glow/hail/", "docs/source/etl/hail.rst")
lazy val ignoreHailTestPathsOption = hailTestPaths.map { p =>
  s"--ignore $p"
}.mkString(" ")

def majorVersion(version: String): String = {
  StringUtils.ordinalIndexOf(version, ".", 1) match {
    case StringUtils.INDEX_NOT_FOUND => version
    case i => version.take(i)
  }
}

def majorMinorVersion(version: String): String = {
  StringUtils.ordinalIndexOf(version, ".", 2) match {
    case StringUtils.INDEX_NOT_FOUND => version
    case i => version.take(i)
  }
}

ThisBuild / scalaVersion := sys.env.getOrElse("SCALA_VERSION", scala212)
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

def groupByHash(tests: Seq[TestDefinition]): Seq[Tests.Group] = {
  tests
    .groupBy(_.name.hashCode % testConcurrency)
    .map {
      case (i, groupTests) =>
        val options = ForkOptions()
          .withRunJVMOptions(Vector("-Dspark.ui.enabled=false", "-Xmx1024m"))

        Group(i.toString, groupTests, SubProcess(options))
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
  scalacOptions += "-target:jvm-1.8",
  resolvers += "Apache Snapshots" at "https://repository.apache.org/snapshots/"
)

lazy val functionsYml = settingKey[File]("functionsYml")
ThisBuild / functionsYml := (ThisBuild / baseDirectory).value / "functions.yml"
// Script to generate function definitions from YAML file
lazy val generatorScript = settingKey[File]("generatorScript")
ThisBuild / generatorScript := (ThisBuild / baseDirectory).value / "python" / "render_template.py"
lazy val generatedFunctionsOutput = settingKey[File]("generatedFunctionsOutput")
lazy val functionsTemplate = settingKey[File]("functionsTemplate")
lazy val generateFunctions = taskKey[Seq[File]]("generateFunctions")
lazy val env = taskKey[Seq[(String, String)]]("env")
lazy val pytest = inputKey[Unit]("pytest")
lazy val hailtest = inputKey[Unit]("hailtest")

def runCmd(args: File*): Unit = {
  args.map(_.getPath).!!
}

lazy val functionGenerationSettings = Seq(
  generateFunctions := {
    runCmd(generatorScript.value, functionsTemplate.value, generatedFunctionsOutput.value)
    Seq.empty[File]
  },
  generateFunctions / fileInputs := Seq(
    functionsTemplate.value,
    functionsYml.value,
    generatorScript.value).map(_.toGlob)
)

lazy val sparkDependencies = settingKey[Seq[ModuleID]]("sparkDependencies")
lazy val providedSparkDependencies = settingKey[Seq[ModuleID]]("providedSparkDependencies")
lazy val testSparkDependencies = settingKey[Seq[ModuleID]]("testSparkDependencies")

ThisBuild / sparkDependencies := Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value,
  "org.apache.spark" %% "spark-core" % sparkVersion.value,
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value,
  "org.apache.spark" %% "spark-sql" % sparkVersion.value
)

ThisBuild / providedSparkDependencies := sparkDependencies.value.map(_ % "provided")
ThisBuild / testSparkDependencies := sparkDependencies.value.map(_ % "test")

lazy val testCoreDependencies = settingKey[Seq[ModuleID]]("testCoreDependencies")
ThisBuild / testCoreDependencies := Seq(
  majorVersion((ThisBuild / sparkVersion).value) match {
    case "3" => "org.scalatest" %% "scalatest" % "3.2.3" % "test"
    case _ => throw new IllegalArgumentException("Only Spark 3 is supported")
  },
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
  "org.xerial" % "sqlite-jdbc" % "3.20.1" % "test"
)

lazy val coreDependencies = settingKey[Seq[ModuleID]]("coreDependencies")
ThisBuild / coreDependencies := (providedSparkDependencies.value ++ testCoreDependencies.value ++ Seq(
  "org.seqdoop" % "hadoop-bam" % "7.9.2",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.jdbi" % "jdbi" % "2.63.1",
  "com.github.broadinstitute" % "picard" % "2.21.9",
  // Fix versions of libraries that are depended on multiple times
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  "io.netty" % "netty" % "3.9.9.Final",
  "io.netty" % "netty-all" % "4.1.68.Final",
  "io.netty" % "netty-handler" % "4.1.68.Final",
  "io.netty" % "netty-transport-native-epoll" % "4.1.68.Final",
  "com.github.samtools" % "htsjdk" % "2.21.2",
  "org.yaml" % "snakeyaml" % "1.16"
)).map(_.exclude("com.google.code.findbugs", "jsr305"))

lazy val root = (project in file(".")).aggregate(core, python, hail, docs)

lazy val scalaLoggingDependency = settingKey[ModuleID]("scalaLoggingDependency")
ThisBuild / scalaLoggingDependency := {
  (ThisBuild / scalaVersion).value match {
    case `scala211` => "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
    case `scala212` => "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
    case _ =>
      throw new IllegalArgumentException(
        "Only supported Scala versions are: " + Seq(scala211, scala212))
  }
}

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    functionGenerationSettings,
    name := s"glow-spark${majorVersion(sparkVersion.value)}",
    publish / skip := false,
    // Adds the Git hash to the MANIFEST file. We set it here instead of relying on sbt-release to
    // do so.
    packageOptions in (Compile, packageBin) += Package.ManifestAttributes("Git-Release-Hash" -> currentGitHash(baseDirectory.value)),
    libraryDependencies ++= coreDependencies.value :+ scalaLoggingDependency.value,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "shim" / majorMinorVersion(sparkVersion.value),
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "shim" / majorMinorVersion(sparkVersion.value),
    functionsTemplate := baseDirectory.value / "functions.scala.TEMPLATE",
    generatedFunctionsOutput := (Compile / scalaSource).value / "io" / "projectglow" / "functions.scala",
    sourceGenerators in Compile += generateFunctions
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

lazy val installHail = taskKey[Unit]("Install Hail")
ThisBuild / installHail := {
  Seq(
    "/bin/bash",
    "-c",
    s"git clone -b ${hailVersion.value} https://github.com/hail-is/hail.git;" + "source $(conda info --base)/etc/profile.d/conda.sh &&" + "conda create -y --name hail &&" + "conda activate hail --stack &&" + "cd \"hail/hail\" &&" + "sed " + "\"" + s"s/^pyspark.*/pyspark==${sparkVersion.value}/" + "\"" + " python/requirements.txt | grep -v '^#' | xargs pip3 install -U &&" +
      s"make SCALA_VERSION=${scalaVersion.value} SPARK_VERSION=${sparkVersion.value} shadowJar wheel &&" +
      s"pip3 install build/deploy/dist/hail-${hailVersion.value}-py3-none-any.whl"
  ) !
}

lazy val uninstallHail = taskKey[Unit]("Uninstall Hail")
ThisBuild / uninstallHail := {
  Seq(
    "/bin/bash",
    "-c",
    "conda env remove --name hail;" + "rm -rf hail"
  ) !
}

lazy val sparkClasspath = taskKey[String]("sparkClasspath")
lazy val sparkHome = taskKey[String]("sparkHome")
lazy val pythonPath = taskKey[String]("pythonPath")

lazy val pythonSettings = Seq(
  libraryDependencies ++= testSparkDependencies.value,
  sparkClasspath := (fullClasspath in Test).value.files.map(_.getCanonicalPath).mkString(":"),
  sparkHome := (ThisBuild / baseDirectory).value.absolutePath,
  pythonPath := ((ThisBuild / baseDirectory).value / "python").absolutePath,
  publish / skip := true,
  env := {
    val baseEnv = Seq(
      // Set so that Python tests can easily know the Spark version
      "SPARK_VERSION" -> sparkVersion.value,
      // Tell PySpark to use the same jars as our scala tests
      "SPARK_CLASSPATH" -> sparkClasspath.value,
      "SPARK_HOME" -> sparkHome.value,
      "PYTHONPATH" -> pythonPath.value
    )
    if (majorMinorVersion(sparkVersion.value) >= "3.0") {
      baseEnv :+ "PYSPARK_ROW_FIELD_SORTING_ENABLED" -> "true"
    } else {
      baseEnv :+ "ARROW_PRE_0_15_IPC_FORMAT" -> "1"
    }
  },
  pytest := {
    val args = spaceDelimited("<arg>").parsed
    val ret = Process("pytest " + args.mkString(" "), None, (env.value): _*).!
    require(ret == 0, "Python tests failed")
  },
  hailtest := {
    val args = spaceDelimited("<arg>").parsed
    val ret = Process(
      Seq(
        "/bin/bash",
        "-c",
        "source $(conda info --base)/etc/profile.d/conda.sh &&" + "conda activate hail --stack &&" + "pytest " + args.mkString(" ")),
      None,
      (env.value): _*
    ).!
    require(ret == 0, "Python tests in Hail environment failed")
  }
)

lazy val yapf = inputKey[Unit]("yapf")
ThisBuild / yapf := {
  val args = spaceDelimited("<arg>").parsed
  val ret = Process(
    Seq("yapf") ++ args ++ Seq(
      "--style",
      "python/.style.yapf",
      "--recursive",
      "--exclude",
      "python/glow/functions.py",
      "--exclude",
      "python/build/lib/glow/functions.py",
      "python")
  ).!
  require(ret == 0, "Python style tests failed")
}

val yapfAll = taskKey[Unit]("Execute the yapf task in-place for all Python files.")
ThisBuild / yapfAll := yapf.toTask(" --in-place").value

lazy val python =
  (project in file("python"))
    .settings(
      pythonSettings,
      functionGenerationSettings,
      test in Test := {
        yapf.toTask(" --diff").value
        pytest.toTask(s" --doctest-modules $ignoreHailTestPathsOption python").value
      },
      generatedFunctionsOutput := baseDirectory.value / "glow" / "functions.py",
      functionsTemplate := baseDirectory.value / "glow" / "functions.py.TEMPLATE",
      sourceGenerators in Compile += generateFunctions
    )
    .dependsOn(core % "test->test")

lazy val hail = (project in file("python/glow/hail"))
  .settings(
    pythonSettings,
    test in Test := {
      hailtest.toTask(s" --doctest-modules ${hailTestPaths.mkString(" ")}").value
    }
  )
  .dependsOn(core % "test->test", python)

lazy val docs = (project in file("docs"))
  .settings(
    pythonSettings,
    test in Test := {
      pytest.toTask(s" $ignoreHailTestPathsOption docs").value
    }
  )
  .dependsOn(core % "test->test", python)

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
    "henry@davidge.me",
    url("https://github.com/henrydavidge")),
  Developer(
    "karenfeng",
    "Karen Feng",
    "karen.feng@databricks.com",
    url("https://github.com/karenfeng")),
  Developer(
    "kianfar77",
    "Kiavash Kianfar",
    "kiavash.kianfar@databricks.com",
    url("https://github.com/kianfar77"))
)

ThisBuild / pomIncludeRepository := { _ =>
  false
}
ThisBuild / publishMavenStyle := true

lazy val stableVersion = settingKey[String]("Stable version")
ThisBuild / stableVersion := IO
  .read((ThisBuild / baseDirectory).value / "stable-version.txt")
  .trim()

lazy val stagedRelease = (project in file("core/src/test"))
  .settings(
    commonSettings,
    resourceDirectory in Test := baseDirectory.value / "resources",
    scalaSource in Test := baseDirectory.value / "scala",
    unmanagedSourceDirectories in Test += baseDirectory.value / "shim" / majorMinorVersion(
      sparkVersion.value),
    libraryDependencies ++= testSparkDependencies.value ++ testCoreDependencies.value :+ "io.projectglow" %% s"glow-spark${majorVersion(sparkVersion.value)}" % stableVersion.value % "test",
    resolvers := Seq(MavenCache("local-sonatype-staging", sonatypeBundleDirectory.value)),
    org
      .jetbrains
      .sbt
      .extractors
      .SettingKeys
      .sbtIdeaIgnoreModule := true // Do not import this SBT project into IDEA
  )

import ReleaseTransformations._

// Don't use sbt-release's cross facility
releaseCrossBuild := false

lazy val updateCondaEnv = taskKey[Unit]("Update Glow Env To Latest Version")
updateCondaEnv := {
  "conda env update -f python/environment.yml" !
}

def crossReleaseStep(step: ReleaseStep, requiresPySpark: Boolean, requiresHail: Boolean): Seq[ReleaseStep] = {
  val updateCondaEnvStep = releaseStepCommandAndRemaining(
    if (requiresPySpark) "updateCondaEnv" else "")
  val installHailStep = releaseStepCommandAndRemaining(if (requiresHail) "installHail" else "")
  val uninstallHailStep = releaseStepCommandAndRemaining(if (requiresHail) "uninstallHail" else "")

  Seq(
    updateCondaEnvStep,
    releaseStepCommandAndRemaining(s"""set ThisBuild / sparkVersion := "$spark3""""),
    releaseStepCommandAndRemaining(s"""set ThisBuild / scalaVersion := "$scala212""""),
    releaseStepCommandAndRemaining(s"""set ThisBuild / hailVersion := "$hailOnSpark3""""),
    installHailStep,
    step,
    uninstallHailStep,
    updateCondaEnvStep
  )
}

def sonatypeSteps(): Seq[ReleaseStep] = Seq(
  releaseStepCommandAndRemaining("sonatypePrepare"),
  releaseStepCommandAndRemaining("sonatypeBundleUpload")
)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean
 ) ++ crossReleaseStep(releaseStepCommandAndRemaining("core/test"), requiresPySpark = false, requiresHail = false) ++
// commenting out for Spark 3.2 release until hail is on spark 3.2
//  crossReleaseStep(releaseStepCommandAndRemaining("python/test"), requiresPySpark = true, requiresHail = false) ++
//  crossReleaseStep(
//    releaseStepCommandAndRemaining("docs/test"),
//    requiresPySpark = true,
//    requiresHail = false) ++
//  crossReleaseStep(
//    releaseStepCommandAndRemaining("hail/test"),
//    requiresPySpark = true,
//    requiresHail = true) ++
  Seq(
    setReleaseVersion,
    updateStableVersion,
    commitReleaseVersion,
    commitStableVersion,
    tagRelease
  ) ++
  crossReleaseStep(
    releaseStepCommandAndRemaining("publishSigned"),
    requiresPySpark = false,
    requiresHail = false) ++
  sonatypeSteps ++
  crossReleaseStep(
    releaseStepCommandAndRemaining("stagedRelease/test"),
    requiresPySpark = false,
    requiresHail = false) ++
  Seq(
    setNextVersion,
    commitNextVersion
  )
