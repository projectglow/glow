import xerial.sbt.Sonatype._

sonatypeProfileName := "io.projectglow"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("projectglow", "glow", "karen.feng@databricks.com"))

developers := List(
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

// Release using Sonatype
publishTo := sonatypePublishToBundle.value
