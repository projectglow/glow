sonatypeProfileName := "io.projectglow"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("projectglow", "glow", "karen.feng@databricks.com"))
