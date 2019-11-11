import sbt._
import sbt.Keys.baseDirectory
import sbtrelease.{ReleasePlugin, Vcs, Versions}
import sbtrelease.ReleasePlugin.runtimeVersion
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys.versions

import scala.sys.process._

/**
 * Changes the version in stable-version.txt during a release.
 */
object StableVersionPlugin extends AutoPlugin {

  override def requires: Plugins = ReleasePlugin

  override def trigger: PluginTrigger = AllRequirements

  object autoImport {

    val stableVersionFile = settingKey[File]("Stable release version file")
    val stableCommitMessage =
      taskKey[String]("The commit message to use when setting the stable version")

    /**
     * Update the stable version file during a release.
     */
    val updateStableVersion: ReleaseStep = updateStableVersionStep(_._1, stableVersionFile)

    /**
     * Commits the stable version file changes.
     */
    val commitStableVersion: ReleaseStep = commitFileStep(stableCommitMessage, stableVersionFile)
  }

  import autoImport._

  override def projectSettings: Seq[Setting[_]] = {
    Seq(
      stableVersionFile := baseDirectory.value / "stable-version.txt",
      stableCommitMessage := s"Setting stable version to ${runtimeVersion.value}"
    )
  }

  private def updateStableVersionStep(
      selectVersion: Versions => String,
      fileSettingKey: SettingKey[File]): ReleaseStep = { st: State =>
    val vs = st
      .get(versions)
      .getOrElse(
        sys.error("No versions are set! Was this release part executed before inquireVersions?"))
    val selected = selectVersion(vs)

    st.log.info("Writing stable version '%s'." format selected)
    IO.writeLines(Project.extract(st).get(fileSettingKey), Seq(selected))
    st
  }

  private def commitFileStep(
      commitMessage: TaskKey[String],
      fileSettingKey: SettingKey[File]): ReleaseStep = { st: State =>
    val log = toProcessLogger(st)
    val file = Project.extract(st).get(fileSettingKey).getCanonicalFile
    val base = vcs(st).baseDir.getCanonicalFile
    val sign = Project.extract(st).get(releaseVcsSign)
    val signOff = Project.extract(st).get(releaseVcsSignOff)
    val relativePath = IO
      .relativize(base, file)
      .getOrElse(
        "Version file [%s] is outside of this VCS repository with base directory [%s]!" format (file, base))

    vcs(st).add(relativePath) !! log
    val status = vcs(st).status.!!.trim

    val newState = if (status.nonEmpty) {
      val (state, msg) = Project.extract(st).runTask(commitMessage, st)
      vcs(state).commit(msg, sign, signOff) ! log
      state
    } else {
      // nothing to commit. this happens if the file hasn't changed.
      st
    }
    newState
  }

  private def toProcessLogger(st: State): ProcessLogger = new ProcessLogger {
    override def err(s: => String): Unit = st.log.info(s)
    override def out(s: => String): Unit = st.log.info(s)
    override def buffer[T](f: => T): T = st.log.buffer(f)
  }

  private def vcs(st: State): Vcs = {
    Project
      .extract(st)
      .get(releaseVcs)
      .getOrElse(
        sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }
}
