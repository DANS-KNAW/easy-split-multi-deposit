package nl.knaw.dans.easy.multideposit

import java.io.ByteArrayOutputStream

import better.files.File
import nl.knaw.dans.easy.multideposit.actions.FfprobeErrorException
import org.apache.commons.io.IOUtils

import scala.util.{ Failure, Success, Try }
import scala.sys.process.{ ProcessIO, _ }
import scala.language.postfixOps

/**
 * Simple runner for ffprobe. The constructor will fail if the executable does not exist or the current user
 * has no execute permissions.
 *
 * @param ffprobeExe the ffprobe executable to run.
 */
class FfprobeRunner(ffprobeExe: File) {
  require(ffprobeExe exists, "Cannot create ExeRunner for executable that does not exist")
  require(ffprobeExe isExecutable, "Program is not executable")

  /**
   * Runs ffprobe on a given file. If a non-zero exit value is returned from the ffprobe process, a [[FfprobeErrorException]] is
   * returned, which details the exit code and the standard error contents.
   *
   * @param target the target to probe
   * @return the result of the call
   */
  def run(target: File): Try[Unit] = {
    val err = new ByteArrayOutputStream()
    Try {
      /*
       * We are ignoring the STDOUT, as we are not interested in the output on successful execution. Note, by the way that
       * ffprobe returns its messages on the STDERR on successful execution as well!
       */
      val proc = Seq(ffprobeExe.toString(), target.toString) run new ProcessIO(_.close(), _ => (), IOUtils.copy(_, err))
      proc.exitValue
    }.flatMap {
      exit =>
        if (exit == 0) Success(())
        else Failure(FfprobeErrorException(target, exit, err.toString))
    }
  }
}
