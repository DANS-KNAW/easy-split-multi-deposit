/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit

import java.io.ByteArrayOutputStream

import better.files.File
import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.FfprobeRunner.FfprobeError
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.io.IOUtils

import scala.language.postfixOps
import scala.sys.process.{ ProcessIO, _ }

/**
 * Simple runner for ffprobe. The constructor will fail if the executable does not exist or the current user
 * has no execute permissions.
 */
trait FfprobeRunner {
  val ffprobeExe: File

  /**
   * Runs ffprobe on a given file. If a non-zero exit value is returned from the ffprobe process, a [[nl.knaw.dans.easy.multideposit.FfprobeRunner.FfprobeError]] is
   * returned, which details the exit code and the standard error contents.
   *
   * @param target the target to probe
   * @return the result of the call
   */
  def run(target: File): Either[FfprobeError, Unit] = {
    val err = new ByteArrayOutputStream()
    Either.catchNonFatal {
      /*
       * We are ignoring the STDOUT, as we are not interested in the output on successful execution. Note, by the way that
       * ffprobe returns its messages on the STDERR on successful execution as well!
       */
      val proc = Seq(ffprobeExe.toString(), target.toString) run new ProcessIO(_.close(), _ => (), IOUtils.copy(_, err))
      proc.exitValue
    }
      .leftMap(e => FfprobeError(target, -1, e.getMessage))
      .flatMap {
        exit =>
          if (exit == 0) ().asRight
          else FfprobeError(target, exit, new String(err.toByteArray)).asLeft
      }
  }
}

object FfprobeRunner extends DebugEnhancedLogging {

  /**
   * Constructs an FfprobeRunner
   *
   * @param exe absolute path of the ffprobe executable
   * @return
   */
  def apply(exe: File): FfprobeRunner = new FfprobeRunner {
    override val ffprobeExe: File = exe
  }

  case class FfprobeError(file: File, exitValue: Int, err: String)
}
