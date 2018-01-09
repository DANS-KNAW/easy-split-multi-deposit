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
package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit2.model.DepositId
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }

trait CreateDirectories extends DebugEnhancedLogging {
  this: StagingPathExplorer =>

  def createDepositDirectories(depositId: DepositId): Try[Unit] = {
    createDirectories(stagingDir(depositId), stagingBagDir(depositId))
  }

  def createMetadataDirectory(depositId: DepositId): Try[Unit] = {
    createDirectories(stagingBagMetadataDir(depositId))
  }

  private def createDirectories(paths: Path*): Try[Unit] = {
    Try {
      for (path <- paths) {
        logger.debug(s"create directory $path")
        Files.createDirectories(path)
      }
    } recoverWith {
      case NonFatal(e) => Failure(ActionException(s"Could not create the directories at $paths", e))
    }
  }

  def discardDeposit(depositId: DepositId): Try[Unit] = {
    logger.debug(s"delete deposit '$depositId' from staging directory")

    val dir = stagingDir(depositId)
    Try { if (Files.exists(dir)) dir.deleteDirectory() } recoverWith {
      case NonFatal(e) => Failure(ActionException(s"Could not delete $dir", e))
    }
  }
}
