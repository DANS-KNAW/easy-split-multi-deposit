/*
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
package nl.knaw.dans.easy.multideposit.actions

import better.files.File
import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.easy.multideposit.{ ActionError, FailFast }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class CreateDirectories extends DebugEnhancedLogging {

  def createDepositDirectories(depositId: DepositId)(implicit stage: StagingPathExplorer): FailFast[Unit] = {
    createDirectories(stage.stagingDir(depositId), stage.stagingBagDir(depositId))
  }

  def createMetadataDirectory(depositId: DepositId)(implicit stage: StagingPathExplorer): FailFast[Unit] = {
    createDirectories(stage.stagingBagMetadataDir(depositId))
  }

  private def createDirectories(directories: File*): FailFast[Unit] = {
    Either.catchNonFatal {
      for (directory <- directories) {
        logger.debug(s"create directory $directory")
        directory.createDirectories()
      }
    }.leftMap(e => ActionError(s"Could not create the directories at $directories", e))
  }

  def discardDeposit(depositId: DepositId)(implicit stage: StagingPathExplorer): FailFast[Unit] = {
    logger.debug(s"delete deposit '$depositId' from staging directory")

    val dir = stage.stagingDir(depositId)
    Either.catchNonFatal { if (dir.exists) dir.delete(); () }
      .leftMap(e => ActionError(s"Could not delete $dir", e))
  }
}
