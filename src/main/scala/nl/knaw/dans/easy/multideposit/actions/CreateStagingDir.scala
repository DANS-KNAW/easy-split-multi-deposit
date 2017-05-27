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
package nl.knaw.dans.easy.multideposit.actions

import java.nio.file.Files

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.model.DepositId

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

case class CreateStagingDir(row: Int, depositId: DepositId)(implicit settings: Settings) extends UnitAction[Unit] {

  private val stagingDirectory = stagingDir(depositId)
  private val bagDir = stagingBagDir(depositId)
  private val metadataDir = stagingBagMetadataDir(depositId)
  private val dirs = stagingDirectory :: bagDir :: metadataDir :: Nil

  override def checkPreconditions: Try[Unit] = checkDirectoriesDoNotExist

  private def checkDirectoriesDoNotExist: Try[Unit] = {
    dirs.find(Files.exists(_))
      .map(file => Failure(ActionException(row, s"The deposit for dataset $depositId already exists in $file.")))
      .getOrElse(Success(()))
  }

  override def execute(): Try[Unit] = {
    debug(s"making directories: $dirs")
    Try {
      dirs.foreach(Files.createDirectories(_))
    } recoverWith {
      case NonFatal(e) => Failure(ActionException(row, s"Could not create the staging directory at $stagingDirectory", e))
    }
  }

  override def rollback(): Try[Unit] = {
    Try { stagingDirectory.deleteDirectory() } recoverWith {
      case NonFatal(e) => Failure(ActionException(row, s"Could not delete $stagingDirectory, exception: $e", e))
    }
  }
}
