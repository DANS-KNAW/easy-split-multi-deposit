/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.actions

import nl.knaw.dans.easy.multideposit._

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

case class CreateOutputDepositDir(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action {

  private val depositDir = outputDepositDir(datasetID)
  private val bagDir = outputDepositBagDir(datasetID)
  private val metadataDir = outputDepositBagMetadataDir(datasetID)
  private val dirs = depositDir :: bagDir :: metadataDir :: Nil

  override def checkPreconditions: Try[Unit] = {
    for {
      _ <- checkDatasetIdIsValid
      _ <- checkDirectoriesDoNotExist
    } yield ()
  }

  private def checkDirectoriesDoNotExist: Try[Unit] = {
    dirs.find(_.exists)
      .map(file => Failure(ActionException(row, s"The deposit for dataset $datasetID already exists in $file.")))
      .getOrElse(Success(()))
  }

  private def checkDatasetIdIsValid: Try[Unit] = {
    val illegalCharacters = "[^a-zA-Z0-9_-]".r.findAllIn(datasetID).toSet
    if (illegalCharacters.isEmpty)
      Success(())
    else
      Failure(ActionException(row, s"The datasetId '$datasetID' contains the following invalid characters: ${ illegalCharacters.mkString("{ ", ", ", " }") }"))
  }

  override def execute(): Try[Unit] = {
    debug(s"making directories: $dirs")
    if (dirs.forall(_.mkdirs)) Success(())
    else Failure(ActionException(row, s"Could not create the dataset output deposit directory at $depositDir"))
  }

  override def rollback(): Try[Unit] = {
    Try { depositDir.deleteDirectory() } recoverWith {
      case NonFatal(e) => Failure(ActionException(row, s"Could not delete $depositDir, exception: $e", e))
    }
  }
}
