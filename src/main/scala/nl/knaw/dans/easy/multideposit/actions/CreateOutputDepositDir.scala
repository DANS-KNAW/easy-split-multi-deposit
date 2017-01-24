/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

case class CreateOutputDepositDir(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action with DebugEnhancedLogging {

  private val depositDir = outputDepositDir(settings, datasetID)
  private val bagDir = outputDepositBagDir(settings, datasetID)
  private val metadataDir = outputDepositBagMetadataDir(settings, datasetID)
  private val dirs = depositDir :: bagDir :: metadataDir :: Nil

  override def checkPreconditions: Try[Unit] = {
    debug(s"Checking preconditions for $this")

    dirs.find(_.exists)
      .map(file => Failure(ActionException(row, s"The deposit for dataset $datasetID already exists in $file.")))
      .getOrElse(Success(()))
  }

  def execute(): Try[Unit] = {
    debug(s"Running $this")
    debug(s"Creating Deposit Directory at $depositDir with bag directory = $bagDir and metadata directory = $metadataDir")

    if (dirs.forall(_.mkdirs))
      Success(())
    else
      Failure(ActionException(row, s"Could not create the dataset output deposit directory at $depositDir"))
  }

  override def rollback(): Try[Unit] = {
    debug(s"Rolling back $this")
    debug(s"Deleting directory $depositDir")

    Try {
      depositDir.deleteDirectory()
    } recoverWith {
      case e: Exception => Failure(ActionException(row, s"Could not delete $depositDir, exception: $e", e))
    }
  }
}
