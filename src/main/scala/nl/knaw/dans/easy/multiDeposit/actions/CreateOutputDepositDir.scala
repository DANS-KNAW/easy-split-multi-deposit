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

import nl.knaw.dans.easy.multideposit
import nl.knaw.dans.easy.multideposit._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CreateOutputDepositDir(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions: Try[Unit] = Success(Unit)

  def run(): Try[Unit] = {
    log.debug(s"Running $this")
    val depositDir = multideposit.outputDepositDir(settings, datasetID)
    val bagDir = outputDepositBagDir(settings, datasetID)
    val metadataDir = outputDepositBagMetadataDir(settings, datasetID)

    log.debug(s"Creating Deposit Directory at $depositDir with bag directory = $bagDir and metadata directory = $metadataDir")

    if (depositDir.mkdirs && bagDir.mkdir && metadataDir.mkdir)
      Success(Unit)
    else
      Failure(new ActionException(row, s"Could not create the dataset output deposit directory at $depositDir"))
  }

  def rollback(): Try[Unit] = {
    val depositDir = multideposit.outputDepositDir(settings, datasetID)
    log.debug(s"Deleting directory $depositDir")

    Try {
      depositDir.deleteDirectory()
    } recoverWith {
      case e: Exception => Failure(ActionException(row, s"Could not delete $depositDir, exception: $e", e))
    }
  }
}
