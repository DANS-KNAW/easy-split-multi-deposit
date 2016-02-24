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
package nl.knaw.dans.easy.multiDeposit.actions

import gov.loc.repository.bagit.BagFactory
import gov.loc.repository.bagit.BagFactory.Version
import gov.loc.repository.bagit.writer.impl.FileSystemWriter
import nl.knaw.dans.easy.multiDeposit._
import nl.knaw.dans.easy.multiDeposit.actions.AddBagToDepositAction._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.seqAsJavaList
import scala.util.{Failure, Success, Try}

case class AddBagToDepositAction(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions = {
    log.debug(s"Checking preconditions for $this")

    val inputDir = multiDepositDir(settings, datasetID)
    if (inputDir.exists) Success(Unit)
    else Failure(ActionException(row, s"Dataset $datasetID: deposit directory does not exist"))
  }

  def run() = {
    log.debug(s"Running $this")

    createBag(datasetID)
  }

  def rollback() = Success(Unit)
}
object AddBagToDepositAction {
  // for examples see https://github.com/LibraryOfCongress/bagit-java/issues/18
  //              and http://www.mpcdf.mpg.de/services/data/annotate/downloads -> TacoHarvest
  def createBag(datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    Try {
      val inputDir = multiDepositDir(settings, datasetID)
      val depositDir = depositBagDir(settings, datasetID)

      val bagFactory = new BagFactory
      val preBag = bagFactory.createPreBag(depositDir)
      val bag = bagFactory.createBag(depositDir)

      bag.addFilesToPayload(inputDir.listFiles.toList)
      bag.makeComplete

      val fsw = new FileSystemWriter(bagFactory)
      fsw.write(bag, depositDir)

      preBag.makeBagInPlace(Version.V0_97, false)
    }
  }
}
