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

import java.io.{ File, FileInputStream }

import gov.loc.repository.bagit.BagFactory
import gov.loc.repository.bagit.BagFactory.Version
import gov.loc.repository.bagit.Manifest.Algorithm
import gov.loc.repository.bagit.transformer.impl.DefaultCompleter
import gov.loc.repository.bagit.utilities.MessageDigestHelper
import gov.loc.repository.bagit.writer.impl.FileSystemWriter
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddBagToDeposit._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.JavaConversions.seqAsJavaList
import scala.util.{ Failure, Try }

case class AddBagToDeposit(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action with DebugEnhancedLogging {

  def execute(): Try[Unit] = {
    debug(s"Running $this")

    createBag(datasetID)
      .recoverWith { case e => Failure(ActionException(row, s"Error occured in creating the bag for $datasetID: ${ e.getMessage }", e)) }
  }
}
object AddBagToDeposit {
  // for examples see https://github.com/LibraryOfCongress/bagit-java/issues/18
  //              and http://www.mpcdf.mpg.de/services/data/annotate/downloads -> TacoHarvest
  def createBag(datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    Try {
      val inputDir = multiDepositDir(settings, datasetID)
      val inputDirExists = inputDir.exists
      val outputBagDir = outputDepositBagDir(settings, datasetID)

      val bagFactory = new BagFactory
      val preBag = bagFactory.createPreBag(outputBagDir)
      val bag = bagFactory.createBag(outputBagDir)

      if (inputDirExists) bag.addFilesToPayload(inputDir.listFiles.toList)
      bag.makeComplete

      val fsw = new FileSystemWriter(bagFactory)
      if (!inputDirExists) fsw.setTagFilesOnly(true)
      fsw.write(bag, outputBagDir)

      val algorithm = Algorithm.SHA1
      val completer = new DefaultCompleter(bagFactory)
      completer.setTagManifestAlgorithm(algorithm)
      completer.setPayloadManifestAlgorithm(algorithm)

      if (!inputDirExists) preBag.setIgnoreAdditionalDirectories(List(metadataDirName))
      preBag.makeBagInPlace(Version.V0_97, false, completer)

      // TODO, this is temporary, waiting for response from the BagIt-Java developers.
      if (!inputDirExists) {
        new File(outputBagDir, "data").mkdir()
        new File(outputBagDir, "manifest-sha1.txt").write("")
        new File(outputBagDir, "tagmanifest-sha1.txt").append(s"${MessageDigestHelper.generateFixity(new FileInputStream(new File(outputBagDir, "manifest-sha1.txt")), Algorithm.SHA1)}  manifest-sha1.txt")
      }
    }
  }
}
