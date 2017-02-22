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

import java.io.{ File, FileInputStream }

import gov.loc.repository.bagit.BagFactory.Version
import gov.loc.repository.bagit.Manifest.Algorithm
import gov.loc.repository.bagit.transformer.Completer
import gov.loc.repository.bagit.transformer.impl.{ ChainingCompleter, DefaultCompleter, TagManifestCompleter }
import gov.loc.repository.bagit.utilities.MessageDigestHelper
import gov.loc.repository.bagit.writer.impl.FileSystemWriter
import gov.loc.repository.bagit.{ Bag, BagFactory }
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddBagToDeposit._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{ Failure, Try }

case class AddBagToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends Action {

  val (datasetID, dataset) = entry

  override def execute(): Try[Unit] = {
    createBag(datasetID, dataset).recoverWith {
      case NonFatal(e) => Failure(ActionException(row, s"Error occured in creating the bag for $datasetID: ${ e.getMessage }", e))
    }
  }
}
object AddBagToDeposit {
  // for examples see https://github.com/LibraryOfCongress/bagit-java/issues/18
  //              and http://www.mpcdf.mpg.de/services/data/annotate/downloads -> TacoHarvest
  def createBag(datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = Try {
    val inputDir = multiDepositDir(datasetID)
    val inputDirExists = inputDir.exists
    val outputBagDir = outputDepositBagDir(datasetID)

    val bagFactory = new BagFactory
    val preBag = bagFactory.createPreBag(outputBagDir)
    val bag = bagFactory.createBag(outputBagDir)

    if (inputDirExists) bag.addFilesToPayload(inputDir.listFiles.toList.asJava)

    val fsw = new FileSystemWriter(bagFactory)
    if (!inputDirExists) fsw.setTagFilesOnly(true)
    fsw.write(bag, outputBagDir)

    val algorithm = Algorithm.SHA1
    val defaultCompleter = {
      val dc = new DefaultCompleter(bagFactory)
      dc.setCompleteTagManifests(false)
      dc.setPayloadManifestAlgorithm(algorithm)
      dc
    }
    val tagManifestCompleter = {
      val tm = new TagManifestCompleter(bagFactory)
      tm.setTagManifestAlgorithm(algorithm)
      tm
    }
    val completer = new ChainingCompleter(
      defaultCompleter,
      new BagInfoCompleter(bagFactory, dataset),
      tagManifestCompleter
    )

    if (!inputDirExists) preBag.setIgnoreAdditionalDirectories(List(metadataDirName).asJava)
    preBag.makeBagInPlace(Version.V0_97, false, completer)

    // TODO, this is temporary, waiting for response from the BagIt-Java developers.
    if (!inputDirExists) {
      new File(outputBagDir, "data").mkdir()
      new File(outputBagDir, "manifest-sha1.txt").write("")
      new File(outputBagDir, "tagmanifest-sha1.txt").append(s"${ MessageDigestHelper.generateFixity(new FileInputStream(new File(outputBagDir, "manifest-sha1.txt")), Algorithm.SHA1) }  manifest-sha1.txt")
    }
  }
}

class BagInfoCompleter(bagFactory: BagFactory, dataset: Dataset) extends Completer {

  def complete(bag: Bag): Bag = {
    val newBag = bagFactory.createBag(bag)

    // copy files from bag to newBag
    newBag.putBagFiles(bag.getPayload)
    newBag.putBagFiles(bag.getTags)

    // create a BagInfoTxt based on the old one
    val bagPartFactory = bagFactory.getBagPartFactory
    val bagInfo = bagPartFactory.createBagInfoTxt(bag.getBagInfoTxt)

    // add the CREATED field
    dataset.get("DDM_CREATED")
      .flatMap(_.find(s => !s.isBlank))
      .map(DateTime.parse) // parse should never fail, since it is already validated in AddDatasetMetadataToDeposit
      .map(_.toString(ISODateTimeFormat.dateTime()))
      .foreach(bagInfo.put("Created", _))

    // add the new BagInfoTxt to the newBag
    newBag.putBagFile(bagInfo)

    newBag
  }
}
