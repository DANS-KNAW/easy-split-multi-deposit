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

import java.nio.file.{ Files, Path }
import java.util
import java.util.Locale

import gov.loc.repository.bagit.creator.BagCreator
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import gov.loc.repository.bagit.verify.FileCountAndTotalSizeVistor
import gov.loc.repository.bagit.writer.BagWriter
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.model.Deposit
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{ Failure, Try }

case class AddBagToDeposit(deposit: Deposit)(implicit settings: Settings) extends UnitAction[Unit] {

  override def checkPreconditions: Try[Unit] = Try {
    Locale.setDefault(Locale.US)
  }

  override def execute(): Try[Unit] = {
    createBag(deposit).recoverWith {
      case NonFatal(e) => Failure(ActionException(deposit.row, s"Error occured in creating the bag for ${ deposit.depositId }: ${ e.getMessage }", e))
    }
  }

  private def createBag(deposit: Deposit)(implicit settings: Settings): Try[Unit] = Try {
    val depositId = deposit.depositId
    val inputDir = multiDepositDir(depositId)
    val stageDir = stagingBagDir(depositId)

    if (inputDir.exists)
      inputDir.copyDir(stageDir)

    val bag = BagCreator.bagInPlace(stageDir.toPath, util.Arrays.asList(StandardSupportedAlgorithms.SHA1), false)

    val bagSize = {
      val payloadSize = calculateSizeOfPath(stagingBagDataDir(depositId).toPath)
      val tagManifestSize = Files.size(stageDir.toPath.resolve("tagmanifest-sha1.txt"))

      payloadSize + tagManifestSize
    }

    bag.getMetadata.add("Bag-Size", formatSize(bagSize))
    bag.getMetadata.add("Bagging-Date", DateTime.now().toString(ISODateTimeFormat.dateTime()))
    bag.getMetadata.add("Created", deposit.profile.created.toString(ISODateTimeFormat.dateTime()))

    for (manifest <- bag.getTagManifests.asScala)
      manifest.getFileToChecksumMap.put(stageDir.toPath.resolve("bag-info.txt"), "0")

    BagWriter.write(bag, stageDir.toPath)
  }

  private def calculateSizeOfPath(dir: Path): Long = {
    val visitor = new FileCountAndTotalSizeVistor

    Files.walkFileTree(dir, visitor)

    visitor.getTotalSize
  }

  private val KB: Double = Math.pow(2, 10)
  private val MB: Double = Math.pow(2, 20)
  private val GB: Double = Math.pow(2, 30)
  private val TB: Double = Math.pow(2, 40)

  private def formatSize(octets: Long): String = {
    def approximate(octets: Long): (String, Double) = {
      octets match {
        case o if o < MB => ("KB", KB)
        case o if o < GB => ("MB", MB)
        case o if o < TB => ("GB", GB)
        case _           => ("TB", TB)
      }
    }

    val (unit, div) = approximate(octets)
    val size = octets / div
    val sizeString = f"$size%1.1f"
    val string = if (sizeString endsWith ".0") size.toInt.toString else sizeString

    s"$string $unit"
  }
}
