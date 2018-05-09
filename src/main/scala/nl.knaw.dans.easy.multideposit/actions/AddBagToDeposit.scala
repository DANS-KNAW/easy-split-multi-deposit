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
import java.util.Collections

import gov.loc.repository.bagit.creator.BagCreator
import gov.loc.repository.bagit.domain.{ Metadata => BagitMetadata }
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import gov.loc.repository.bagit.verify.FileCountAndTotalSizeVistor
import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.easy.multideposit.parser.MultiDepositParser.parse
import nl.knaw.dans.easy.multideposit.parser.{ DepositRow, DepositRows, MultiDepositParser, ParserUtils }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.csv.CSVParser
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }

class AddBagToDeposit extends DebugEnhancedLogging {

  def addBagToDeposit(depositId: DepositId, created: DateTime, base:String)(implicit input: InputPathExplorer, stage: StagingPathExplorer): Try[Unit] = {
    logger.debug(s"construct the bag for $depositId with timestamp ${ created.toString(ISODateTimeFormat.dateTime()) }")

    createBag(depositId, created, base) recoverWith {
      case NonFatal(e) => Failure(ActionException(s"Error occured in creating the bag for $depositId", e))
    }
  }

  private def createBag(depositId: DepositId, created: DateTime, base:String)(implicit input: InputPathExplorer, stage: StagingPathExplorer): Try[Unit] = Try {
    val inputDir = input.depositDir(depositId)
    val stageDir = stage.stagingBagDir(depositId)

    val metadata = new BagitMetadata {
      add("Created", created.toString(ISODateTimeFormat.dateTime()))
    }

    metadata.add("Is-Version-Of", base )

    if (Files.exists(inputDir)) {
      inputDir.copyDir(stageDir)
      metadata.add("Bag-Size", formatSize(calculateSizeOfPath(inputDir)))

    }
    else {
      metadata.add("Bag-Size", formatSize(0L))
    }

    BagCreator.bagInPlace(stageDir, Collections.singletonList(StandardSupportedAlgorithms.SHA1), true, metadata)
  }

  private def calculateSizeOfPath(dir: Path): Long = {
    val visitor = new FileCountAndTotalSizeVistor

    Files.walkFileTree(dir, visitor)

    visitor.getTotalSize
  }

  private val kb: Double = Math.pow(2, 10)
  private val mb: Double = Math.pow(2, 20)
  private val gb: Double = Math.pow(2, 30)
  private val tb: Double = Math.pow(2, 40)

  private def formatSize(octets: Long): String = {
    def approximate(octets: Long): (String, Double) = {
      octets match {
        case o if o < mb => ("KB", kb)
        case o if o < gb => ("MB", mb)
        case o if o < tb => ("GB", gb)
        case _ => ("TB", tb)
      }
    }

    val (unit, div) = approximate(octets)
    val size = octets / div
    val sizeString = f"$size%1.1f"
    val string = if (sizeString endsWith ".0") size.toInt.toString
                 else sizeString

    s"$string $unit"
  }
}


