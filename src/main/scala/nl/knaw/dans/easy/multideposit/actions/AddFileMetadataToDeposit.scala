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

import java.io.File

import nl.knaw.dans.easy.multideposit.actions.AddFileMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.{ Action, Settings, _ }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import org.apache.tika.Tika

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.Elem

case class AddFileMetadataToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends Action[Unit] {

  val (datasetID, dataset) = entry

  /**
   * Verifies whether all preconditions are met for this specific action.
   * All files referenced in the instructions are checked for existence.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    val inputDir = settings.multidepositDir
    // Note that the FILE_SIP paths are not really used in this action
    val nonExistingFileSIPs = dataset.getOrElse("FILE_SIP", List.empty)
      .filterNot(_.isEmpty)
      .filterNot(fp => new File(settings.multidepositDir, fp).exists())

    if (nonExistingFileSIPs.isEmpty) Success(())
    else Failure(ActionException(row, s"""The following SIP files are referenced in the instructions but not found in the deposit input dir "$inputDir" for dataset "$datasetID": ${ nonExistingFileSIPs.mkString("[", ", ", "]") }""".stripMargin))
  }

  override def execute(): Try[Unit] = writeFileMetadataXml(row, datasetID)
}
object AddFileMetadataToDeposit {

  def writeFileMetadataXml(row: Int, datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    datasetToFileXml(datasetID)
      .map(stagingFileMetadataFile(datasetID).writeXml(_))
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(row, s"Could not write file meta data: $e", e))
      }
  }

  def datasetToFileXml(datasetID: DatasetID)(implicit settings: Settings): Try[Elem] = {
    val inputDir = multiDepositDir(datasetID)

    if (inputDir.exists() && inputDir.isDirectory)
      inputDir.listRecursively
        .map(xmlPerPath(inputDir))
        .collectResults
        .map(elems => {
          // @formatter:off
          <files xmlns:dcterms="http://purl.org/dc/terms/">{elems}</files>
          // @formatter:on
        })
    else Try {
      // @formatter:off
      <files xmlns:dcterms="http://purl.org/dc/terms/"/>
      // @formatter:on
    }
  }

  // TODO other fields need to be added here later
  def xmlPerPath(inputDir: File)(file: File): Try[Elem] = {
    getMimeType(file)
      .map(mimetype => {
        // @formatter:off
        <file filepath={s"data/${inputDir.toPath.relativize(file.toPath)}"}>
          <dcterms:format>{mimetype}</dcterms:format>
        </file>
        // @formatter:off
      })
  }

  /**
   * Identify the mimeType of a file.
   *
   * @param file the file to identify
   * @return the mimeType of the file if the identification was successful; `Failure` otherwise
   */
  def getMimeType(file: File): Try[String] = Try {
    MimeTypeIdentifierFactory.tika.detect(file)
  }
}

object MimeTypeIdentifierFactory {
  val tika = new Tika
}
