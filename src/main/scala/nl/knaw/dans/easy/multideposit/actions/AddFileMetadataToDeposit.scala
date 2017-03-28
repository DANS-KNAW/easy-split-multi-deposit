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
import nl.knaw.dans.easy.multideposit.{ Settings, UnitAction, _ }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import org.apache.tika.Tika

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }
import scala.xml.Elem

case class AddFileMetadataToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends UnitAction[Unit] {

  val (datasetID, dataset) = entry

  // TODO @rvanheest move the mimetype computation to a separate action once the new
  // parameterized version of Action is installed?
  lazy val mimetypeMap: Try[List[(File, MimeType)]] = Try {
    val datasetDir = multiDepositDir(datasetID)
    if (datasetDir.exists()) {
      val files = datasetDir.listRecursively

      files.map(getMimeType)
        .collectResults
        .map(files.zip(_))
    }
    else // this means the dataset does not contain any data
      Try { List.empty }
  }.flatten

  /**
   * Verifies whether all preconditions are met for this specific action.
   * All files referenced in the instructions are checked for existence.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    import validators._

    def checkSFColumnsIfDatasetContainsAVFiles(mimetypes: List[(File, MimeType)]): Try[Unit] = {
      val avFiles = mimetypes.filter {
        case (_, mimetype) => (mimetype startsWith "video") || (mimetype startsWith "audio")
      }

      if (avFiles.nonEmpty)
        checkColumnsAreNonEmpty(row, dataset, "SF_USER", "SF_COLLECTION")
          .recoverWith {
            case ActionException(r, msg, _) => Failure(ActionException(r,
              s"$msg\ncause: these columns should contain values because audio/video files are " +
                s"found:\n${ avFiles.map { case (file, _) => s" - $file" }.mkString("\n") }"))
          }
      else
        checkColumnsAreEmpty(row, dataset, "SF_DOMAIN", "SF_USER", "SF_COLLECTION")
          .recoverWith {
            case ActionException(r, msg, _) => Failure(ActionException(r,
              s"$msg\ncause: these columns should be empty because there are no audio/video " +
                s"files found in this dataset"))
          }
    }

    mimetypeMap.flatMap(checkSFColumnsIfDatasetContainsAVFiles)
  }

  override def execute(): Try[Unit] = {
    datasetToFileXml
      .map(stagingFileMetadataFile(datasetID).writeXml(_))
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(row, s"Could not write file meta data: $e", e))
      }
  }

  private def datasetToFileXml: Try[Elem] = {
    mimetypeMap.map(fileXmls(_) match {
      case Nil => <files xmlns:dcterms="http://purl.org/dc/terms/"/>
      case files =>
        // @formatter:off
        <files xmlns:dcterms="http://purl.org/dc/terms/">{files}</files>
        // @formatter:on
    })
  }

  private def fileXmls(filesAndMimetypes: List[(File, MimeType)]) = {
    filesAndMimetypes.map {
      case (file, mimetype) =>
        val filepath = multiDepositDir(datasetID).toPath.relativize(file.toPath).toFile
        pathXml(filepath, mimetype)
    }
  }

  private def pathXml(filepath: File, mimetype: MimeType): Elem = {
    // @formatter:off
    <file filepath={s"data/$filepath"}>
      <dcterms:format>{mimetype}</dcterms:format>
    </file>
    // @formatter:on
  }
}

object AddFileMetadataToDeposit {
  private val tika = new Tika
  type MimeType = String

  /**
   * Identify the mimeType of a file.
   *
   * @param file the file to identify
   * @return the mimeType of the file if the identification was successful; `Failure` otherwise
   */
  def getMimeType(file: File): Try[MimeType] = Try {
    tika.detect(file)
  }
}
