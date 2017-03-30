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

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.actions.AddFileMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.{ Settings, UnitAction, _ }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import org.apache.tika.Tika

import scala.collection.immutable
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.Elem

case class AddFileMetadataToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends UnitAction[Unit] {

  val (datasetID, dataset) = entry

  case class FileInstruction(file: File, title: Option[String], subtitles: Seq[Subtitle])

  sealed abstract class AudioVideo(val vocabulary: String)
  case object Audio extends AudioVideo("http://schema.org/AudioObject")
  case object Video extends AudioVideo("http://schema.org/VideoObject")

  case class Subtitle(filepath: File, language: Option[String])

  class FileMetadata(val filepath: File, val mimeType: MimeType)
  case class AVFileMetadata(override val filepath: File,
                            override val mimeType: MimeType,
                            vocabulary: AudioVideo,
                            title: String,
                            accessibleTo: FileAccessRights.UserCategory,
                            subtitles: Seq[Subtitle]) extends FileMetadata(filepath, mimeType)

  private lazy val avAccessibility = {
    dataset.findValue("SF_ACCESSIBILITY")
      .flatMap(FileAccessRights.valueOf)
      .getOrElse(FileAccessRights.accessibleTo(AccessCategory.valueOf(dataset.findValue("DDM_ACCESSRIGHTS").get)))
  }

  private lazy val fileInstructions: Map[File, FileInstruction] = {
    def resolve(filepath: String): File = new File(settings.multidepositDir, filepath).getAbsoluteFile

    dataset.getColumns("AV_FILE", "AV_FILE_TITLE", "AV_SUBTITLES", "AV_SUBTITLES_LANGUAGE")
      .toRows
      .map(row => (
        row.get("AV_FILE"),
        row.get("AV_FILE_TITLE"),
        row.get("AV_SUBTITLES"),
        row.get("AV_SUBTITLES_LANGUAGE")
      ))
      .collect { case (file, title, sub, subLang) if file.exists(!_.isBlank) => (file.get, title, sub, subLang) }
      .groupBy { case (file, _, _, _) => file }
      .map { case (file, instr) =>
        val f = resolve(file)
        val optTitle = instr.collectFirst { case (_, Some(title), _, _) => title }
        val subtitles = instr.collect { case (_, _, Some(sub), subLang) => Subtitle(resolve(sub), subLang) }
        f -> FileInstruction(f, optTitle, subtitles)
      }
  }

  private lazy val fileMetadata: Try[Seq[FileMetadata]] = Try {
    val datasetDir = multiDepositDir(datasetID)
    if (datasetDir.exists()) {
      datasetDir.listRecursively
        .map(file => getFileMetadata(file.getAbsoluteFile))
        .collectResults
    }
    else // this means the dataset does not contain any data
      Try { List.empty }
  }.flatten

  private def getFileMetadata(file: File): Try[FileMetadata] = {
    def mkFileMetadata(m: MimeType, voca: AudioVideo) = {
      fileInstructions.get(file) match {
        case Some(FileInstruction(_, optTitle, subtitles)) =>
          val title = optTitle.getOrElse(file.getName)
          AVFileMetadata(file, m, voca, title, avAccessibility, subtitles)
        case None =>
          AVFileMetadata(file, m, voca, file.getName, avAccessibility, Seq.empty)
      }
    }

    getMimeType(file).map {
      case mimeType if mimeType startsWith "audio" => mkFileMetadata(mimeType, Audio)
      case mimeType if mimeType startsWith "video" => mkFileMetadata(mimeType, Video)
      case mimeType => new FileMetadata(file, mimeType)
    }
  }

  /**
   * Verifies whether all preconditions are met for this specific action.
   * All files referenced in the instructions are checked for existence.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    import validators._

    def checkSFColumnsIfDatasetContainsAVFiles(mimetypes: Seq[FileMetadata]): Try[Unit] = {
      val avFiles = mimetypes.filter(fmd => (fmd.mimeType startsWith "video") || (fmd.mimeType startsWith "audio"))

      if (avFiles.nonEmpty)
        checkColumnsAreNonEmpty(row, dataset, "SF_USER", "SF_COLLECTION")
          .recoverWith {
            case ActionException(r, msg, _) => Failure(ActionException(r,
              s"$msg\ncause: these columns should contain values because audio/video files are " +
                s"found:\n${ avFiles.map(fmd => s" - ${fmd.filepath}").mkString("\n") }"))
          }
      else
        checkColumnsAreEmpty(row, dataset, "SF_DOMAIN", "SF_USER", "SF_COLLECTION")
          .recoverWith {
            case ActionException(r, msg, _) => Failure(ActionException(r,
              s"$msg\ncause: these columns should be empty because there are no audio/video " +
                "files found in this dataset"))
          }
    }

    fileMetadata.flatMap(checkSFColumnsIfDatasetContainsAVFiles)
  }

  override def execute(): Try[Unit] = {
    datasetToFileXml
      .map(stagingFileMetadataFile(datasetID).writeXml(_))
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(row, s"Could not write file meta data: $e", e))
      }
  }

  def datasetToFileXml: Try[Elem] = {
    fileMetadata.map(fileXmls(_) match {
      case Nil => <files xmlns:dcterms="http://purl.org/dc/terms/"/>
      case files =>
        // @formatter:off
        <files xmlns:dcterms="http://purl.org/dc/terms/">{files}</files>
        // @formatter:on
    })
  }

  private def fileXmls(fmds: Seq[FileMetadata]): Seq[Elem] = {
    fmds.map {
      case av: AVFileMetadata => avFileXml(av)
      case fmd: FileMetadata => fileXml(fmd)
    }
  }

  private def fileXml(fmd: FileMetadata): Elem = {
    // @formatter:off
    <file filepath={s"data/${formatFilePath(fmd.filepath)}"}>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
    </file>
    // @formatter:on
  }

  private def avFileXml(fmd: AVFileMetadata): Elem = {
    // @formatter:off
    <file filepath={s"data/${formatFilePath(fmd.filepath)}"}>
      <dcterms:type>{fmd.vocabulary.vocabulary}</dcterms:type>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      <dcterms:title>{fmd.title}</dcterms:title>
      <dcterms:accessRights>{fmd.accessibleTo}</dcterms:accessRights>
      {fmd.subtitles.map(subtitleXml)}
    </file>
    // @formatter:on
  }

  private def subtitleXml(subtitle: Subtitle): Elem = {
    val filepath = formatFilePath(subtitle.filepath)

    // @formatter:off
    subtitle.language
      .map(lang => <dcterms:relation xml:lang={lang}>{s"data/$filepath"}</dcterms:relation>)
      .getOrElse(<dcterms:relation>{s"data/$filepath"}</dcterms:relation>)
    // @formatter:on
  }

  private def formatFilePath(file: File): File = {
    multiDepositDir(datasetID)
      .getAbsoluteFile
      .toPath
      .relativize(file.toPath)
      .toFile
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
