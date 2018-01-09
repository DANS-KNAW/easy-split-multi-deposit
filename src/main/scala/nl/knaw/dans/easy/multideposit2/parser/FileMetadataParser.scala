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
package nl.knaw.dans.easy.multideposit2.parser

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.model._
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

trait FileMetadataParser extends DebugEnhancedLogging {

  def extractFileMetadata(depositDir: Path, instructions: Instructions): Try[Seq[FileMetadata]] = {
    logger.info(s"extract metadata from files in $depositDir")
    if (Files.exists(depositDir))
      for {
        fms <- depositDir.listRecursively(! Files.isDirectory(_))
          .map(getFileMetadata(instructions))
          .collectResults
        _ <- checkSFColumnsIfDepositContainsAVFiles(instructions, fms)
        _ <- checkEitherVideoOrAudio(fms, instructions.row)
      } yield fms
    else // if the deposit does not contain any data
      Success { List.empty }
  }

  private def getFileMetadata(instructions: Instructions)(file: Path): Try[FileMetadata] = {
    MimeType.getMimeType(file).map {
      case mimetype if mimetype startsWith "audio" => mkAvFileMetadata(file, mimetype, Audio, instructions)
      case mimetype if mimetype startsWith "video" => mkAvFileMetadata(file, mimetype, Video, instructions)
      case mimetype => mkDefaultFileMetadata(file, mimetype, instructions)
    }
  }

  private def mkDefaultFileMetadata(file: Path, m: MimeType, instructions: Instructions): FileMetadata = {
    instructions.files.get(file)
      .map(fd => DefaultFileMetadata(file, m, fd.title, fd.accessibility))
      .getOrElse(DefaultFileMetadata(file, m))
  }

  private def mkAvFileMetadata(file: Path, m: MimeType, vocabulary: AvVocabulary, instructions: Instructions): FileMetadata = {
    val subtitles = instructions.audioVideo.avFiles.getOrElse(file, Set.empty)
    lazy val defaultAccess = defaultAccessibility(instructions)
    lazy val filename = file.getFileName.toString

    instructions.files.get(file)
      .map(fd => {
        val title = fd.title.getOrElse(filename)
        val accessibility = fd.accessibility.getOrElse(defaultAccess)
        AVFileMetadata(file, m, vocabulary, title, accessibility, subtitles)
      })
      .getOrElse(AVFileMetadata(file, m, vocabulary, filename, defaultAccess, subtitles))
  }

  private def defaultAccessibility(instructions: Instructions): FileAccessRights.Value = {
    FileAccessRights.accessibleTo(instructions.profile.accessright)
  }

  private def checkSFColumnsIfDepositContainsAVFiles(instructions: Instructions, fileMetadata: Seq[FileMetadata]): Try[Unit] = {
    val avFiles = fileMetadata.collect { case fmd: AVFileMetadata => fmd.filepath }

    (instructions.audioVideo.springfield.isDefined, avFiles.isEmpty) match {
      case (true, false) | (false, true) => Success(())
      case (true, true) =>
        Failure(ParseException(instructions.row,
          "Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]\n" +
            "cause: these columns should be empty because there are no audio/video files " +
            "found in this deposit"))
      case (false, false) =>
        Failure(ParseException(instructions.row,
          "No values found for these columns: [SF_USER, SF_COLLECTION]\n" +
            "cause: these columns should contain values because audio/video files are " +
            s"found:\n${ avFiles.map(filepath => s" - $filepath").mkString("\n") }"))
    }
  }

  private def checkEitherVideoOrAudio(fileMetadata: Seq[FileMetadata], row: Int): Try[Unit] = {
    fileMetadata.collect { case fmd: AVFileMetadata => fmd.vocabulary }.distinct match {
      case Nil | Seq(_) => Success(())
      case _ => Failure(ParseException(row, "Found both audio and video in this dataset. Only one of them is allowed."))
    }
  }
}
