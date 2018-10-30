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
package nl.knaw.dans.easy.multideposit.parser

import better.files.File
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

trait FileMetadataParser extends DebugEnhancedLogging {

  def extractFileMetadata(depositDir: File, instructions: Instructions): Try[Seq[FileMetadata]] = {
    logger.info(s"extract metadata from files in $depositDir")
    if (depositDir.exists)
      depositDir.listRecursively
        .collect { case file if !file.isDirectory => getFileMetadata(instructions)(file) }
        .toList
        .collectResults
    else // if the deposit does not contain any data
      Success { List.empty }
  }

  private def getFileMetadata(instructions: Instructions)(file: File): Try[FileMetadata] = {
    MimeType.get(file).flatMap {
      case mimetype if mimetype startsWith "audio" => mkAvFileMetadata(file, mimetype, Audio, instructions)
      case mimetype if mimetype startsWith "video" => mkAvFileMetadata(file, mimetype, Video, instructions)
      case mimetype => Success(mkDefaultFileMetadata(file, mimetype, instructions))
    }
  }

  private def mkDefaultFileMetadata(file: File, m: MimeType,
                                    instructions: Instructions): FileMetadata = {
    instructions.files.get(file.path)
      .map(fd => DefaultFileMetadata(file.path, m, fd.title, fd.accessibility, fd.visibility))
      .getOrElse(DefaultFileMetadata(file.path, m))
  }

  private def mkAvFileMetadata(file: File, m: MimeType, vocabulary: AvVocabulary,
                               instructions: Instructions): Try[FileMetadata] = {
    instructions.audioVideo.springfield
      .map(mkAvFileWithSpringfield(file, m, vocabulary, _, instructions))
      .getOrElse(Success(mkAvFileWithoutSpringfield(file, m, vocabulary, instructions)))
  }

  private def mkAvFileWithSpringfield(file: File, m: MimeType, vocabulary: AvVocabulary,
                                      springfield: Springfield,
                                      instructions: Instructions): Try[FileMetadata] = {
    val subtitles = instructions.audioVideo.avFiles.getOrElse(file.path, Set.empty)
    lazy val defaultAccess = defaultAccessibility(instructions)
    lazy val defaultVisibility = FileAccessRights.ANONYMOUS
    lazy val filename = file.name.toString

    instructions.files.get(file.path)
      .map(fd => {
        val accessibility = fd.accessibility.getOrElse(defaultAccess)
        val visibility = fd.visibility.getOrElse(defaultVisibility)

        springfield.playMode match {
          case PlayMode.Menu =>
            fd.title
              .map(title => Success(AVFileMetadata(file.path, m, vocabulary, title, accessibility,
                visibility, subtitles)))
              .getOrElse(Failure(ParseException(instructions.row, s"No FILE_TITLE given for A/V file $file.")))
          case PlayMode.Continuous =>
            Success(AVFileMetadata(file.path, m, vocabulary, fd.title.getOrElse(filename),
              accessibility, visibility, subtitles))
        }
      })
      .getOrElse {
        springfield.playMode match {
          case PlayMode.Menu =>
            Failure(ParseException(instructions.row, s"Not listed A/V file detected: $file. " +
              "Because Springfield PlayMode 'MENU' was choosen, all A/V files must be listed " +
              "with a human readable title in the FILE_TITLE field."))
          case PlayMode.Continuous =>
            Success(AVFileMetadata(file.path, m, vocabulary, filename, defaultAccess,
              defaultVisibility, subtitles))
        }
      }
  }

  // no springfield configuratie, but av files; this will result in a failure later on
  private def mkAvFileWithoutSpringfield(file: File, m: MimeType, vocabulary: AvVocabulary,
                                         instructions: Instructions): FileMetadata = {
    val subtitles = instructions.audioVideo.avFiles.getOrElse(file.path, Set.empty)
    lazy val defaultAccess = defaultAccessibility(instructions)
    lazy val defaultVisibility = FileAccessRights.ANONYMOUS
    lazy val filename = file.name.toString

    instructions.files.get(file.path)
      .map(fd => {
        val title = fd.title.getOrElse(filename)
        val accessibility = fd.accessibility.getOrElse(defaultAccess)
        val visibility = fd.visibility.getOrElse(defaultVisibility)
        AVFileMetadata(file.path, m, vocabulary, title, accessibility, visibility, subtitles)
      })
      .getOrElse {
        AVFileMetadata(file.path, m, vocabulary, filename, defaultAccess, defaultVisibility, subtitles)
      }
  }

  private def defaultAccessibility(instructions: Instructions): FileAccessRights.Value = {
    FileAccessRights.accessibleTo(instructions.profile.accessright)
  }
}
