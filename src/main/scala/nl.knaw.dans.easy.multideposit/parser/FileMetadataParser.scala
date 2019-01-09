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
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.model.FileAccessRights.FileAccessRights
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Audio, AvVocabulary, DefaultFileMetadata, FileAccessRights, FileMetadata, Instructions, MimeType, PlayMode, Springfield, Video }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

trait FileMetadataParser extends DebugEnhancedLogging {

  def extractFileMetadata(depositDir: File, instructions: Instructions): Validated[List[FileMetadata]] = {
    logger.info(s"extract metadata from files in $depositDir")

    if (depositDir.exists)
      depositDir.listRecursively
        .collect { case file if !file.isDirectory => getFileMetadata(instructions)(file).toValidated }
        .toList
        .sequence[Validated, FileMetadata]
    else // if the deposit does not contain any data
      List.empty[FileMetadata].toValidated
  }

  private def getFileMetadata(instructions: Instructions)(file: File): FailFast[FileMetadata] = {
    MimeType.get(file)
      .flatMap {
        case mimetype if mimetype startsWith "audio" => mkAvFileMetadata(file, mimetype, Audio, instructions)
        case mimetype if mimetype startsWith "video" => mkAvFileMetadata(file, mimetype, Video, instructions)
        case mimetype => mkDefaultFileMetadata(file, mimetype, instructions).asRight
      }
  }

  private def mkDefaultFileMetadata(file: File, m: MimeType, instructions: Instructions): FileMetadata = {
    instructions.files.get(file.path)
      .map(fd => DefaultFileMetadata(file.path, m, fd.title, fd.accessibility, fd.visibility))
      .getOrElse(DefaultFileMetadata(file.path, m))
  }

  private def mkAvFileMetadata(file: File, m: MimeType, vocabulary: AvVocabulary, instructions: Instructions): FailFast[FileMetadata] = {
    instructions.audioVideo.springfield
      .map(mkAvFileWithSpringfield(file, m, vocabulary, instructions))
      .getOrElse(mkAvFileWithoutSpringfield(file, m, vocabulary, instructions).asRight)
  }

  private def mkAvFileWithSpringfield(file: File, m: MimeType, vocabulary: AvVocabulary, instructions: Instructions)
                                     (springfield: Springfield): FailFast[AVFileMetadata] = {
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
              .map(title => AVFileMetadata(file.path, m, vocabulary, title, accessibility, visibility, subtitles).asRight)
              .getOrElse(ParseError(instructions.row, s"No FILE_TITLE given for A/V file $file.").asLeft)
          case PlayMode.Continuous =>
            AVFileMetadata(file.path, m, vocabulary, fd.title.getOrElse(filename), accessibility, visibility, subtitles).asRight
        }
      })
      .getOrElse {
        springfield.playMode match {
          case PlayMode.Menu =>
            ParseError(instructions.row, s"Not listed A/V file detected: $file. " +
              "Because Springfield PlayMode 'MENU' was choosen, all A/V files must be listed " +
              "with a human readable title in the FILE_TITLE field.").asLeft
          case PlayMode.Continuous =>
            AVFileMetadata(file.path, m, vocabulary, filename, defaultAccess, defaultVisibility, subtitles).asRight
        }
      }
  }

  // no springfield configuratie, but av files; this will result in a failure later on
  private def mkAvFileWithoutSpringfield(file: File, m: MimeType, vocabulary: AvVocabulary, instructions: Instructions): FileMetadata = {
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

  private def defaultAccessibility(instructions: Instructions): FileAccessRights = {
    FileAccessRights.accessibleTo(instructions.profile.accessright)
  }
}
