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

import java.io.File

import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, Settings, _ }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

trait AudioVideoParser {
  this: ParserUtils with DebugEnhancedLogging =>

  implicit val settings: Settings

  def extractAudioVideo(rows: DepositRows, rowNum: Int, depositId: DepositId): Try[AudioVideo] = {
    Try {
      ((springf: Option[Springfield], acc: Option[FileAccessRights.Value], avFiles: Set[AVFile]) => {
        (springf, acc, avFiles) match {
          case (None, _, fs) if fs.nonEmpty => Failure(ParseException(rowNum, "The column " +
            "'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not"))
          case (s, a, fs) => Try { AudioVideo(s, a, fs) }
        }
      }).curried
    }
      .combine(extractList(rows)(springfield)
        .flatMap(ss => atMostOne(rowNum, List("SF_DOMAIN", "SF_USER", "SF_COLLECTION"))(ss.map { case Springfield(d, u, c) => (d, u, c) }).map(_.map(Springfield.tupled))))
      .combine(extractList(rows)(fileAccessRight)
        .flatMap(atMostOne(rowNum, List("SF_ACCESSIBILITY"))))
      .combine(extractList(rows)(avFile(depositId))
        .flatMap(_.groupBy { case (file, _, _) => file }
          .map {
            case (file, (instrPerFile: Seq[(File, Option[String], Option[Subtitles])])) =>
              val fileTitle = instrPerFile.collect { case (_, Some(title), _) => title } match {
                case Seq() => Success(None)
                case Seq(title) => Success(Some(title))
                case Seq(_, _ @ _*) => Failure(ParseException(rowNum, s"The column 'AV_FILE_TITLE' " +
                  s"can only have one value for file '$file'"))
              }
              val subtitles = instrPerFile.collect { case (_, _, Some(instr)) => instr }

              fileTitle.map(AVFile(file, _, subtitles))
          }.collectResults.map(_.toSet)))
      .flatten
  }

  def springfield(rowNum: => Int)(row: DepositRow): Option[Try[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")

    def springfield(domain: String, user: String, collection: String): Try[Springfield] = {
      Try { Springfield.curried }
        .combine(checkValidChars(rowNum, "SF_DOMAIN", domain))
        .combine(checkValidChars(rowNum, "SF_USER", user))
        .combine(checkValidChars(rowNum, "SF_COLLECTION", collection))
    }

    def springfieldWithDefaultDomain(user: String, collection: String): Try[Springfield] = {
      Try { ((user: String, collection: String) => Springfield(user = user, collection = collection)).curried }
        .combine(checkValidChars(rowNum, "SF_USER", user))
        .combine(checkValidChars(rowNum, "SF_COLLECTION", collection))
    }

    (domain, user, collection) match {
      case (Some(d), Some(u), Some(c)) => Some(springfield(d, u, c))
      case (None, Some(u), Some(c)) => Some(springfieldWithDefaultDomain(u, c))
      case (_, Some(_), None) => Some(Failure(ParseException(rowNum, "Missing value for: SF_COLLECTION")))
      case (_, None, Some(_)) => Some(Failure(ParseException(rowNum, "Missing value for: SF_USER")))
      case (_, None, None) => None
    }
  }

  /**
   * Returns the absolute file for the given `path`. If the input is correct, `path` is relative
   * to the deposit it is in.
   *
   * By means of backwards compatibility, the `path` might also be
   * relative to the multideposit. In this case the correct absolute file is returned as well,
   * besides which a warning is logged, notifying the user that `path` should be relative to the
   * deposit instead.
   *
   * If both options do not suffice, the path is just wrapped in a `File`.
   *
   * @param path the path to a file, as provided by the user input
   * @return the absolute path to this file, if it exists
   */
  private def findPath(depositId: DepositId)(path: String): File = {
    lazy val option1 = new File(multiDepositDir(depositId), path)
    lazy val option2 = new File(settings.multidepositDir, path)

    (option1, option2) match {
      case (f1, _) if f1.exists() => f1
      case (_, f2) if f2.exists() =>
        logger.warn(s"path '$path' is not relative to its depositId '$depositId', but rather relative to the multideposit")
        f2
      case (_, _) => new File(path)
    }
  }

  def avFile(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Try[(File, Option[String], Option[Subtitles])]] = {
    val file = row.find("AV_FILE").map(findPath(depositId))
    val title = row.find("AV_FILE_TITLE")
    val subtitle = row.find("AV_SUBTITLES").map(findPath(depositId))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, title, subtitle, subtitleLang) match {
      case (Some(p), t, Some(sub), subLang) if p.exists() && p.isFile && sub.exists() && sub.isFile && subLang.forall(isValidISO639_1Language) => Some(Try { (p, t, Some(Subtitles(sub, subLang))) })
      case (Some(p), _, Some(_), _) if !p.exists() => Some(Failure(ParseException(rowNum, s"AV_FILE '$p' does not exist")))
      case (Some(p), _, Some(_), _) if !p.isFile => Some(Failure(ParseException(rowNum, s"AV_FILE '$p' is not a file")))
      case (Some(_), _, Some(sub), _) if !sub.exists() => Some(Failure(ParseException(rowNum, s"AV_SUBTITLES '$sub' does not exist")))
      case (Some(_), _, Some(sub), _) if !sub.isFile => Some(Failure(ParseException(rowNum, s"AV_SUBTITLES '$sub' is not a file")))
      case (Some(_), _, Some(_), subLang) if subLang.exists(!isValidISO639_1Language(_)) => Some(Failure(ParseException(rowNum, s"AV_SUBTITLES_LANGUAGE '${ subLang.get }' doesn't have a valid ISO 639-1 language value")))
      case (Some(_), _, None, Some(subLang)) => Some(Failure(ParseException(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'")))
      case (Some(p), t, None, None) if p.exists() && p.isFile => Some(Success((p, t, None)))
      case (Some(p), _, None, None) if !p.exists() => Some(Failure(ParseException(rowNum, s"AV_FILE '$p' does not exist")))
      case (Some(p), _, None, None) if !p.isFile => Some(Failure(ParseException(rowNum, s"AV_FILE '$p' is not a file")))
      case (None, None, None, None) => None
      case (None, _, _, _) => Some(Failure(ParseException(rowNum, "No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined")))
    }
  }

  def fileAccessRight(rowNum: => Int)(row: DepositRow): Option[Try[FileAccessRights.Value]] = {
    row.find("SF_ACCESSIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file accessright"))))
  }
}

object AudioVideoParser {
  def apply()(implicit ss: Settings): AudioVideoParser = new AudioVideoParser with ParserUtils with DebugEnhancedLogging {
    override val settings: Settings = ss
  }
}
