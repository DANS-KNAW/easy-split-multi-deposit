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

import nl.knaw.dans.easy.multideposit.{ ParseException, Settings }
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error._

import scala.util.{ Failure, Success, Try }

trait AudioVideoParser {
  this: ParserUtils =>

  val settings: Settings

  def extractAudioVideo(rows: DatasetRows, rowNum: Int): Try[AudioVideo] = {
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
      .combine(extractList(rows)(avFile)
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

  def springfield(rowNum: => Int)(row: DatasetRow): Option[Try[Springfield]] = {
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

  def avFile(rowNum: => Int)(row: DatasetRow): Option[Try[(File, Option[String], Option[Subtitles])]] = {
    val file = row.find("AV_FILE").map(new File(settings.multidepositDir, _))
    val title = row.find("AV_FILE_TITLE")
    val subtitle = row.find("AV_SUBTITLES").map(new File(settings.multidepositDir, _))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, title, subtitle, subtitleLang) match {
      case (Some(p), t, Some(sub), subLang) if p.exists() && sub.exists() && subLang.forall(isValidISO639_1Language) => Some(Try { (p, t, Some(Subtitles(sub, subLang))) })
      case (Some(p), _, Some(_), _) if !p.exists() => Some(Failure(ParseException(rowNum, s"AV_FILE file '$p' does not exist")))
      case (Some(_), _, Some(sub), _) if !sub.exists() => Some(Failure(ParseException(rowNum, s"AV_SUBTITLES file '$sub' does not exist")))
      case (Some(_), _, Some(_), subLang) if subLang.exists(!isValidISO639_1Language(_)) => Some(Failure(ParseException(rowNum, s"AV_SUBTITLES_LANGUAGE '${ subLang.get }' doesn't have a valid ISO 639-1 language value")))
      case (Some(_), _, None, Some(subLang)) => Some(Failure(ParseException(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'")))
      case (Some(p), t, None, None) if p.exists() => Some(Success((p, t, None)))
      case (Some(p), _, None, None) => Some(Failure(ParseException(rowNum, s"AV_FILE file '$p' does not exist")))
      case (None, None, None, None) => None
      case (None, _, _, _) => Some(Failure(ParseException(rowNum, "No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined")))
    }
  }

  def fileAccessRight(rowNum: => Int)(row: DatasetRow): Option[Try[FileAccessRights.Value]] = {
    row.find("SF_ACCESSIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file accessright"))))
  }
}

object AudioVideoParser {
  def apply()(implicit ss: Settings): AudioVideoParser = new AudioVideoParser with ParserUtils {
    override val settings: Settings = ss
  }
}
