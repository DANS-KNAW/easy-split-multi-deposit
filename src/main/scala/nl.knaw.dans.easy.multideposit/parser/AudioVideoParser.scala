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

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit.model.PlayMode.PlayMode
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, Settings }
import nl.knaw.dans.lib.error._

import scala.util.{ Failure, Success, Try }

trait AudioVideoParser {
  this: ParserUtils =>

  implicit val settings: Settings

  def extractAudioVideo(rows: DepositRows, rowNum: Int, depositId: DepositId): Try[AudioVideo] = {
    Try {
      ((springf: Option[Springfield], avFiles: Map[Path, Set[Subtitles]]) => {
        (springf, avFiles) match {
          case (None, avs) if avs.nonEmpty => Failure(ParseException(rowNum, "The column " +
            "'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not"))
          case (s, avs) => Try { AudioVideo(s, avs) }
        }
      }).curried
    }
      .combine(extractList(rows)(springfield)
        .flatMap(ss => atMostOne(rowNum, List("SF_DOMAIN", "SF_USER", "SF_COLLECTION", "SF_PLAY_MODE"))(ss.map { case Springfield(d, u, c, pm) => (d, u, c, pm) }).map(_.map(Springfield.tupled))))
      .combine(extractList(rows)(avFile(depositId))
        .map(_.groupBy { case (file, _) => file }
          .mapValues(_.collect { case (_, subtitles) => subtitles }.toSet)))
      .flatten
  }

  def springfield(rowNum: => Int)(row: DepositRow): Option[Try[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")
    val plm = playMode(rowNum)(row)

    def springfield(domain: Option[String], user: String, collection: String, plm: PlayMode): Try[Springfield] = {
      domain
        .map(d => Try { Springfield.curried }
          .combine(checkValidChars(d, rowNum, "SF_DOMAIN")))
        .getOrElse(Try { ((user: String, collection: String, playMode: PlayMode) => Springfield(user = user, collection = collection, playMode = playMode)).curried })
        .combine(checkValidChars(user, rowNum, "SF_USER"))
        .combine(checkValidChars(collection, rowNum, "SF_COLLECTION"))
        .map(_(plm))
    }

    def springfieldWithoutPlayMode(domain: Option[String], user: String, collection: String): Try[Springfield] = {
      domain
        .map(d => Try { ((domain: String, user: String, collection: String) => Springfield(domain, user, collection)).curried }
          .combine(checkValidChars(d, rowNum, "SF_DOMAIN")))
        .getOrElse(Try { ((user: String, collection: String) => Springfield(user = user, collection = collection)).curried })
        .combine(checkValidChars(user, rowNum, "SF_USER"))
        .combine(checkValidChars(collection, rowNum, "SF_COLLECTION"))
    }

    lazy val collectionException = ParseException(rowNum, "Missing value for: SF_COLLECTION")
    lazy val userException = ParseException(rowNum, "Missing value for: SF_USER")

    (domain, user, collection, plm) match {
      case (d, Some(u), Some(c), Some(Success(pm))) => Some(springfield(d, u, c, pm))
      case (d, Some(u), Some(c), None) => Some(springfieldWithoutPlayMode(d, u, c))
      case (_, Some(_), None, Some(Failure(e))) => Some(Failure(new CompositeException(collectionException, e)))
      case (_, Some(_), None, _) => Some(Failure(collectionException))
      case (_, None, Some(_), Some(Failure(e))) => Some(Failure(new CompositeException(userException, e)))
      case (_, None, Some(_), _) => Some(Failure(userException))
      case (_, _, _, Some(Failure(e))) => Some(Failure(e))
      case (_, None, None, Some(Success(pm))) => Some(Failure(ParseException(rowNum, "Missing values for these columns: [SF_COLLECTION, SF_USER]")))
      case (_, None, None, _) => None
    }
  }

  def playMode(rowNum: => Int)(row: DepositRow): Option[Try[PlayMode.Value]] = {
    row.find("SF_PLAY_MODE")
      .map(mode => PlayMode.valueOf(mode)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$mode' is not a valid play mode"))))
  }

  def avFile(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Try[(Path, Subtitles)]] = {
    val file = row.find("AV_FILE").map(findPath(depositId))
    val subtitle = row.find("AV_SUBTITLES").map(findPath(depositId))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, subtitle, subtitleLang) match {
      case (Some(p), Some(sub), subLang)
        if Files.exists(p) &&
          Files.isRegularFile(p) &&
          Files.exists(sub) &&
          Files.isRegularFile(sub) &&
          subLang.forall(isValidISO639_1Language) =>
        Some(Success { (p, Subtitles(sub, subLang)) })
      case (Some(p), Some(_), _)
        if !Files.exists(p) =>
        Some(Failure(ParseException(rowNum, s"AV_FILE '$p' does not exist")))
      case (Some(p), Some(_), _)
        if !Files.isRegularFile(p) =>
        Some(Failure(ParseException(rowNum, s"AV_FILE '$p' is not a file")))
      case (Some(_), Some(sub), _)
        if !Files.exists(sub) =>
        Some(Failure(ParseException(rowNum, s"AV_SUBTITLES '$sub' does not exist")))
      case (Some(_), Some(sub), _)
        if !Files.isRegularFile(sub) =>
        Some(Failure(ParseException(rowNum, s"AV_SUBTITLES '$sub' is not a file")))
      case (Some(_), Some(_), Some(subLang))
        if !isValidISO639_1Language(subLang) =>
        Some(Failure(ParseException(rowNum, s"AV_SUBTITLES_LANGUAGE '$subLang' doesn't have a valid ISO 639-1 language value")))
      case (Some(_), None, Some(subLang)) =>
        Some(Failure(ParseException(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'")))
      case (Some(p), None, None)
        if Files.exists(p) &&
          Files.isRegularFile(p) => None
      case (Some(p), None, None)
        if !Files.exists(p) =>
        Some(Failure(ParseException(rowNum, s"AV_FILE '$p' does not exist")))
      case (Some(p), None, None)
        if !Files.isRegularFile(p) =>
        Some(Failure(ParseException(rowNum, s"AV_FILE '$p' is not a file")))
      case (None, None, None) => None
      case (None, _, _) =>
        Some(Failure(ParseException(rowNum, "No value is defined for AV_FILE, while some of [AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined")))
    }
  }
}

object AudioVideoParser {
  def apply()(implicit ss: Settings): AudioVideoParser = new AudioVideoParser with ParserUtils {
    override val settings: Settings = ss
  }
}
