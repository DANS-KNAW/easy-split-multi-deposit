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
import nl.knaw.dans.easy.multideposit.model.PlayMode.PlayMode
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error._

import scala.util.{ Failure, Success, Try }

trait AudioVideoParser {
  this: ParserUtils =>

  def extractAudioVideo(rows: DepositRows, rowNum: Int, depositId: DepositId): Try[AudioVideo] = {
    Try {
      springf: Option[Springfield] => avFiles: Map[File, Set[SubtitlesFile]] => {
        (springf, avFiles) match {
          case (None, avs) if avs.nonEmpty => Failure(ParseException(rowNum, "The column " +
            "'AV_FILE_PATH' contains values, but the columns [SF_COLLECTION, SF_USER] do not"))
          case (s, avs) => Try { AudioVideo(s, avs) }
        }
      }
    }
      .combine(extractSpringfieldList(rows, rowNum))
      .combine(extractSubtitlesPerFile(rows, depositId))
      .flatten
  }

  def extractSpringfieldList(rows: DepositRows, rowNum: => Int): Try[Option[Springfield]] = {
    for {
      ss <- extractList(rows)(springfield)
      list = ss.map(_.toTuple)
      s <- atMostOne(rowNum, List("SF_DOMAIN", "SF_USER", "SF_COLLECTION", "SF_PLAY_MODE"))(list)
    } yield s.map((Springfield.apply _).tupled)
  }

  def springfield(rowNum: => Int)(row: DepositRow): Option[Try[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")
    val plm = playMode(rowNum)(row)

    def springfieldWithDomain(domain: String, user: String, collection: String, playMode: PlayMode): Try[Springfield] = {
      Try { (Springfield.withDomain _).curried }
        .combine(checkValidChars(domain, rowNum, "SF_DOMAIN"))
        .combine(checkValidChars(user, rowNum, "SF_USER"))
        .combine(checkValidChars(collection, rowNum, "SF_COLLECTION"))
        .map(_ (playMode))
    }

    def springfieldWithoutDomain(user: String, collection: String, playMode: PlayMode): Try[Springfield] = {
      Try { (Springfield.withoutDomain _).curried }
        .combine(checkValidChars(user, rowNum, "SF_USER"))
        .combine(checkValidChars(collection, rowNum, "SF_COLLECTION"))
        .map(_ (playMode))
    }

    lazy val collectionException = ParseException(rowNum, "Missing value for: SF_COLLECTION")
    lazy val userException = ParseException(rowNum, "Missing value for: SF_USER")
    lazy val playModeException = ParseException(rowNum, "Missing value for: SF_PLAY_MODE")

    // @formatter:off
    (domain, user, collection, plm) match {
      case (Some(d), Some(u), Some(c), Some(Success(pm))) => Some(springfieldWithDomain(d, u, c, pm))
      case (None,    Some(u), Some(c), Some(Success(pm))) => Some(springfieldWithoutDomain(u, c, pm))
      case (_,       Some(_), Some(_), Some(Failure(e)) ) => Some(Failure(e))
      case (_,       Some(_), Some(_), None             ) => Some(Failure(playModeException))
      case (_,       Some(_), None,    Some(Success(_)) ) => Some(Failure(collectionException))
      case (_,       Some(_), None,    Some(Failure(e)) ) => Some(Failure(new CompositeException(collectionException, e)))
      case (_,       Some(_), None,    None             ) => Some(Failure(new CompositeException(collectionException, playModeException)))
      case (_,       None,    Some(_), Some(Success(_)) ) => Some(Failure(userException))
      case (_,       None,    Some(_), Some(Failure(e)) ) => Some(Failure(new CompositeException(userException, e)))
      case (_,       None,    Some(_), None             ) => Some(Failure(new CompositeException(userException, playModeException)))
      case (_,       None,    None,    Some(Success(_)) ) => Some(Failure(new CompositeException(collectionException, userException)))
      case (_,       None,    None,    Some(Failure(e)) ) => Some(Failure(new CompositeException(collectionException, userException, e)))
      case (_,       None,    None,    None             ) => None
    }
    // @formatter:on
  }

  def playMode(rowNum: => Int)(row: DepositRow): Option[Try[PlayMode.Value]] = {
    row.find("SF_PLAY_MODE")
      .map(mode => PlayMode.valueOf(mode)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$mode' is not a valid play mode"))))
  }

  def extractSubtitlesPerFile(rows: DepositRows, depositId: DepositId): Try[Map[File, Set[SubtitlesFile]]] = {
    for {
      filesAndSubtitles <- extractList(rows)(avFile(depositId))
      subtitlesPerFile = filesAndSubtitles.groupBy { case (file, _) => file }
    } yield subtitlesPerFile.mapValues(_.collect { case (_, subtitles) => subtitles }.toSet)
  }

  def avFile(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Try[(File, SubtitlesFile)]] = {
    val file = row.find("AV_FILE_PATH").map(findRegularFile(depositId))
    val subtitle = row.find("AV_SUBTITLES").map(findRegularFile(depositId))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, subtitle, subtitleLang) match {
      case (Some(Failure(e1)), Some(Failure(e2)), _) => Some(Failure(ParseException(rowNum, "Both AV_FILE_PATH and AV_SUBTITLES do not represent a valid path", new CompositeException(e1, e2))))
      case (Some(Failure(e)), _, _) => Some(Failure(ParseException(rowNum, "AV_FILE_PATH does not represent a valid path", e)))
      case (_, Some(Failure(e)), _) => Some(Failure(ParseException(rowNum, "AV_SUBTITLES does not represent a valid path", e)))
      case (Some(Success(p)), Some(Success(sub)), subLang)
        if p.exists &&
          p.isRegularFile &&
          sub.exists &&
          sub.isRegularFile &&
          subLang.forall(isValidISO639_1Language) =>
        Some(Success { (p, SubtitlesFile(sub, subLang)) })
      case (Some(Success(p)), Some(_), _)
        if !p.exists =>
        Some(Failure(ParseException(rowNum, s"AV_FILE_PATH '$p' does not exist")))
      case (Some(Success(p)), Some(_), _)
        if !p.isRegularFile =>
        Some(Failure(ParseException(rowNum, s"AV_FILE_PATH '$p' is not a file")))
      case (Some(_), Some(Success(sub)), _)
        if !sub.exists =>
        Some(Failure(ParseException(rowNum, s"AV_SUBTITLES '$sub' does not exist")))
      case (Some(_), Some(Success(sub)), _)
        if !sub.isRegularFile =>
        Some(Failure(ParseException(rowNum, s"AV_SUBTITLES '$sub' is not a file")))
      case (Some(_), Some(_), Some(subLang))
        if !isValidISO639_1Language(subLang) =>
        Some(Failure(ParseException(rowNum, s"AV_SUBTITLES_LANGUAGE '$subLang' doesn't have a valid ISO 639-1 language value")))
      case (Some(_), None, Some(subLang)) =>
        Some(Failure(ParseException(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'")))
      case (Some(Success(p)), None, None)
        if p.exists &&
          p.isRegularFile => None
      case (Some(Success(p)), None, None)
        if !p.exists =>
        Some(Failure(ParseException(rowNum, s"AV_FILE_PATH '$p' does not exist")))
      case (Some(Success(p)), None, None)
        if !p.isRegularFile =>
        Some(Failure(ParseException(rowNum, s"AV_FILE_PATH '$p' is not a file")))
      case (None, None, None) => None
      case (None, _, _) =>
        Some(Failure(ParseException(rowNum, "No value is defined for AV_FILE_PATH, while some of [AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined")))
    }
  }
}
