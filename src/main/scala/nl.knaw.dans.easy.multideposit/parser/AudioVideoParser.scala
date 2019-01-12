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

import java.util.Locale

import better.files.File
import cats.data.NonEmptyChain
import cats.data.Validated.{ Invalid, Valid }
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.option._
import cats.syntax.traverse._
import cats.syntax.validated._
import nl.knaw.dans.easy.multideposit.model.PlayMode.PlayMode
import nl.knaw.dans.easy.multideposit.model.{ AudioVideo, DepositId, PlayMode, Springfield, SubtitlesFile }

trait AudioVideoParser {
  this: ParserUtils =>

  def extractAudioVideo(depositId: DepositId, rowNum: Int, rows: DepositRows): Validated[AudioVideo] = {
    (
      extractSpringfieldList(rowNum, rows),
      extractSubtitlesPerFile(depositId, rows),
    ).mapN(AudioVideo)
      .andThen {
        case AudioVideo(None, avFiles) if avFiles.nonEmpty =>
          ParseError(rowNum, "The column 'AV_FILE_PATH' contains values, but the columns [SF_COLLECTION, SF_USER] do not").toInvalid
        case otherwise => otherwise.toValidated
      }
  }

  def extractSpringfieldList(rowNum: => Int, rows: DepositRows): Validated[Option[Springfield]] = {
    extractList(rows)(springfield)
      .fold(_.invalid, springfields => springfields.distinct match {
        case Seq() => none.toValidated
        case Seq(t) => t.some.toValidated
        case ts => ParseError(rowNum, s"At most one row is allowed to contain a value for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]. Found: ${ ts.map(_.toTuple).mkString("[", ", ", "]") }").toInvalid
      })
  }

  def springfield(row: DepositRow): Option[Validated[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")
    val playmode = row.find("SF_PLAY_MODE").map(playMode(row.rowNum))

    lazy val collectionException = ParseError(row.rowNum, "Missing value for: SF_COLLECTION")
    lazy val userException = ParseError(row.rowNum, "Missing value for: SF_USER")
    lazy val playModeException = ParseError(row.rowNum, "Missing value for: SF_PLAY_MODE")

    (domain, user, collection, playmode) match {
      case (maybeD, Some(u), Some(c), Some(pm)) => Some {
        (
          maybeD.map(checkValidChars(_, row.rowNum, "SF_DOMAIN")).sequence,
          checkValidChars(u, row.rowNum, "SF_USER"),
          checkValidChars(c, row.rowNum, "SF_COLLECTION"),
          pm,
        ).mapN(Springfield.maybeWithDomain)
      }
      case (_, Some(_), Some(_), None) => playModeException.toInvalid.some
      case (_, Some(_), None, Some(Valid(_))) => collectionException.toInvalid.some
      case (_, Some(_), None, Some(Invalid(parseError))) => (collectionException +: parseError).invalid.some
      case (_, Some(_), None, None) => NonEmptyChain(collectionException, playModeException).invalid.some
      case (_, None, Some(_), Some(Valid(_))) => userException.toInvalid.some
      case (_, None, Some(_), Some(Invalid(parseError))) => (userException +: parseError).invalid.some
      case (_, None, Some(_), None) => NonEmptyChain(userException, playModeException).invalid.some
      case (_, None, None, Some(Valid(_))) => NonEmptyChain(collectionException, userException).invalid.some
      case (_, None, None, Some(Invalid(parseError))) => (collectionException +: userException +: parseError).invalid.some
      case (_, None, None, None) => None
    }
  }

  def playMode(rowNum: => Int)(pm: String): Validated[PlayMode] = {
    PlayMode.valueOf(pm)
      .toValidNec(ParseError(rowNum, s"Value '$pm' is not a valid play mode"))
  }

  def extractSubtitlesPerFile(depositId: DepositId, rows: DepositRows): Validated[Map[File, Set[SubtitlesFile]]] = {
    extractList(rows)(avFile(depositId))
      .map(_.groupBy { case (file, _) => file }.mapValues(_.collect { case (_, subtitles) => subtitles }.toSet))
  }

  def avFile(depositId: DepositId)(row: DepositRow): Option[Validated[(File, SubtitlesFile)]] = {
    val file = row.find("AV_FILE_PATH").map(findRegularFile(depositId, row.rowNum))
    val subtitle = row.find("AV_SUBTITLES").map(findRegularFile(depositId, row.rowNum))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, subtitle, subtitleLang) match {
      case (Some(Invalid(_)), Some(Invalid(_)), _) => ParseError(row.rowNum, "Both AV_FILE_PATH and AV_SUBTITLES do not represent a valid path").toInvalid.some
      case (Some(Invalid(_)), _, _) => ParseError(row.rowNum, "AV_FILE_PATH does not represent a valid path").toInvalid.some
      case (_, Some(Invalid(_)), _) => ParseError(row.rowNum, "AV_SUBTITLES does not represent a valid path").toInvalid.some
      case (Some(Valid(p)), Some(Valid(sub)), subLang)
        if p.exists &&
          p.isRegularFile &&
          sub.exists &&
          sub.isRegularFile &&
          subLang.forall(isValidISO639_1Language) =>
        (p, SubtitlesFile(sub, subLang)).toValidated.some
      case (Some(Valid(p)), Some(_), _)
        if !p.exists =>
        ParseError(row.rowNum, s"AV_FILE_PATH '$p' does not exist").toInvalid.some
      case (Some(Valid(p)), Some(_), _)
        if !p.isRegularFile =>
        ParseError(row.rowNum, s"AV_FILE_PATH '$p' is not a file").toInvalid.some
      case (Some(_), Some(Valid(sub)), _)
        if !sub.exists =>
        ParseError(row.rowNum, s"AV_SUBTITLES '$sub' does not exist").toInvalid.some
      case (Some(_), Some(Valid(sub)), _)
        if !sub.isRegularFile =>
        ParseError(row.rowNum, s"AV_SUBTITLES '$sub' is not a file").toInvalid.some
      case (Some(_), Some(_), Some(subLang))
        if !isValidISO639_1Language(subLang) =>
        ParseError(row.rowNum, s"AV_SUBTITLES_LANGUAGE '$subLang' doesn't have a valid ISO 639-1 language value").toInvalid.some
      case (Some(_), None, Some(subLang)) =>
        ParseError(row.rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'").toInvalid.some
      case (Some(Valid(p)), None, None)
        if p.exists &&
          p.isRegularFile => none
      case (Some(Valid(p)), None, None)
        if !p.exists =>
        ParseError(row.rowNum, s"AV_FILE_PATH '$p' does not exist").toInvalid.some
      case (Some(Valid(p)), None, None)
        if !p.isRegularFile =>
        ParseError(row.rowNum, s"AV_FILE_PATH '$p' is not a file").toInvalid.some
      case (None, None, None) => None
      case (None, _, _) =>
        ParseError(row.rowNum, "No value is defined for AV_FILE_PATH, while some of [AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined").toInvalid.some
    }
  }

  def isValidISO639_1Language(lang: String): Boolean = {
    val b0: Boolean = lang.length == 2
    val b1: Boolean = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

    b0 && b1
  }
}
