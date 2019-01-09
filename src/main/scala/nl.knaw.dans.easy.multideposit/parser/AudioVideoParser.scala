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
      .ensure(ParseError(rowNum, "The column 'AV_FILE_PATH' contains values, but the columns [SF_COLLECTION, SF_USER] do not").chained) {
        case AudioVideo(None, avFiles) => avFiles.isEmpty
        case _ => true
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

  def springfield(rowNum: => Int)(row: DepositRow): Option[Validated[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")
    val playmode = row.find("SF_PLAY_MODE").map(playMode(rowNum))

    lazy val collectionException = ParseError(rowNum, "Missing value for: SF_COLLECTION")
    lazy val userException = ParseError(rowNum, "Missing value for: SF_USER")
    lazy val playModeException = ParseError(rowNum, "Missing value for: SF_PLAY_MODE")

    (domain, user, collection, playmode) match {
      case (maybeD, Some(u), Some(c), Some(pm)) => Some {
        (
          maybeD.map(checkValidChars(_, rowNum, "SF_DOMAIN").toValidated).sequence,
          checkValidChars(u, rowNum, "SF_USER").toValidated,
          checkValidChars(c, rowNum, "SF_COLLECTION").toValidated,
          pm.toValidated,
        ).mapN(Springfield.maybeWithDomain)
      }
      case (_, Some(_), Some(_), None) => Some(playModeException.toInvalid)
      case (_, Some(_), None, Some(Right(_))) => Some(collectionException.toInvalid)
      case (_, Some(_), None, Some(Left(parseError))) => Some(NonEmptyChain(collectionException, parseError).invalid)
      case (_, Some(_), None, None) => Some(NonEmptyChain(collectionException, playModeException).invalid)
      case (_, None, Some(_), Some(Right(_))) => Some(userException.toInvalid)
      case (_, None, Some(_), Some(Left(parseError))) => Some(NonEmptyChain(userException, parseError).invalid)
      case (_, None, Some(_), None) => Some(NonEmptyChain(userException, playModeException).invalid)
      case (_, None, None, Some(Right(_))) => Some(NonEmptyChain(collectionException, userException).invalid)
      case (_, None, None, Some(Left(parseError))) => Some(NonEmptyChain(collectionException, userException, parseError).invalid)
      case (_, None, None, None) => None
    }
  }

  def playMode(rowNum: => Int)(pm: String): FailFast[PlayMode] = {
    PlayMode.valueOf(pm)
      .toRight(ParseError(rowNum, s"Value '$pm' is not a valid play mode"))
  }

  def extractSubtitlesPerFile(depositId: DepositId, rows: DepositRows): Validated[Map[File, Set[SubtitlesFile]]] = {
    extractList(rows)(avFile(depositId))
      .map(_.groupBy { case (file, _) => file }.mapValues(_.collect { case (_, subtitles) => subtitles }.toSet))
  }

  def avFile(depositId: DepositId)(rowNum: => Int)(row: DepositRow): Option[Validated[(File, SubtitlesFile)]] = {
    val file = row.find("AV_FILE_PATH").map(findRegularFile(depositId, rowNum))
    val subtitle = row.find("AV_SUBTITLES").map(findRegularFile(depositId, rowNum))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, subtitle, subtitleLang) match {
      case (Some(Left(_)), Some(Left(_)), _) => ParseError(rowNum, "Both AV_FILE_PATH and AV_SUBTITLES do not represent a valid path").toInvalid.some
      case (Some(Left(_)), _, _) => ParseError(rowNum, "AV_FILE_PATH does not represent a valid path").toInvalid.some
      case (_, Some(Left(_)), _) => ParseError(rowNum, "AV_SUBTITLES does not represent a valid path").toInvalid.some
      case (Some(Right(p)), Some(Right(sub)), subLang)
        if p.exists &&
          p.isRegularFile &&
          sub.exists &&
          sub.isRegularFile &&
          subLang.forall(isValidISO639_1Language) =>
        (p, SubtitlesFile(sub, subLang)).toValidated.some
      case (Some(Right(p)), Some(_), _)
        if !p.exists =>
        ParseError(rowNum, s"AV_FILE_PATH '$p' does not exist").toInvalid.some
      case (Some(Right(p)), Some(_), _)
        if !p.isRegularFile =>
        ParseError(rowNum, s"AV_FILE_PATH '$p' is not a file").toInvalid.some
      case (Some(_), Some(Right(sub)), _)
        if !sub.exists =>
        ParseError(rowNum, s"AV_SUBTITLES '$sub' does not exist").toInvalid.some
      case (Some(_), Some(Right(sub)), _)
        if !sub.isRegularFile =>
        ParseError(rowNum, s"AV_SUBTITLES '$sub' is not a file").toInvalid.some
      case (Some(_), Some(_), Some(subLang))
        if !isValidISO639_1Language(subLang) =>
        ParseError(rowNum, s"AV_SUBTITLES_LANGUAGE '$subLang' doesn't have a valid ISO 639-1 language value").toInvalid.some
      case (Some(_), None, Some(subLang)) =>
        ParseError(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'").toInvalid.some
      case (Some(Right(p)), None, None)
        if p.exists &&
          p.isRegularFile => none
      case (Some(Right(p)), None, None)
        if !p.exists =>
        ParseError(rowNum, s"AV_FILE_PATH '$p' does not exist").toInvalid.some
      case (Some(Right(p)), None, None)
        if !p.isRegularFile =>
        ParseError(rowNum, s"AV_FILE_PATH '$p' is not a file").toInvalid.some
      case (None, None, None) => None
      case (None, _, _) =>
        ParseError(rowNum, "No value is defined for AV_FILE_PATH, while some of [AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined").toInvalid.some
    }
  }

  def isValidISO639_1Language(lang: String): Boolean = {
    val b0: Boolean = lang.length == 2
    val b1: Boolean = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

    b0 && b1
  }
}
