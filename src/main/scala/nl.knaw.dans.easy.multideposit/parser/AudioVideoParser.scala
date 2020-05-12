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
import nl.knaw.dans.easy.multideposit.model._

trait AudioVideoParser {
  this: ParserUtils =>

  def extractAudioVideo(depositId: DepositId, rowNum: Int, rows: DepositRows): Validated[AudioVideo] = {
    (
      extractSpringfieldList(rowNum, rows),
      extractSubtitlesPerFile(depositId, rows),
    ).mapN(AudioVideo)
      .andThen {
        case AudioVideo(None, avFiles) if avFiles.nonEmpty =>
          ParseError(rowNum, s"The column '${ Headers.AudioVideoFilePath }' contains values, but the columns [${ Headers.SpringfieldCollection }, ${ Headers.SpringfieldUser }] do not").toInvalid
        case otherwise => otherwise.toValidated
      }
  }

  def extractSpringfieldList(rowNum: => Int, rows: DepositRows): Validated[Option[Springfield]] = {
    val records = rows.flatMap(springfield).toList
    val validSpringfields = records.collect { case Valid(a) => a }
    val invalidRecords = records.collect { case Invalid(e) => e }.reduceOption(_ ++ _)

    (validSpringfields, invalidRecords) match {
      case (Nil, None) => none.toValidated
      case (Nil, Some(invalids)) => invalids.invalid
      case (singleSpringfield :: Nil, None) => singleSpringfield.some.toValidated
      case (singleSpringfield :: Nil, Some(_)) => ParseError(rowNum, s"At most one row is allowed to contain a value for these columns: [${ Headers.SpringfieldDomain }, ${ Headers.SpringfieldUser }, ${ Headers.SpringfieldCollection }, ${ Headers.SpringfieldPlayMode }]. Found one complete instance ${ singleSpringfield.toTuple } as well as one or more incomplete instances.").toInvalid
      case (multipleSpringfields @ head :: _, None) if multipleSpringfields.distinct.size == 1 => head.some.toValidated
      case (multipleSpringfields, None) => ParseError(rowNum, s"At most one row is allowed to contain a value for these columns: [${ Headers.SpringfieldDomain }, ${ Headers.SpringfieldUser }, ${ Headers.SpringfieldCollection }, ${ Headers.SpringfieldPlayMode }]. Found: ${ multipleSpringfields.map(_.toTuple).mkString("[", ", ", "]") }").toInvalid
      case (multipleSpringfields, Some(_)) => ParseError(rowNum, s"At most one row is allowed to contain a value for these columns: [${ Headers.SpringfieldDomain }, ${ Headers.SpringfieldUser }, ${ Headers.SpringfieldCollection }, ${ Headers.SpringfieldPlayMode }]. Found: ${ multipleSpringfields.map(_.toTuple).mkString("[", ", ", "]") } as well as one or more incomplete instances.").toInvalid
    }
  }

  def springfield(row: DepositRow): Option[Validated[Springfield]] = {
    val domain = row.find(Headers.SpringfieldDomain)
    val user = row.find(Headers.SpringfieldUser)
    val collection = row.find(Headers.SpringfieldCollection)
    val playmode = row.find(Headers.SpringfieldPlayMode).map(playMode(row.rowNum))

    lazy val collectionException = ParseError(row.rowNum, s"Missing value for: ${ Headers.SpringfieldCollection }")
    lazy val userException = ParseError(row.rowNum, s"Missing value for: ${ Headers.SpringfieldUser }")
    lazy val playModeException = ParseError(row.rowNum, s"Missing value for: ${ Headers.SpringfieldPlayMode }")

    (domain, user, collection, playmode) match {
      case (maybeD, Some(u), Some(c), Some(pm)) =>
        (
          maybeD.traverse(checkValidChars(_, row.rowNum, Headers.SpringfieldDomain)),
          checkValidChars(u, row.rowNum, Headers.SpringfieldUser),
          checkValidChars(c, row.rowNum, Headers.SpringfieldCollection),
          pm,
        ).mapN(Springfield.maybeWithDomain).some
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
    val file = row.find(Headers.AudioVideoFilePath).map(findRegularFile(depositId, row.rowNum))
    val subtitle = row.find(Headers.AudioVideoSubtitles).map(findRegularFile(depositId, row.rowNum))
    val subtitleLang = row.find(Headers.AudioVideoSubtitlesLanguage)

    (file, subtitle, subtitleLang) match {
      case (Some(Invalid(_)), Some(Invalid(_)), _) => ParseError(row.rowNum, s"Both ${ Headers.AudioVideoFilePath } and ${ Headers.AudioVideoSubtitles } do not represent a valid path").toInvalid.some
      case (Some(Invalid(_)), _, _) => ParseError(row.rowNum, s"${ Headers.AudioVideoFilePath } does not represent a valid path").toInvalid.some
      case (_, Some(Invalid(_)), _) => ParseError(row.rowNum, s"${ Headers.AudioVideoSubtitles } does not represent a valid path").toInvalid.some
      case (Some(Valid(p)), Some(Valid(sub)), subLang)
        if p.exists &&
          p.isRegularFile &&
          sub.exists &&
          sub.isRegularFile &&
          subLang.forall(isValidISO639_1Language) =>
        (p, SubtitlesFile(sub, subLang)).toValidated.some
      case (Some(Valid(p)), Some(_), _)
        if !p.exists =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoFilePath } '$p' does not exist").toInvalid.some
      case (Some(Valid(p)), Some(_), _)
        if !p.isRegularFile =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoFilePath } '$p' is not a file").toInvalid.some
      case (Some(_), Some(Valid(sub)), _)
        if !sub.exists =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoSubtitles } '$sub' does not exist").toInvalid.some
      case (Some(_), Some(Valid(sub)), _)
        if !sub.isRegularFile =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoSubtitles } '$sub' is not a file").toInvalid.some
      case (Some(_), Some(_), Some(subLang))
        if !isValidISO639_1Language(subLang) =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoSubtitlesLanguage } '$subLang' doesn't have a valid ISO 639-1 language value").toInvalid.some
      case (Some(_), None, Some(subLang)) =>
        ParseError(row.rowNum, s"Missing value for ${ Headers.AudioVideoSubtitles }, since ${ Headers.AudioVideoSubtitlesLanguage } does have a value: '$subLang'").toInvalid.some
      case (Some(Valid(p)), None, None)
        if p.exists &&
          p.isRegularFile => none
      case (Some(Valid(p)), None, None)
        if !p.exists =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoFilePath } '$p' does not exist").toInvalid.some
      case (Some(Valid(p)), None, None)
        if !p.isRegularFile =>
        ParseError(row.rowNum, s"${ Headers.AudioVideoFilePath } '$p' is not a file").toInvalid.some
      case (None, None, None) => None
      case (None, _, _) =>
        ParseError(row.rowNum, s"No value is defined for ${ Headers.AudioVideoFilePath }, while some of [${ Headers.AudioVideoSubtitles }, ${ Headers.AudioVideoSubtitlesLanguage }] are defined").toInvalid.some
    }
  }

  def isValidISO639_1Language(lang: String): Boolean = {
    val b0: Boolean = lang.length == 2
    val b1: Boolean = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

    b0 && b1
  }
}
