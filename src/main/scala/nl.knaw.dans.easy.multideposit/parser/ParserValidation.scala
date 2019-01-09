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

import cats.syntax.apply._
import cats.syntax.either._
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Deposit }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

trait ParserValidation extends DebugEnhancedLogging {

  def validateDeposit(deposit: Deposit): Validated[Unit] = {
    (
      checkUserLicenseOnlyWithOpenAccess(deposit).toValidated,
      checkSpringFieldDepositHasAVformat(deposit).toValidated,
      checkSFColumnsIfDepositContainsAVFiles(deposit).toValidated,
      checkEitherVideoOrAudio(deposit).toValidated,
    ).tupled.map(_ => ())
  }

  def checkUserLicenseOnlyWithOpenAccess(deposit: Deposit): FailFast[Unit] = {
    val openaccess = AccessCategory.OPEN_ACCESS

    (deposit.profile.accessright, deposit.metadata.userLicense) match {
      case (`openaccess`, Some(_)) => ().asRight
      case (`openaccess`, None) => ParseError(deposit.row, s"When access right '$openaccess' is used, a user license must be specified as well.").asLeft
      case (_, Some(_)) => ParseError(deposit.row, s"When access right '$openaccess' is used, a user license must be specified as well.").asLeft
      case (_, None) => ().asRight
    }
  }

  def checkSpringFieldDepositHasAVformat(deposit: Deposit): FailFast[Unit] = {
    logger.debug("check that a Springfield deposit has an A/V format")

    deposit.springfield match {
      case None => ().asRight
      case Some(_) => deposit.metadata.formats
        .find(s => s.startsWith("audio/") || s.startsWith("video/"))
        .map(_ => ().asRight)
        .getOrElse(ParseError(deposit.row,
          "No audio/video format found for this column: [DC_FORMAT]\n" +
            "cause: this column should contain at least one " +
            "audio/ or video/ value because SF columns are present").asLeft)
    }
  }

  def checkSFColumnsIfDepositContainsAVFiles(deposit: Deposit): FailFast[Unit] = {
    val avFiles = deposit.files.collect { case fmd: AVFileMetadata => fmd.filepath }

    (deposit.springfield.isDefined, avFiles.isEmpty) match {
      case (true, false) | (false, true) => ().asRight
      case (true, true) =>
        ParseError(deposit.row,
          "Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]; " +
            "these columns should be empty because there are no audio/video files " +
            "found in this deposit").asLeft
      case (false, false) =>
        ParseError(deposit.row,
          "No values found for these columns: [SF_USER, SF_COLLECTION, SF_PLAY_MODE]; " +
            "these columns should contain values because audio/video files are " +
            s"found:\n${ avFiles.map(filepath => s" - $filepath").mkString("\n") }").asLeft
    }
  }

  def checkEitherVideoOrAudio(deposit: Deposit): FailFast[Unit] = {
    deposit.files.collect { case fmd: AVFileMetadata => fmd.vocabulary }.distinct match {
      case Seq() | Seq(_) => ().asRight
      case _ => ParseError(deposit.row, "Found both audio and video in this dataset. Only one of them is allowed.").asLeft
    }
  }
}
