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

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Deposit }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

trait ParserValidation extends DebugEnhancedLogging {

  def validateDeposit(deposit: Deposit): Try[Unit] = {
    logger.debug(s"validating deposit ${ deposit.depositId }")

    for {
      _ <- checkUserLicenseOnlyWithOpenAccess(deposit)
      _ <- checkSpringFieldDepositHasAVformat(deposit)
      _ <- checkSFColumnsIfDepositContainsAVFiles(deposit)
      _ <- checkEitherVideoOrAudio(deposit)
      _ <- checkAllAVFilesHaveSameAccessibility(deposit)
    } yield ()
  }

  def checkUserLicenseOnlyWithOpenAccess(deposit: Deposit): Try[Unit] = {
    val openaccess = AccessCategory.OPEN_ACCESS

    (deposit.profile.accessright, deposit.metadata.userLicense) match {
      case (`openaccess`, Some(_)) => Success(())
      case (`openaccess`, None) => Failure(ParseException(deposit.row, s"When access right '$openaccess' is used, a user license must be specified as well."))
      case (_, Some(_)) => Failure(ParseException(deposit.row, s"When access right '$openaccess' is used, a user license must be specified as well."))
      case (_, None) => Success(())
    }
  }

  def checkSpringFieldDepositHasAVformat(deposit: Deposit): Try[Unit] = {
    logger.debug("check that a Springfield deposit has an A/V format")

    deposit.springfield match {
      case None => Success(())
      case Some(_) => deposit.metadata.formats
        .find(s => s.startsWith("audio/") || s.startsWith("video/"))
        .map(_ => Success(()))
        .getOrElse(Failure(ParseException(deposit.row,
          "No audio/video format found for this column: [DC_FORMAT]\n" +
            "cause: this column should contain at least one " +
            "audio/ or video/ value because SF columns are present")))
    }
  }

  def checkSFColumnsIfDepositContainsAVFiles(deposit: Deposit): Try[Unit] = {
    val avFiles = deposit.files.collect { case fmd: AVFileMetadata => fmd.filepath }

    (deposit.springfield.isDefined, avFiles.isEmpty) match {
      case (true, false) | (false, true) => Success(())
      case (true, true) =>
        Failure(ParseException(deposit.row,
          "Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]; " +
            "these columns should be empty because there are no audio/video files " +
            "found in this deposit"))
      case (false, false) =>
        Failure(ParseException(deposit.row,
          "No values found for these columns: [SF_USER, SF_COLLECTION, SF_PLAY_MODE]; " +
            "these columns should contain values because audio/video files are " +
            s"found:\n${ avFiles.map(filepath => s" - $filepath").mkString("\n") }"))
    }
  }

  def checkEitherVideoOrAudio(deposit: Deposit): Try[Unit] = {
    deposit.files.collect { case fmd: AVFileMetadata => fmd.vocabulary }.distinct match {
      case Nil | Seq(_) => Success(())
      case _ => Failure(ParseException(deposit.row,
        "Found both audio and video in this dataset. Only one of them is allowed."))
    }
  }

  def checkAllAVFilesHaveSameAccessibility(deposit: Deposit): Try[Unit] = {
    deposit.files.collect { case fmd: AVFileMetadata => fmd.accessibleTo }.distinct match {
      case Seq() | Seq(_) => Success(())
      case accs => Failure(ParseException(deposit.row,
        s"Multiple accessibility levels found for A/V files: ${accs.mkString("{", ", ", "}")}"))
    }
  }
}
