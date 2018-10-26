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
package nl.knaw.dans.easy.multideposit.actions

import better.files.File
import nl.knaw.dans.easy.multideposit.PathExplorer.{ OutputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Deposit, DepositId }
import nl.knaw.dans.easy.multideposit.{ FfprobeRunner, Ldap }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

class ValidatePreconditions(ldap: Ldap, ffprobe: FfprobeRunner) extends DebugEnhancedLogging {

  def validateDeposit(deposit: Deposit)(implicit stage: StagingPathExplorer, output: OutputPathExplorer): Try[Unit] = {
    val id = deposit.depositId
    logger.debug(s"validating deposit $id")
    for {
      _ <- checkDirectoriesDoNotExist(id)(stage.stagingDir(id), stage.stagingBagDir(id), stage.stagingBagMetadataDir(id))
      _ <- checkSpringFieldDepositHasAVformat(deposit)
      _ <- checkSFColumnsIfDepositContainsAVFiles(deposit)
      _ <- checkEitherVideoOrAudio(deposit)
      _ <- checkAudioVideoNotCorrupt(deposit)
      _ <- checkDepositorUserId(deposit)
    } yield ()
  }

  def checkDirectoriesDoNotExist(depositId: DepositId)(directories: File*): Try[Unit] = {
    logger.debug(s"check directories don't exist yet: ${ directories.mkString("[", ", ", "]") }")

    directories.find(_.exists)
      .map(file => Failure(ActionException(s"The deposit for dataset $depositId already exists in $file.")))
      .getOrElse(Success(()))
  }

  def checkSpringFieldDepositHasAVformat(deposit: Deposit): Try[Unit] = {
    logger.debug("check that a Springfield deposit has an A/V format")

    deposit.springfield match {
      case None => Success(())
      case Some(_) => deposit.metadata.formats
        .find(s => s.startsWith("audio/") || s.startsWith("video/"))
        .map(_ => Success(()))
        .getOrElse(Failure(InvalidInputException(deposit.row,
          "No audio/video format found for this column: [DC_FORMAT]\n" +
            "cause: this column should contain at least one " +
            "audio/ or video/ value because SF columns are present")))
    }
  }

  def checkSFColumnsIfDepositContainsAVFiles(deposit: Deposit): Try[Unit] = {
    logger.debug("check that the Springfield parameters are only defined iff there are A/V files defined")

    val avFiles = deposit.files.collect { case fmd: AVFileMetadata => fmd.filepath }
    (deposit.springfield.isDefined, avFiles.isEmpty) match {
      case (true, false) | (false, true) => Success(())
      case (true, true) =>
        Failure(InvalidInputException(deposit.row,
          "Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]; " +
            "these columns should be empty because there are no audio/video files " +
            "found in this deposit"))
      case (false, false) =>
        Failure(InvalidInputException(deposit.row,
          "No values found for these columns: [SF_USER, SF_COLLECTION, SF_PLAY_MODE]; " +
            "these columns should contain values because audio/video files are " +
            s"found:\n${ avFiles.map(filepath => s" - $filepath").mkString("\n") }"))
    }
  }

  def checkEitherVideoOrAudio(deposit: Deposit): Try[Unit] = {
    logger.debug("check that a deposit has only either audio or video material")

    deposit.files.collect { case fmd: AVFileMetadata => fmd.vocabulary }.distinct match {
      case Nil | Seq(_) => Success(())
      case _ => Failure(InvalidInputException(deposit.row, "Found both audio and video in this dataset. Only one of them is allowed."))
    }
  }

  def checkAudioVideoNotCorrupt(deposit: Deposit): Try[Unit] = {
    logger.debug("check that A/V files can be successfully probed by ffprobe")

    deposit.files.collect { case fmd: AVFileMetadata => fmd.filepath }
      .map(ffprobe.run)
      .collectResults
      .recoverWith {
        case CompositeException(errors) =>
          Failure(InvalidInputException(deposit.row, s"Possibly found corrupt A/V files. Ffprobe failed when probing the following files:\n${
            errors.map { case FfprobeErrorException(t, e, _) => s" - File: $t, exit code: $e" }.mkString("\n")
          }"))
      }.map(_ => ())
  }

  def checkDepositorUserId(deposit: Deposit): Try[Unit] = {
    logger.debug("check that the depositor is an active user")

    val depositorUserId = deposit.depositorUserId
    ldap.query(depositorUserId)(attrs => Option(attrs.get("dansState")).exists(_.get().toString == "ACTIVE"))
      .flatMap {
        case Seq() => Failure(InvalidInputException(deposit.row, s"depositorUserId '$depositorUserId' is unknown"))
        case Seq(head) => Success(head)
        case _ => Failure(ActionException(s"There appear to be multiple users with id '$depositorUserId'"))
      }
      .flatMap {
        case true => Success(())
        case false => Failure(InvalidInputException(deposit.row, s"The depositor '$depositorUserId' is not an active user"))
      }
  }
}
