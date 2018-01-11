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
package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit2.Ldap
import nl.knaw.dans.easy.multideposit2.PathExplorer.{ OutputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.model.{ AVFileMetadata, Deposit, DepositId }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

class ValidatePreconditions(ldap: Ldap) extends DebugEnhancedLogging {

  def validateDeposit(deposit: Deposit)(implicit stage: StagingPathExplorer, output: OutputPathExplorer): Try[Unit] = {
    val id = deposit.depositId
    logger.debug(s"validating deposit $id")
    for {
      _ <- checkDirectoriesDoNotExist(id)(stage.stagingDir(id), stage.stagingBagDir(id), stage.stagingBagMetadataDir(id))
      _ <- checkOutputDirectoryExists(id)
      _ <- checkSpringFieldDepositHasAVformat(deposit)
      _ <- checkSFColumnsIfDepositContainsAVFiles(deposit)
      _ <- checkEitherVideoOrAudio(deposit)
      _ <- checkDepositorUserId(deposit)
    } yield ()
  }

  def checkDirectoriesDoNotExist(depositId: DepositId)(paths: Path*): Try[Unit] = {
    logger.debug(s"check directories don't exist yet: ${ paths.mkString("[", ", ", "]") }")

    paths.find(Files.exists(_))
      .map(file => Failure(ActionException(s"The deposit for dataset $depositId already exists in $file.")))
      .getOrElse(Success(()))
  }

  def checkOutputDirectoryExists(depositId: DepositId)(implicit output: OutputPathExplorer): Try[Unit] = {
    logger.debug("check output directory does exist")

    Try { Files.exists(output.outputDepositDir(depositId)) }
      .flatMap {
        case true => Failure(ActionException(s"The deposit for dataset $depositId already exists in ${ output.outputDepositDir }"))
        case false => Success(())
      }
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
          "Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]\n" +
            "cause: these columns should be empty because there are no audio/video files " +
            "found in this deposit"))
      case (false, false) =>
        Failure(InvalidInputException(deposit.row,
          "No values found for these columns: [SF_USER, SF_COLLECTION]\n" +
            "cause: these columns should contain values because audio/video files are " +
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

  def checkDepositorUserId(deposit: Deposit): Try[Unit] = {
    logger.debug("check that the depositor is an active user")

    val depositorUserId = deposit.depositorUserId
    ldap.ldapQuery(depositorUserId)(attrs => Option(attrs.get("dansState")).exists(_.get().toString == "ACTIVE"))
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
