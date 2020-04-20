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
import cats.data.ValidatedNec
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.traverse._
import javax.naming.directory.Attributes
import nl.knaw.dans.easy.multideposit.FfprobeRunner.FfprobeError
import nl.knaw.dans.easy.multideposit.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Deposit, DepositId, DepositorUserId }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class ValidatePreconditions(ldap: Ldap, ffprobe: FfprobeRunner) extends DebugEnhancedLogging {

  //  TODO refactor to Validated
  def validateDeposit(deposit: Deposit)(implicit stage: StagingPathExplorer): FailFast[Unit] = {
    val id = deposit.depositId
    logger.debug(s"validating deposit $id")
    for {
      _ <- checkDirectoriesDoNotExist(id)(stage.stagingDir(id), stage.stagingBagDir(id), stage.stagingBagMetadataDir(id))
      _ <- checkAudioVideoNotCorrupt(deposit)
      _ <- checkDepositorUserId(deposit)
    } yield ()
  }

  def checkDirectoriesDoNotExist(depositId: DepositId)(directories: File*): FailFast[Unit] = {
    logger.debug(s"check directories don't exist yet: ${ directories.mkString("[", ", ", "]") }")

    directories.find(_.exists)
      .map(file => ActionError(s"The deposit for dataset $depositId already exists in $file.").asLeft)
      .getOrElse(().asRight)
  }

  def checkAudioVideoNotCorrupt(deposit: Deposit): Either[InvalidInput, Unit] = {
    logger.debug("check that A/V files can be successfully probed by ffprobe")

    deposit.files.collect { case fmd: AVFileMetadata => fmd.filepath }
      .toList
      .traverse[ValidatedNec[FfprobeError, ?], Unit](ffprobe.run(_).toValidatedNec)
      .leftMap(errors => {
        val ffProbeErrors = errors.toNonEmptyList.toList
          .map { case FfprobeError(t, e, _) => s" - File: $t, exit code: $e" }
          .mkString("\n")

        InvalidInput(deposit.row, "Possibly found corrupt A/V files. Ffprobe failed when probing the following files:\\n" + ffProbeErrors)
      })
      .map(_ => ())
      .toEither
  }

  def checkDepositorUserId(deposit: Deposit): FailFast[Unit] = {
    logger.debug("check that the depositor is an active user")

    val depositorUserId = deposit.depositorUserId
    ldap.query(depositorUserId)(validateDepositorUserId(deposit.row, depositorUserId))
      .flatMap {
        case Seq() => InvalidInput(deposit.row, s"depositorUserId '$depositorUserId' is unknown").asLeft
        case Seq(head) => head.asRight
        case _ => ActionError(s"There appear to be multiple users with id '$depositorUserId'").asLeft
      }
      .flatMap(identity)
  }

  def validateDepositorUserId(row: Int, depositorUserId: DepositorUserId)(attrs: Attributes): FailFast[Unit] = {
    val existingDepositorId = Option(attrs.get("uid")).exists(_.get().toString == depositorUserId)
    lazy val activeState = Option(attrs.get("dansState")).exists(_.get().toString == "ACTIVE")

    if (existingDepositorId)
      if (activeState) ().asRight
      else InvalidInput(row, s"The depositor '$depositorUserId' is not an active user").asLeft
    else InvalidInput(row, "The depositor does not exist. Please check for spelling mistakes and upper/lowercase letters").asLeft
  }
}
