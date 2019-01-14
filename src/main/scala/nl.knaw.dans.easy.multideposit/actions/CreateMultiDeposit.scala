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

import cats.data.{ NonEmptyChain, ValidatedNec }
import cats.instances.either._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, OutputPathExplorer, PathExplorers, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class CreateMultiDeposit(formats: Set[String],
                         ldap: Ldap,
                         ffprobe: FfprobeRunner,
                         permissions: DepositPermissions) extends DebugEnhancedLogging {

  private val validator = new ValidatePreconditions(ldap, ffprobe)
  private val datamanager = new RetrieveDatamanager(ldap)
  private val createDirs = new CreateDirectories()
  private val createBag = new AddBagToDeposit()
  private val datasetMetadata = new AddDatasetMetadataToDeposit(formats)
  private val fileMetadata = new AddFileMetadataToDeposit()
  private val depositProperties = new AddPropertiesToDeposit()
  private val setPermissions = new SetDepositPermissions(permissions)
  private val reportDatasets = new ReportDatasets()
  private val moveDeposit = new MoveDepositToOutputDir()

  private type ConversionFailedValidated[T] = ValidatedNec[ConversionFailed, T]

  def validateDeposits(deposits: List[Deposit])(implicit staging: StagingPathExplorer): ValidatedNec[ConversionFailed, Unit] = {
    deposits.traverse[ConversionFailedValidated, Unit](validateDeposit).map(_ => ())
  }

  private def validateDeposit(deposit: Deposit)(implicit staging: StagingPathExplorer): ValidatedNec[ConversionFailed, Unit] = {
    validator.validateDeposit(deposit).toValidatedNec
  }

  def getDatamanagerEmailaddress(datamanagerId: Datamanager): FailFast[DatamanagerEmailaddress] = {
    datamanager.getDatamanagerEmailaddress(datamanagerId)
  }

  def convertDeposits(deposits: List[Deposit],
                      paths: PathExplorers,
                      datamanagerId: Datamanager,
                      dataManagerEmailAddress: DatamanagerEmailaddress)
                     (implicit staging: StagingPathExplorer): Either[NonEmptyChain[ConversionFailed], Unit] = {
    deposits.traverse[FailFast, Unit](convertDeposit(paths, datamanagerId, dataManagerEmailAddress))
      .leftMap(error => {
        deposits.traverse[FailFast, Unit](d => createDirs.discardDeposit(d.depositId))
          .fold(discardError => NonEmptyChain(
            error,
            discardError
          ), _ => NonEmptyChain.one(error))
      })
      .map(_ => ())
  }

  private def convertDeposit(paths: PathExplorers,
                             datamanagerId: Datamanager,
                             dataManagerEmailAddress: DatamanagerEmailaddress)
                            (deposit: Deposit): FailFast[Unit] = {
    val depositId = deposit.depositId
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    logger.info(s"convert ${ depositId }")

    for {
      _ <- validator.validateDeposit(deposit)
      _ <- createDirs.createDepositDirectories(depositId)
      _ <- createBag.addBagToDeposit(depositId, deposit.profile.created, deposit.baseUUID)
      _ <- createDirs.createMetadataDirectory(depositId)
      _ <- datasetMetadata.addDatasetMetadata(deposit)
      _ <- fileMetadata.addFileMetadata(depositId, deposit.files)
      _ <- depositProperties.addDepositProperties(deposit, datamanagerId, dataManagerEmailAddress)
      _ <- setPermissions.setDepositPermissions(depositId)
      _ = logger.info(s"deposit was created successfully for $depositId with bagId ${ deposit.bagId }")
    } yield ()
  }

  def report(deposits: List[Deposit])(implicit output: OutputPathExplorer): FailFast[Unit] = {
    reportDatasets.report(deposits)
  }

  def moveDepositsToOutputDir(deposits: List[Deposit])(implicit staging: StagingPathExplorer, output: OutputPathExplorer): FailFast[Unit] = {
    deposits.traverse[FailFast, Unit](deposit => moveDeposit.moveDepositsToOutputDir(deposit.depositId, deposit.bagId))
      .map(_ => ())
  }
}
