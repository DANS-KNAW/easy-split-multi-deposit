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
package nl.knaw.dans.easy.multideposit2

import java.nio.file.Path
import java.util.Locale

import nl.knaw.dans.easy.multideposit2.PathExplorer.PathExplorers
import nl.knaw.dans.easy.multideposit2.actions.{ RetrieveDatamanager, _ }
import nl.knaw.dans.easy.multideposit2.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit2.parser.MultiDepositParser
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

trait SplitMultiDepositApp extends DebugEnhancedLogging {
  this: PathExplorers
    with ValidatePreconditions
    with RetrieveDatamanager
    with CreateDirectories
    with AddBagToDeposit
    with AddDatasetMetadataToDeposit
    with AddFileMetadataToDeposit
    with AddPropertiesToDeposit =>

  val datamanagerId: Datamanager

  def validate(): Try[Seq[Deposit]] = {
    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(multiDepositDir)
      _ <- deposits.map(validateDeposit).collectResults
      _ <- getDatamanagerEmailaddress(datamanagerId)
    } yield deposits
  }

  def convert(): Try[Unit] = {
    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(multiDepositDir)
      dataManagerEmailAddress <- getDatamanagerEmailaddress(datamanagerId)
      _ <- deposits.mapUntilFailure(convert(dataManagerEmailAddress))
    } yield ()
  }

  private def convert(dataManagerEmailAddress: DatamanagerEmailaddress)(deposit: Deposit): Try[Unit] = {
    logger.info(s"convert ${ deposit.depositId }")
    for {
      _ <- validateDeposit(deposit)
      _ <- createDepositDirectories(deposit.depositId)
      _ <- addBagToDeposit(deposit.depositId, deposit.profile.created)
      _ <- createMetadataDirectory(deposit.depositId)
      _ <- addDatasetMetadata(deposit)
      _ <- addFileMetadata(deposit.depositId, deposit.files)
      _ <- addDepositProperties(deposit, datamanagerId, dataManagerEmailAddress)
    } yield ()
  }
}

object SplitMultiDepositApp {
  def apply(smd: Path, sd: Path, od: Path, fs: Set[String], dmId: Datamanager, ldapService: Ldap): SplitMultiDepositApp = {
    new SplitMultiDepositApp with PathExplorers
      with ValidatePreconditions with RetrieveDatamanager with CreateDirectories
      with AddBagToDeposit with AddDatasetMetadataToDeposit with AddFileMetadataToDeposit
      with AddPropertiesToDeposit {

      override val multiDepositDir: Path = smd
      override val stagingDir: Path = sd
      override val outputDepositDir: Path = od
      override val formats: Set[String] = fs
      override val datamanagerId: Datamanager = dmId
      override val ldap: Ldap = ldapService
    }
  }
}
