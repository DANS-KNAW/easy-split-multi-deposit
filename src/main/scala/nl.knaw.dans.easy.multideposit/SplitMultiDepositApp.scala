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
package nl.knaw.dans.easy.multideposit

import java.util.Locale

import javax.naming.Context
import javax.naming.ldap.InitialLdapContext
import nl.knaw.dans.easy.multideposit.PathExplorer._
import nl.knaw.dans.easy.multideposit.actions.{ RetrieveDatamanager, _ }
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit.parser.MultiDepositParser
import nl.knaw.dans.lib.error.{ CompositeException, TraversableTryExtensions }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class SplitMultiDepositApp(formats: Set[String], ldap: Ldap, permissions: DepositPermissions) extends AutoCloseable with DebugEnhancedLogging {
  private val validator = new ValidatePreconditions(ldap)
  private val datamanager = new RetrieveDatamanager(ldap)
  private val createDirs = new CreateDirectories()
  private val createBag = new AddBagToDeposit()
  private val datasetMetadata = new AddDatasetMetadataToDeposit(formats)
  private val fileMetadata = new AddFileMetadataToDeposit()
  private val depositProperties = new AddPropertiesToDeposit()
  private val setPermissions = new SetDepositPermissions(permissions)
  private val reportDatasets = new ReportDatasets()
  private val moveDeposit = new MoveDepositToOutputDir()

  override def close(): Unit = ldap.close()

  def validate(paths: PathExplorers, datamanagerId: Datamanager): Try[Seq[Deposit]] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(input.multiDepositDir)
      _ <- deposits.map(validator.validateDeposit).collectResults
      _ <- datamanager.getDatamanagerEmailaddress(datamanagerId)
    } yield deposits
  }

  def convert(paths: PathExplorers, datamanagerId: Datamanager): Try[Unit] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    for {
      _ <- Try { Locale.setDefault(Locale.US) }
      deposits <- MultiDepositParser.parse(input.multiDepositDir)
      dataManagerEmailAddress <- datamanager.getDatamanagerEmailaddress(datamanagerId)
      _ <- deposits.mapUntilFailure(convertDeposit(paths, datamanagerId, dataManagerEmailAddress)).recoverWith {
        case NonFatal(e) => deposits.mapUntilFailure(d => createDirs.discardDeposit(d.depositId)) match {
          case Success(_) => Failure(e)
          case Failure(e2) => Failure(ActionException("discarding deposits failed after creating deposits failed", new CompositeException(e, e2)))
        }
      }
      _ = logger.info("deposits were created successfully")
      _ <- reportDatasets.report(deposits)
      _ = logger.info(s"report generated at ${paths.reportFile}")
      _ <- deposits.mapUntilFailure(deposit => moveDeposit.moveDepositsToOutputDir(deposit.depositId, deposit.bagId))
      _ = logger.info(s"deposits were successfully moved to ${ output.outputDepositDir }")
    } yield ()
  }

  private def convertDeposit(paths: PathExplorers, datamanagerId: Datamanager, dataManagerEmailAddress: DatamanagerEmailaddress)(deposit: Deposit): Try[Unit] = {
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
}

object SplitMultiDepositApp {
  def apply(configuration: Configuration): SplitMultiDepositApp = {
    val ldap = {
      val env = new java.util.Hashtable[String, String]
      env.put(Context.PROVIDER_URL, configuration.properties.getString("auth.ldap.url"))
      env.put(Context.SECURITY_AUTHENTICATION, "simple")
      env.put(Context.SECURITY_PRINCIPAL, configuration.properties.getString("auth.ldap.user"))
      env.put(Context.SECURITY_CREDENTIALS, configuration.properties.getString("auth.ldap.password"))
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")

      Ldap(new InitialLdapContext(env, null))
    }
    val permissions = DepositPermissions(
      permissions = configuration.properties.getString("deposit.permissions.access"),
      group = configuration.properties.getString("deposit.permissions.group")
    )

    new SplitMultiDepositApp(configuration.formats, ldap, permissions)
  }
}
