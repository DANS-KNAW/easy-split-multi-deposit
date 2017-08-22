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

import nl.knaw.dans.easy.multideposit.actions._
import nl.knaw.dans.easy.multideposit.model.Deposit
import nl.knaw.dans.easy.multideposit.parser.MultiDepositParser
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.language.postfixOps
import scala.util.{ Failure, Try }

object Main extends DebugEnhancedLogging {

  def main(args: Array[String]): Unit = {
    debug("Starting application")
    implicit val settings: Settings = CommandLineOptions.parse(args)

    run
      .doIfFailure { case e =>
        logger.error(e.getMessage)
        logger.debug(e.getMessage, e)
      }
      .doIfSuccess(_ => logger.info(s"Finished successfully! The output can be found in ${ settings.outputDepositDir }."))

    logger.debug("closing ldap")
    settings.ldap.close()
  }

  def run(implicit settings: Settings): Try[Unit] = {
    for {
      deposits <- MultiDepositParser().parse(multiDepositInstructionsFile)
      _ <- getActions(deposits).map(_.run(())).getOrElse(Failure(new Exception("no actions were defined")))
    } yield ()
  }

  def getActions(deposits: Seq[Deposit])(implicit settings: Settings): Option[Action[Unit, Unit]] = {
    logger.info("Compiling list of actions to perform ...")

    val retrieveDatamanagerAction = RetrieveDatamanagerAction()
    val depositActions = deposits.map(deposit => {
      val depositId = deposit.depositId
      val row = deposit.row

      logger.debug(s"Getting actions for deposit $depositId ...")

      CreateDirectories(stagingDir(depositId), stagingBagDir(depositId))(row, depositId)
        .combine(AddBagToDeposit(deposit))
        .combine(CreateDirectories(stagingBagMetadataDir(depositId))(row, depositId))
        .combine(AddDatasetMetadataToDeposit(deposit))
        .combine(AddFileMetadataToDeposit(deposit))
        .combine(retrieveDatamanagerAction)
        .combine(AddPropertiesToDeposit(deposit))
        .combine(SetDepositPermissions(row, depositId))
        .withLogMessages(s"Checking preconditions for $depositId", s"Executing $depositId", s"Rolling back $depositId")
    }).reduceOption(_ combine _)
    val moveActions = deposits.map(deposit => MoveDepositToOutputDir(deposit.row, deposit.depositId): Action[Unit, Unit]).reduceOption(_ combine _)

    for {
      dsAct <- depositActions
      mvAct <- moveActions
    } yield dsAct combine mvAct
  }
}
