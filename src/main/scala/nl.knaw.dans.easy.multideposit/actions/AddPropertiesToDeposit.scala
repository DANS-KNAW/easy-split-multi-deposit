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

import java.{ util => ju }
import java.util.{ Collections, Properties, UUID }

import nl.knaw.dans.easy.multideposit.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit.{ encoding, dateTimeFormatter }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.{ DateTime, DateTimeZone }
import resource.Using

import scala.util.{ Failure, Try }
import scala.util.control.NonFatal

class AddPropertiesToDeposit extends DebugEnhancedLogging {

  def addDepositProperties(deposit: Deposit, datamanagerId: Datamanager, emailaddress: DatamanagerEmailaddress)(implicit stage: StagingPathExplorer): Try[Unit] = {
    logger.debug(s"add deposit properties for ${ deposit.depositId }")

    val props = new Properties {
      // Make sure we get sorted output, which is better readable than random
      override def keys(): ju.Enumeration[AnyRef] = Collections.enumeration(new ju.TreeSet[Object](super.keySet()))
    }

    Try { addProperties(deposit, datamanagerId, emailaddress)(props) }
      .flatMap(_ => Using.fileWriter(encoding)(stage.stagingPropertiesFile(deposit.depositId).toFile)
        .map(out => props.store(out, ""))
        .tried)
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(s"Could not write properties to file: $e", e))
      }
  }

  private def addProperties(deposit: Deposit, datamanagerId: Datamanager, emailaddress: DatamanagerEmailaddress)(properties: Properties): Unit = {
    val sf = deposit.springfield
    val props: Map[String, Option[String]] = Map(
      "bag-store.bag-id" -> Some(UUID.randomUUID().toString),
      "creation.timestamp" -> Some(DateTime.now(DateTimeZone.UTC).toString(dateTimeFormatter)),
      "state.label" -> Some("SUBMITTED"),
      "state.description" -> Some("Deposit is valid and ready for post-submission processing"),
      "depositor.userId" -> Some(deposit.depositorUserId),
      "datamanager.userId" -> Some(datamanagerId),
      "datamanager.email" -> Some(emailaddress),
      "springfield.domain" -> sf.map(_.domain),
      "springfield.user" -> sf.map(_.user),
      "springfield.collection" -> sf.map(_.collection),
      "springfield.playmode" -> sf.map(_.playMode.toString)
    )

    for ((key, value) <- props.collect { case (k, Some(v)) => (k, v) }) {
      properties.setProperty(key, value)
    }
  }
}
