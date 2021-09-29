/*
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

import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress, Deposit }
import nl.knaw.dans.easy.multideposit.{ ActionError, FailFast, now }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration

class AddPropertiesToDeposit(dansDoiPrefix: String) extends DebugEnhancedLogging {

  def addDepositProperties(deposit: Deposit, datamanagerId: Datamanager, emailaddress: DatamanagerEmailaddress)(implicit stage: StagingPathExplorer): FailFast[Unit] = {
    logger.debug(s"add deposit properties for ${ deposit.depositId }")

    val props = new PropertiesConfiguration {
      setDelimiterParsingDisabled(false)
      setFile(stage.stagingPropertiesFile(deposit.depositId)
        .createIfNotExists(createParents = true)
        .toJava)
    }

    Either.catchNonFatal { addProperties(deposit, datamanagerId, emailaddress, stage.bagDirName)(props) }
      .map(_ => props.save())
      .leftMap(e => ActionError(s"Could not write properties to file: $e", e))
  }

  private def addProperties(deposit: Deposit, datamanagerId: Datamanager, emailaddress: DatamanagerEmailaddress, bagDirName: String)(properties: PropertiesConfiguration): Unit = {
    val doiAction = if (deposit.baseUUID.isDefined && hasDansDoiPrefix(deposit)) "update" else "create"
    val sf = deposit.springfield
    val props: Map[String, Option[String]] = Map(
      "bag-store.bag-id" -> Some(deposit.bagId.toString),
      "creation.timestamp" -> Some(now),
      "state.label" -> Some("SUBMITTED"),
      "state.description" -> Some("Deposit is valid and ready for post-submission processing"),
      "depositor.userId" -> Some(deposit.depositorUserId),
      "curation.datamanager.userId" -> Some(datamanagerId),
      "curation.datamanager.email" -> Some(emailaddress),
      "curation.required" -> Some("yes"),
      "curation.performed" -> Some("yes"),
      "springfield.domain" -> sf.map(_.domain),
      "springfield.user" -> sf.map(_.user),
      "springfield.collection" -> sf.map(_.collection),
      "springfield.playmode" -> sf.map(_.playMode.toString),
      "identifier.dans-doi.registered" -> Some("no"),
      "identifier.dans-doi.action" -> Some(doiAction),
      "bag-store.bag-name" -> Some(bagDirName),
      "deposit.origin" -> Some("SMD"),
    )

    for ((key, value) <- props.collect { case (k, Some(v)) => (k, v) }) {
      properties.setProperty(key, value)
    }
  }

  private def hasDansDoiPrefix(deposit: Deposit): Boolean = {
    var exists = false
    for (identifier <- deposit.metadata.identifiers) {
      if (identifier.id.startsWith(dansDoiPrefix))
        exists = true
    }
    exists
  }
}
