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

import java.util.{ Collections, Properties, UUID }
import java.{ util => ju }

import nl.knaw.dans.easy.multideposit.model.Deposit
import nl.knaw.dans.easy.multideposit.{ Action, Settings, _ }
import resource._

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

case class AddPropertiesToDeposit(deposit: Deposit)(implicit settings: Settings) extends Action[DatamanagerEmailaddress, Unit] {

  override def checkPreconditions: Try[Unit] = validateDepositorUserId

  override def execute(datamanagerEmailaddress: DatamanagerEmailaddress): Try[Unit] = {
    writeProperties(datamanagerEmailaddress)
  }

  /**
   * Checks whether there is only one unique DEPOSITOR_ID set in the `Deposit` (there can be multiple values but the must all be equal!).
   */
  private def validateDepositorUserId: Try[Unit] = {
    settings.ldap.query(deposit.depositorUserId)(attrs => Option(attrs.get("dansState")).exists(_.get().toString == "ACTIVE"))
      .flatMap {
        case Seq() => Failure(ActionException(deposit.row, s"depositorUserId '${ deposit.depositorUserId }' is unknown"))
        case Seq(head) => Success(head)
        case _ => Failure(ActionException(deposit.row, s"There appear to be multiple users with id '${ deposit.depositorUserId }'"))
      }
      .flatMap {
        case true => Success(())
        case false => Failure(ActionException(deposit.row, s"The depositor '${ deposit.depositorUserId }' is not an active user"))
      }
  }

  private def writeProperties(emailaddress: DatamanagerEmailaddress)(implicit settings: Settings): Try[Unit] = {
    val props = new Properties {
      // Make sure we get sorted output, which is better readable than random
      override def keys(): ju.Enumeration[AnyRef] = Collections.enumeration(new ju.TreeSet[Object](super.keySet()))
    }

    Try { addProperties(props, emailaddress) }
      .flatMap(_ => Using.fileWriter(encoding)(stagingPropertiesFile(deposit.depositId).toFile)
        .map(out => props.store(out, ""))
        .tried)
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(deposit.row, s"Could not write properties to file: $e", e))
      }
  }

  private def addProperties(properties: Properties, emailaddress: DatamanagerEmailaddress): Unit = {
    val sf = deposit.audioVideo.springfield
    val props: Map[String, Option[String]] = Map(
      "bag-store.bag-id" -> Some(UUID.randomUUID().toString),
      "state.label" -> Some("SUBMITTED"),
      "state.description" -> Some("Deposit is valid and ready for post-submission processing"),
      "depositor.userId" -> Some(deposit.depositorUserId),
      "datamanager.userId" -> Some(settings.datamanager),
      "datamanager.email" -> Some(emailaddress),
      "springfield.domain" -> sf.map(_.domain),
      "springfield.user" -> sf.map(_.user),
      "springfield.collection" -> sf.map(_.collection),
      "springfield.playmode" -> deposit.audioVideo.playMode.map(_.toString)
    )

    for ((key, value) <- props.collect { case (k, Some(v)) => (k, v) }) {
      properties.setProperty(key, value)
    }
  }
}
