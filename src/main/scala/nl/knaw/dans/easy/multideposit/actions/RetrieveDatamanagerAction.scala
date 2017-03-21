/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.actions

import nl.knaw.dans.easy.multideposit.{ Action, ActionException, DatamanagerEmailaddress, Settings }

import scala.util.{ Failure, Success, Try }

case class RetrieveDatamanagerAction(implicit settings: Settings) extends Action[DatamanagerEmailaddress] {

  private lazy val datamanagerEmailaddress = getDatamanagerMailadres

  override def checkPreconditions: Try[Unit] = {
    datamanagerEmailaddress.map(_ => ())
  }

  override def execute(): Try[String] = datamanagerEmailaddress

  /**
   * Tries to retrieve the email address of the datamanager
   * Also used for validation: checks if the datamanager is an active archivist with an email address
   */
  private def getDatamanagerMailadres(implicit settings: Settings): Try[String] = {
    val row = -1
    // Note that the datamanager 'precondition' is checked when datamanagerEmailaddress is evaluated the first time
    val datamanagerId = settings.datamanager
    settings.ldap.query(datamanagerId)(a => a)
      .flatMap(attrsSeq => {
        if (attrsSeq.isEmpty) Failure(ActionException(row, s"""The datamanager "$datamanagerId" is unknown"""))
        else if (attrsSeq.size > 1) Failure(ActionException(row, s"""There appear to be multiple users with id "$datamanagerId""""))
        else Success(attrsSeq.head)
      })
      .flatMap(attrs => {
        Option(attrs.get("dansState"))
          .filter(_.get.toString == "ACTIVE")
          .map(_ => Success(attrs))
          .getOrElse(Failure(ActionException(row, s"""The datamanager "$datamanagerId" is not an active user""")))
      })
      .flatMap(attrs => {
        Option(attrs.get("easyRoles"))
          .filter(_.contains("ARCHIVIST"))
          .map(_ => Success(attrs))
          .getOrElse(Failure(ActionException(row, s"""The datamanager "$datamanagerId" is not an archivist""")))
      })
      .flatMap(attrs => {
        Option(attrs.get("mail"))
          .filter(_.get().toString.nonEmpty)
          .map(att => Success(att.get().toString))
          .getOrElse(Failure(ActionException(row, s"""The datamanager "$datamanagerId" does not have an email address""")))
      })
  }
}
