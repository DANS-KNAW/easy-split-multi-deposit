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

import javax.naming.directory.Attributes

import nl.knaw.dans.easy.multideposit.Ldap
import nl.knaw.dans.easy.multideposit.model.{ Datamanager, DatamanagerEmailaddress }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

class RetrieveDatamanager(ldap: Ldap) extends DebugEnhancedLogging {

  /**
   * Tries to retrieve the email address of the datamanager
   * Also used for validation: checks if the datamanager is an active archivist with an email address
   */
  def getDatamanagerEmailaddress(datamanagerId: Datamanager): Try[DatamanagerEmailaddress] = {
    logger.info("retrieve datamanager email address")

    for {
      attrs <- ldap.query(datamanagerId)(a => a)
      attr <- getFirstAttrs(datamanagerId)(attrs)
      _ <- datamanagerIsActive(datamanagerId)(attr)
      _ <- datamanagerHasArchivistRole(datamanagerId)(attr)
      email <- getDatamanagerEmail(datamanagerId)(attr)
    } yield email
  }

  private def getFirstAttrs(datamanagerId: Datamanager)(attrsSeq: Seq[Attributes]): Try[Attributes] = {
    logger.debug("check that only one user is found")
    attrsSeq match {
      case Seq() => Failure(InvalidDatamanagerException(s"The datamanager '$datamanagerId' is unknown"))
      case Seq(attr) => Success(attr)
      case _ => Failure(ActionException(s"There appear to be multiple users with id '$datamanagerId'"))
    }
  }

  private def datamanagerIsActive(datamanagerId: Datamanager)(attrs: Attributes) = {
    logger.debug("check that datamanager is an active user")
    Option(attrs.get("dansState"))
      .filter(_.get().toString == "ACTIVE")
      .map(_ => Success(attrs))
      .getOrElse(Failure(InvalidDatamanagerException(s"The datamanager '$datamanagerId' is not an active user")))
  }

  private def datamanagerHasArchivistRole(datamanagerId: Datamanager)(attrs: Attributes) = {
    logger.debug("check that datamanager has archivist role")
    Option(attrs.get("easyRoles"))
      .filter(_.contains("ARCHIVIST"))
      .map(_ => Success(attrs))
      .getOrElse(Failure(InvalidDatamanagerException(s"The datamanager '$datamanagerId' is not an archivist")))
  }

  private def getDatamanagerEmail(datamanagerId: Datamanager)(attrs: Attributes) = {
    logger.debug("get datamanager email address")
    Option(attrs.get("mail"))
      .filter(_.get().toString.nonEmpty)
      .map(att => Success(att.get().toString))
      .getOrElse(Failure(InvalidDatamanagerException(s"The datamanager '$datamanagerId' does not have an email address")))
  }
}
