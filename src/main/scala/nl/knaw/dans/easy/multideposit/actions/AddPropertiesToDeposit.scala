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

import java.{util => ju}
import java.util.{Collections, Properties}

import nl.knaw.dans.easy.multideposit.actions.AddPropertiesToDeposit._
import nl.knaw.dans.easy.multideposit.{Action, Settings, _}
import resource._

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import nl.knaw.dans.lib.error.TraversableTryExtensions

case class AddPropertiesToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends Action {

  val (datasetID, dataset) = entry

  // TODO administratieve metadata, to be decided

  override def checkPreconditions: Try[Unit] = {
    List(validateDepositor(row, datasetID, dataset), getDatamanagerMailadres)
      .collectResults
      .map(_ => ())
  }

  override def execute(): Try[Unit] = getDatamanagerMailadres.flatMap(writeProperties(row, datasetID, dataset, _))
}
object AddPropertiesToDeposit {
  // The email needs to be acquired (from LDAP) only once during the program execution
  private var datamanagerEmailaddress: Try[String] = _

  // Only used for testing
  private def resetDatamanagerEmailaddress() = {
    datamanagerEmailaddress = null
  }

  /**
   * Tries to retrieve the email address of the datamanager
   * Also used for validation: checks if the datamanager is an active archivist with an email address
   */
  def getDatamanagerMailadres(implicit settings: Settings): Try[String] = {
    val row = -1
    // Note that the datamanager 'precondition' is checked when datamanagerEmailaddress is evaluated the first time
    if(datamanagerEmailaddress == null) {
      val id = settings.datamanager
      datamanagerEmailaddress = settings.ldap.query(id)(a => a)
        .flatMap(attrsSeq => {
          if (attrsSeq.isEmpty) Failure(ActionException(row, s"""The datamanager "$id" is unknown"""))
          else if (attrsSeq.size > 1) Failure(ActionException(row, s"""There appear to be multiple users with id "$id""""))
          else Success(attrsSeq.head)
        })
        .flatMap(attrs => {
          Option(attrs.get("dansState"))
            .filter(_.get.toString == "ACTIVE")
            .map(_ => Success(attrs))
            .getOrElse(Failure(ActionException(row, s"""The datamanager "$id" is not an active user""")))
        })
        .flatMap(attrs => {
          Option(attrs.get("easyRoles"))
            .filter(_.contains("ARCHIVIST"))
            .map(_ => Success(attrs))
            .getOrElse(Failure(ActionException(row, s"""The datamanager "$id" is not an archivist""")))
        })
        .flatMap(attrs => {
          Option(attrs.get("mail"))
            .filter(_.get().toString.nonEmpty)
            .map(att => Success(att.get().toString))
            .getOrElse(Failure(ActionException(row, s"""The datamanager "$id" does not have an email address""")))
        })
    }
    datamanagerEmailaddress
  }

  /**
   * Checks whether there is only one unique DEPOSITOR_ID set in the `Dataset` (there can be multiple values but the must all be equal!).
   */
  def validateDepositor(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    // TODO a for-comprehension over monad-transformers would be nice here...
    // see https://github.com/rvanheest/Experiments/tree/master/src/main/scala/experiments/transformers
    // TODO check that only one depositorID is given, rather than multiple that are the same
    dataset.get("DEPOSITOR_ID")
      .map(_.filterNot(_.isBlank).toSet)
      .map(uniqueIDs => {
        if (uniqueIDs.size > 1) Failure(ActionException(row, s"""There are multiple distinct depositorIDs in dataset "$datasetID": $uniqueIDs""".stripMargin))
        else if (uniqueIDs.size < 1) Failure(ActionException(row, "No depositorID found"))
        else Success(uniqueIDs.head)
      })
      .map(_.flatMap(id => {
        settings.ldap
          .query(id)(attrs => Option(attrs.get("dansState")).exists(_.get.toString == "ACTIVE"))
          .flatMap(seq => if (seq.isEmpty) Failure(ActionException(row, s"""DepositorID "$id" is unknown"""))
                          else if (seq.size > 1) Failure(ActionException(row, s"""There appear to be multiple users with id "$id""""))
                          else Success(seq.head))
          .flatMap {
            case true => Success(())
            case false => Failure(ActionException(row, s"""The depositor "$id" is not an active user"""))
          }
      }))
      .getOrElse(Failure(ActionException(row, """The column "DEPOSITOR_ID" is not present""")))
  }

  def writeProperties(row: Int, datasetID: DatasetID, dataset: Dataset, emailaddress: String)(implicit settings: Settings): Try[Unit] = {
    val props = new Properties {
      // Make sure we get sorted output, which is better readable than random
      override def keys(): ju.Enumeration[AnyRef] = Collections.enumeration(new ju.TreeSet[Object](super.keySet()))
    }

    addProperties(props, dataset, settings.datamanager, emailaddress)
      .flatMap(_ => Using.fileWriter(encoding)(outputPropertiesFile(datasetID)).map(out => props.store(out, "")).tried)
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(row, s"Could not write properties to file: $e", e))
      }
  }

  def addProperties(properties: Properties, dataset: Dataset, datamanager: String, emailaddress: String): Try[Unit] = {
    for {
      depositorUserID <- dataset.get("DEPOSITOR_ID")
        .flatMap(_.headOption)
        .map(Success(_))
        .getOrElse(Failure(new IllegalStateException("""The column "DEPOSITOR_ID" is not present""")))
      _ = properties.setProperty("state.label", "SUBMITTED")
      _ = properties.setProperty("state.description", "Deposit is valid and ready for post-submission processing")
      _ = properties.setProperty("depositor.userId", depositorUserID)
      _ = properties.setProperty("datamanager.userId", datamanager)
      _ = properties.setProperty("datamanager.email", emailaddress)
    } yield ()
  }
}
