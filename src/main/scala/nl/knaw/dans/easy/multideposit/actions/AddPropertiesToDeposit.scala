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

import nl.knaw.dans.easy.multideposit.{Action, Settings, _}
import resource._

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class AddPropertiesToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends Action[DatamanagerEmailaddress, Unit] {

  val (datasetID, dataset) = entry

  // TODO administratieve metadata, to be decided

  override def checkPreconditions: Try[Unit] = validateDepositor(row, datasetID, dataset)

  override def execute(datamanagerEmailaddress: DatamanagerEmailaddress): Try[Unit] = {
    getDepositorID(dataset).map(writeProperties(_, datamanagerEmailaddress))
  }

  /**
   * Checks whether there is only one unique DEPOSITOR_ID set in the `Dataset` (there can be multiple values but the must all be equal!).
   */
  private def validateDepositor(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    def depositorIsActive(id: String) = {
      settings.ldap.query(id)(attrs => Option(attrs.get("dansState")).exists(_.get.toString == "ACTIVE"))
        .flatMap {
          case Seq() => Failure(ActionException(row, s"""DepositorID "$id" is unknown"""))
          case Seq(head) => Success(head)
          case _ => Failure(ActionException(row, s"""There appear to be multiple users with id "$id""""))
        }
        .flatMap {
          case true => Success(())
          case false => Failure(ActionException(row, s"""The depositor "$id" is not an active user"""))
        }
    }

    dataset.get("DEPOSITOR_ID")
      .map(mdValues => {
        val depIds = mdValues.filterNot(_.isBlank).toSet
        val depIdsSize = depIds.size

        if (depIdsSize > 1)
          Failure(ActionException(row, s"""There are multiple distinct depositorIDs in dataset "$datasetID": $depIds""".stripMargin))
        else if (depIdsSize < 1)
          Failure(ActionException(row, "No depositorID found"))
        else
          depositorIsActive(depIds.head)
      })
      .getOrElse(Failure(ActionException(row, """The column "DEPOSITOR_ID" is not present""")))
  }

  private def writeProperties(depositorUserID: String, emailaddress: DatamanagerEmailaddress)(implicit settings: Settings): Try[Unit] = {
    val props = new Properties {
      // Make sure we get sorted output, which is better readable than random
      override def keys(): ju.Enumeration[AnyRef] = Collections.enumeration(new ju.TreeSet[Object](super.keySet()))
    }

    Try { addProperties(props, depositorUserID, settings.datamanager, emailaddress) }
      .flatMap(_ => Using.fileWriter(encoding)(stagingPropertiesFile(datasetID)).map(out => props.store(out, "")).tried)
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(row, s"Could not write properties to file: $e", e))
      }
  }

  private def addProperties(properties: Properties, depositorUserID: String, datamanager: String, emailaddress: DatamanagerEmailaddress): Unit = {
    val sf = getSpringfieldData
    val props: Map[String, Option[String]] = Map(
      "state.label" -> Some("SUBMITTED"),
      "state.description" -> Some("Deposit is valid and ready for post-submission processing"),
      "depositor.userId" -> Some(depositorUserID),
      "datamanager.userId" -> Some(datamanager),
      "datamanager.email" -> Some(emailaddress),
      "springfield.domain" -> sf.map(_.domain),
      "springfield.user" -> sf.map(_.user),
      "springfield.collection" -> sf.map(_.collection)
    )

    for ((key, value) <- props.collect { case (k, Some(v)) => (k, v) }) {
      properties.setProperty(key, value)
    }
  }

  private def getDepositorID(dataset: Dataset): Try[String] = {
    dataset.get("DEPOSITOR_ID")
      .flatMap(_.headOption)
      .map(Success(_))
      .getOrElse(Failure(new IllegalStateException("""The column "DEPOSITOR_ID" is not present""")))
  }

  /**
   * @return Retrieve the springfield data from the dataset iff the springfield data is present
   */
  private def getSpringfieldData: Option[SpringfieldData] = {
    for {
      user <- dataset.findValue("SF_USER")
      collection <- dataset.findValue("SF_COLLECTION")
    } yield {
      dataset.findValue("SF_DOMAIN")
        .map(SpringfieldData(collection, user, _))
        .getOrElse(SpringfieldData(collection, user))
    }
  }

  private case class SpringfieldData(collection: String,
                                     user: String,
                                     domain: String = "dans")
}
