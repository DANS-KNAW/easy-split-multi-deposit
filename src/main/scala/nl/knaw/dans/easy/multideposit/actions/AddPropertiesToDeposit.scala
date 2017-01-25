/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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

import java.util.Properties

import nl.knaw.dans.easy.multideposit.actions.AddPropertiesToDeposit._
import nl.knaw.dans.easy.multideposit.{ Action, Settings, _ }
import resource._

import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

case class AddPropertiesToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends Action {

  val (datasetID, dataset) = entry

  // TODO administratieve metadata, to be decided

  /**
   * @inheritdoc
   * Checks whether there is only one unique DEPOSITOR_ID set in the `Dataset` (there can be multiple values but the must all be equal!).
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    for {
      _ <- super.checkPreconditions
      _ <- validateDepositor(row, datasetID, dataset)
    } yield ()
  }

  override def execute(): Try[Unit] = {
    for {
      _ <- super.execute()
      _ <- writeProperties(row, datasetID, dataset)
    } yield ()
  }
}
object AddPropertiesToDeposit {

  def validateDepositor(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    // TODO a for-comprehension over monad-transformers would be nice here...
    // see https://github.com/rvanheest/Experiments/tree/master/src/main/scala/experiments/transformers
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

  def writeProperties(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    val props = new Properties

    addProperties(props, dataset)
      .flatMap(_ => Using.fileWriter(encoding)(outputPropertiesFile(settings, datasetID)).map(out => props.store(out, "")).tried)
      .recoverWith {
      case e => Failure(ActionException(row, s"Could not write properties to file: $e", e))
    }
  }

  def addProperties(properties: Properties, dataset: Dataset): Try[Unit] = {
    for {
      depositorUserID <- dataset.get("DEPOSITOR_ID")
        .flatMap(_.headOption)
        .map(Success(_))
        .getOrElse(Failure(new IllegalStateException("""The column "DEPOSITOR_ID" is not present""")))
      _ = properties.setProperty("state.label", "SUBMITTED")
      _ = properties.setProperty("state.description", "Deposit is valid and ready for post-submission processing")
      _ = properties.setProperty("depositor.userId", depositorUserID)
    } yield ()
  }
}
