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

import java.io.FileOutputStream
import java.util.Properties

import nl.knaw.dans.easy.multideposit.actions.AddPropertiesToDeposit._
import nl.knaw.dans.easy.multideposit.{Action, Settings, _}
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Success, Try}

case class AddPropertiesToDeposit(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action {

  val log = LogFactory.getLog(getClass)

  def checkPreconditions = Success(())

  def run() = {
    log.debug(s"Running $this")

    writeProperties(row, datasetID)
  }

  def rollback() = Success(())
}
object AddPropertiesToDeposit {

  def writeProperties(row: Int, datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    Try {
      val props = new Properties
      // set properties???
      props.store(new FileOutputStream(outputPropertiesFile(settings, datasetID)), "")
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write properties to file: $e", e))
    }
  }
}
