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

import nl.knaw.dans.easy.multideposit.actions.AddFileMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.{Action, Settings, _}
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Success, Try}
import scala.xml.PrettyPrinter

case class AddFileMetadataToDeposit(row: Int, dataset: (DatasetID, Dataset))(implicit settings: Settings) extends Action(row) {

  val log = LogFactory.getLog(getClass)

  def checkPreconditions = Success(())

  def run() = {
    log.debug(s"Running $this")

    writeFileMetadataXml(row, dataset)
  }

  def rollback() = Success(())
}
object AddFileMetadataToDeposit {

  def writeFileMetadataXml(row: Int, dataset: (DatasetID, Dataset))(implicit settings: Settings): Try[Unit] = {
    Try {
      outputFileMetadataFile(settings, dataset._1)
        .write(new PrettyPrinter(160, 2).format(datasetToFileXml(dataset)))
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write file meta data: $e"))
    }
  }

  def datasetToFileXml(dataset: (DatasetID, Dataset))(implicit settings: Settings) = {
    val inputDir = multiDepositDir(settings, dataset._1)

    <files>{
      if (inputDir.exists && inputDir.isDirectory)
        inputDir.listRecursively
          .map(file => s"data${file.getAbsolutePath.split(dataset._1).last}")
          .map(xmlPerPath(dataset._2))
    }</files>
  }

  def xmlPerPath(dataset: Dataset)(relativePath: String) = {
    <file filepath={relativePath}>{
      DDM.filesFields
        .map {
          case (name, xmlTag) => dataset.getOrElse(name, Nil)
            .filter(!_.isBlank)
            .map(value => <label>{value}</label>.copy(label = xmlTag))
        }
    }</file>
  }
}
