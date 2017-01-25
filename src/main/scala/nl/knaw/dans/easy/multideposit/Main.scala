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
package nl.knaw.dans.easy.multideposit

import nl.knaw.dans.easy.multideposit.actions._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

object Main extends DebugEnhancedLogging {

  def main(args: Array[String]): Unit = {
    debug("Starting application")
    implicit val settings = CommandLineOptions.parse(args)

    run
      .ifFailure { case e => logger.error(e.getMessage) }
      .ifFailure { case e => logger.debug(e.getMessage, e) }
      .ifSuccess(_ => logger.info("Finished successfully!"))

    logger.debug("closing ldap")
    settings.ldap.close()
  }

  def run(implicit settings: Settings): Try[Unit] = {
    val datasets: Try[Datasets] = MultiDepositParser.parse(multiDepositInstructionsFile(settings))
    val composedAction: Try[Action] = datasets.map(getActions)
    val result: Try[Unit] = composedAction.flatMap(_.run)

    result
  }

  def getActions(datasets: Datasets)(implicit s: Settings): Action = {
    logger.info("Compiling list of actions to perform ...")

    val csa = CreateSpringfieldActions(-1, datasets)
    val actions = datasets.map(getDatasetAction) += csa
    val action = actions.reduce(_ compose _)

    action
  }

  def getDatasetAction(entry: (DatasetID, Dataset))(implicit settings: Settings): Action = {
    val datasetID = entry._1
    val dataset = entry._2
    val row = dataset("ROW").head.toInt // first occurence of dataset, assuming it is not empty

    logger.debug(s"Getting actions for dataset $datasetID ...")

    val actions: List[Action] = List(
      CreateOutputDepositDir(row, datasetID),
      AddBagToDeposit(row, datasetID),
      AddDatasetMetadataToDeposit(row, entry),
      AddFileMetadataToDeposit(row, entry),
      AddPropertiesToDeposit(row, entry))

    val composedAction: Action = actions.reduce(_ compose _)

    getFileAction(dataset, extractFileParameters(dataset))
      .map(composedAction.compose)
      .getOrElse(composedAction)
  }

  def getFileAction(dataset: Dataset, fpss: Seq[FileParameters])(implicit settings: Settings): Option[Action] = {
    fpss.collect {
      case FileParameters(Some(row), Some(fileMd), _, _, _, Some(isThisAudioVideo))
        if isThisAudioVideo matches "(?i)yes" => CopyToSpringfieldInbox(row, fileMd): Action
    }.reduceOption(_ compose _)
  }

  def extractFileParameters(dataset: Dataset): Seq[FileParameters] = {
    Seq("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")
      .flatMap(dataset.get)
      .take(1)
      .flatMap(xs => xs.indices
        .map(index => {
          def valueAt(key: String): Option[String] = {
            dataset.get(key).flatMap(_(index).toOption)
          }
          def intAt(key: String): Option[Int] = {
            dataset.get(key).flatMap(_(index).toIntOption)
          }

          FileParameters(
            row = intAt("ROW"),
            sip = valueAt("FILE_SIP"),
            dataset = valueAt("FILE_DATASET"),
            storageService = valueAt("FILE_STORAGE_SERVICE"),
            storagePath = valueAt("FILE_STORAGE_PATH"),
            audioVideo = valueAt("FILE_AUDIO_VIDEO")
          )
        })
        .filter {
          case FileParameters(_, None, None, None, None, None) => false
          case _ => true
        })
  }
}
