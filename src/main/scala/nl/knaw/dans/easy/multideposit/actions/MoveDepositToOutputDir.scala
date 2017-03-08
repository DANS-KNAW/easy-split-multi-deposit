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

import nl.knaw.dans.easy.multideposit.{ Action, DatasetID, Settings, _ }

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

case class MoveDepositToOutputDir(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action {

  private val outputDir = outputDepositDir(datasetID)

  override def checkPreconditions: Try[Unit] = {
    Try { outputDir.exists() }
      .flatMap {
        case true => Failure(ActionException(row, s"The deposit for dataset $datasetID already exists in $outputDir"))
        case false => Success(())
      }
  }

  def execute(): Try[Unit] = {
    val stagingDirectory = stagingDir(datasetID)

    debug(s"moving $stagingDirectory to $outputDir")

    Try { stagingDirectory.moveDir(outputDir) } recover {
      case NonFatal(e) => println(s"An error occurred while moving $stagingDirectory to " +
        s"$outputDir: ${e.getMessage}. This move is NOT revertable! When in doubt, contact your " +
        s"application manager.", e)
    }
  }

  // Moves from staging to output only happen when all deposit creations have completed successfully.
  // These moves are not revertable, since they (depending on configuration) will be moved to the
  // easy-ingest-flow inbox, which might have started processing the first deposit when a second
  // move fails. At this point the application manager needs to take a look at what happened and why
  // the deposits where not able to be moved.
}
