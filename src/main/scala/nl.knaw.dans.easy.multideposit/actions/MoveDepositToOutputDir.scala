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

import java.nio.file.FileAlreadyExistsException

import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.{ ActionError, FailFast }
import nl.knaw.dans.easy.multideposit.PathExplorer.{ OutputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.{ BagId, DepositId }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

class MoveDepositToOutputDir extends DebugEnhancedLogging {

  def moveDepositsToOutputDir(depositId: DepositId, bagId: BagId)(implicit stage: StagingPathExplorer, output: OutputPathExplorer): FailFast[Unit] = {
    val stagingDirectory = stage.stagingDir(depositId)
    val outputDir = output.outputDepositDir(bagId)

    logger.debug(s"moving $stagingDirectory to $outputDir")

    Either.catchNonFatal { stagingDirectory.moveTo(outputDir, overwrite = false); () }.leftMap {
      case e: FileAlreadyExistsException =>
        ActionError(s"Could not move $stagingDirectory to $outputDir. The target " +
          "directory already exists. Since this is only possible when a UUID (universally unique " +
          "identifier) is not unique; you have hit the jackpot. The chance of this happening is " +
          "smaller than you being hit by a meteorite. So rejoice in the moment, because this " +
          "will be a once-in-a-lifetime experience. When you're done celebrating, just try to " +
          "deposit this and all remaining deposits (be careful not to deposit the deposits that " +
          "came before this lucky one, because they went through successfully).", e)
      case e =>
        Either.catchNonFatal { outputDir.exists } match {
          case Right(true) => ActionError("An error occurred while moving " +
            s"$stagingDirectory to $outputDir: ${ e.getMessage }. The move is probably only partially " +
            "done since the output directory does exist. This move is, however, NOT revertable! " +
            "Please contact your application manager ASAP!", e)
          case Right(false) => ActionError("An error occurred while moving " +
            s"$stagingDirectory to $outputDir: ${ e.getMessage }. The move did not take place, since " +
            "the output directory does not yet exist or is not on the same partition as the " +
            "staging directory.", e)
          case Left(e2) => ActionError("An error occurred both while moving " +
            s"$stagingDirectory to $outputDir: ${ e.getMessage } and while checking whether the " +
            s"output directory actually exists now: ${ e2.getMessage }. Please contact your " +
            "application manager ASAP!")
        }
    }
  }

  // Moves from staging to output only happen when all deposit creations have completed successfully.
  // These moves are not revertable, since they (depending on configuration) will be moved to the
  // easy-ingest-flow inbox, which might have started processing the first deposit when a second
  // move fails. At this point the application manager needs to take a look at what happened and why
  // the deposits were not able to be moved.
}
