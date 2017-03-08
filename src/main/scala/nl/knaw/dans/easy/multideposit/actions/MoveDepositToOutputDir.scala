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
