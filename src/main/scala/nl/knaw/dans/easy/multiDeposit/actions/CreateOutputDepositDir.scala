package nl.knaw.dans.easy.multiDeposit.actions

import nl.knaw.dans.easy.multiDeposit
import nl.knaw.dans.easy.multiDeposit._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CreateOutputDepositDir(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions: Try[Unit] = Success(Unit)

  def run(): Try[Unit] = {
    log.debug(s"Running $this")
    val depositDir = multiDeposit.depositDir(settings, datasetID)
    val bagDir = depositBagDir(settings, datasetID)
    val metadataDir = depositBagMetadataDir(settings, datasetID)

    log.debug(s"Creating Deposit Directory at $depositDir with bag directory = $bagDir and metadata directory = $metadataDir")

    if (depositDir.mkdir && bagDir.mkdir && metadataDir.mkdir)
      Success(Unit)
    else
      Failure(new ActionException(row, s"Could not create the dataset output deposit directory at $depositDir"))
  }

  def rollback(): Try[Unit] = {
    val depositDir = multiDeposit.depositDir(settings, datasetID)
    log.debug(s"Deleting directory $depositDir")

    Try {
      depositDir.deleteDirectory()
    } recoverWith {
      case e: Exception => Failure(ActionException(row, s"Could not delete $depositDir, exception: $e"))
    }
  }
}
