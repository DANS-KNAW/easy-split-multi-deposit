package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit2.model.DepositId
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }

trait CreateDirectories extends DebugEnhancedLogging {
  this: StagingPathExplorer =>

  def createDepositDirectories(depositId: DepositId): Try[Unit] = {
    createDirectories(depositId)(stagingDir(depositId), stagingBagDir(depositId))
  }

  def createMetadataDirectory(depositId: DepositId): Try[Unit] = {
    createDirectories(depositId)(stagingBagMetadataDir(depositId))
  }

  private def createDirectories(depositId: DepositId)(paths: Path*): Try[Unit] = {
    logger.debug(s"creating directories: ${ paths.toString() }")
    paths.find(Files.exists(_))
      .map(file => Failure(ActionException(s"The deposit $depositId already exists in $file.")))
      .getOrElse {
        Try {
          paths.foreach(Files.createDirectories(_))
        } recoverWith {
          case NonFatal(e) => Failure(ActionException(s"Could not create the directories at $paths", e))
        }
      }
  }

  def discardDeposit(depositId: DepositId): Try[Unit] = {
    val dir = stagingDir(depositId)
    Try { if (Files.exists(dir)) dir.deleteDirectory() } recoverWith {
      case NonFatal(e) => Failure(ActionException(s"Could not delete $dir", e))
    }
  }
}
