package nl.knaw.dans.easy.multideposit.actions

import java.io.{ File, IOException }
import java.nio.file.attribute.{ BasicFileAttributes, PosixFilePermissions }
import java.nio.file.{ FileVisitResult, Files, Path, SimpleFileVisitor }

import nl.knaw.dans.easy.multideposit.{ Action, DatasetID, _ }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

// TODO add the 'move to easy-ingest-flow inbox' functionality in this class
// TODO rename this file accordingly
case class SetDepositPermissions(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action {

  def execute(): Try[Unit] = {
    setFilePermissions().recoverWith {
      case NonFatal(e) => Failure(ActionException(row, e.getMessage, e))
    }
  }

  private def setFilePermissions(): Try[Unit] = {
    val depositDir = outputDepositDir(datasetID)
    isOnPosixFileSystem(depositDir)
      .flatMap {
        case true => Try { Files.walkFileTree(depositDir.toPath, PermissionFileVisitor(settings.depositPermissions)) }
        case false => Success(())
      }
  }

  private def isOnPosixFileSystem(file: File): Try[Boolean] = Try {
    Files.getPosixFilePermissions(file.toPath)
    true
  } recover {
    case _: UnsupportedOperationException => false
  }

  private case class PermissionFileVisitor(permissions: String) extends SimpleFileVisitor[Path] with DebugEnhancedLogging {
    override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      Try {
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(permissions))
        FileVisitResult.CONTINUE
      } onError {
        case usoe: UnsupportedOperationException => logger.error("Not on a POSIX supported file system", usoe); FileVisitResult.TERMINATE
        case cce: ClassCastException => logger.error("Non file permission elements in set", cce); FileVisitResult.TERMINATE
        case iae: IllegalArgumentException => logger.error(s"Invalid privileges ($permissions)", iae); FileVisitResult.TERMINATE
        case ioe: IOException => logger.error(s"Could not set file permissions on $path", ioe); FileVisitResult.TERMINATE
        case se: SecurityException => logger.error(s"Not enough privileges to set file permissions on $path", se); FileVisitResult.TERMINATE
        case NonFatal(e) => logger.error(s"unexpected error occured on $path", e); FileVisitResult.TERMINATE
      }
    }

    override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
      Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString(permissions))
      if (exception == null) FileVisitResult.CONTINUE
      else FileVisitResult.TERMINATE
    }
  }
}
