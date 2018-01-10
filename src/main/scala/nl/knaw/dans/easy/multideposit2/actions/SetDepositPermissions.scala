package nl.knaw.dans.easy.multideposit2.actions

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute._

import nl.knaw.dans.easy.multideposit2.DepositPermissions
import nl.knaw.dans.easy.multideposit2.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit2.model.DepositId
import nl.knaw.dans.lib.error.TryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait SetDepositPermissions {
  this: StagingPathExplorer =>

  val depositPermissions: DepositPermissions

  def setDepositPermissions(depositId: DepositId): Try[Unit] = {
    setFilePermissions(depositId: DepositId).recoverWith {
      case e: ActionException => Failure(e)
      case NonFatal(e) => Failure(ActionException(e.getMessage, e))
    }
  }

  private def setFilePermissions(depositId: DepositId): Try[Unit] = {
    val stagingDirectory = stagingDir(depositId)
    isOnPosixFileSystem(stagingDirectory)
      .flatMap {
        case true => Try {
          Files.walkFileTree(stagingDirectory, PermissionFileVisitor(depositPermissions))
        }
        case false => Success(())
      }
  }

  private def isOnPosixFileSystem(file: Path): Try[Boolean] = Try {
    Files.getPosixFilePermissions(file)
    true
  } recover {
    case _: UnsupportedOperationException => false
  }

  private case class PermissionFileVisitor(depositPermissions: DepositPermissions) extends SimpleFileVisitor[Path] with DebugEnhancedLogging {
    override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      changePermissions(path)
    }

    override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
      Option(exception)
        .map(_ => FileVisitResult.TERMINATE)
        .getOrElse(changePermissions(dir))
    }

    private def changePermissions(path: Path): FileVisitResult = {
      Try {
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(depositPermissions.permissions))

        val group = path.getFileSystem.getUserPrincipalLookupService.lookupPrincipalByGroupName(depositPermissions.group)
        Files.getFileAttributeView(path, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setGroup(group)

        FileVisitResult.CONTINUE
      } getOrRecover {
        case upnf: UserPrincipalNotFoundException => throw ActionException(s"Group ${ depositPermissions.group } could not be found", upnf)
        case usoe: UnsupportedOperationException => throw ActionException("Not on a POSIX supported file system", usoe)
        case cce: ClassCastException => throw ActionException("No file permission elements in set", cce)
        case iae: IllegalArgumentException => throw ActionException(s"Invalid privileges (${ depositPermissions.permissions })", iae)
        case fse: FileSystemException => throw ActionException(s"Not able to set the group to ${ depositPermissions.group }. Probably the current user (${ System.getProperty("user.name") }) is not part of this group.", fse)
        case ioe: IOException => throw ActionException(s"Could not set file permissions or group on $path", ioe)
        case se: SecurityException => throw ActionException(s"Not enough privileges to set file permissions or group on $path", se)
        case NonFatal(e) => throw ActionException(s"unexpected error occured on $path", e)
      }
    }
  }
}
