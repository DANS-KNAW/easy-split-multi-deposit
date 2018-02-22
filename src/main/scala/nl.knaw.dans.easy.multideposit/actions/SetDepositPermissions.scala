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

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute._

import nl.knaw.dans.easy.multideposit.DepositPermissions
import nl.knaw.dans.easy.multideposit.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.lib.error.TryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class SetDepositPermissions(depositPermissions: DepositPermissions) extends DebugEnhancedLogging {

  def setDepositPermissions(depositId: DepositId)(implicit stage: StagingPathExplorer): Try[Unit] = {
    logger.debug(s"set deposit permissions for $depositId")

    setFilePermissions(depositId).recoverWith {
      case e: ActionException => Failure(e)
      case NonFatal(e) => Failure(ActionException(e.getMessage, e))
    }
  }

  private def setFilePermissions(depositId: DepositId)(implicit stage: StagingPathExplorer): Try[Unit] = {
    val stagingDirectory = stage.stagingDir(depositId)
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
