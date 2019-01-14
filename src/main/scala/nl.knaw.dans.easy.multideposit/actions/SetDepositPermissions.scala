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

import better.files.File
import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.PathExplorer.StagingPathExplorer
import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.easy.multideposit.{ ActionError, DepositPermissions, FailFast }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.control.NonFatal

class SetDepositPermissions(depositPermissions: DepositPermissions) extends DebugEnhancedLogging {

  case class FilePermissionException(msg: String, cause: Throwable) extends Exception(msg, cause)

  def setDepositPermissions(depositId: DepositId)(implicit stage: StagingPathExplorer): FailFast[Unit] = {
    logger.debug(s"set deposit permissions for $depositId")

    setFilePermissions(depositId).leftMap {
      case FilePermissionException(msg, cause) => ActionError(msg, cause)
      case e => ActionError(e.getMessage, e)
    }
  }

  private def setFilePermissions(depositId: DepositId)(implicit stage: StagingPathExplorer): Either[Throwable, Unit] = {
    val stagingDirectory = stage.stagingDir(depositId)
    isOnPosixFileSystem(stagingDirectory)
      .flatMap {
        case true => Either.catchNonFatal {
          Files.walkFileTree(stagingDirectory.path, PermissionFileVisitor(depositPermissions))
        }
        case false => ().asRight
      }
  }

  private def isOnPosixFileSystem(file: File): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      file.permissions
      true
    } recover {
      case _: UnsupportedOperationException => false
    }
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
      Either.catchNonFatal {
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(depositPermissions.permissions))

        val group = path.getFileSystem.getUserPrincipalLookupService.lookupPrincipalByGroupName(depositPermissions.group)
        Files.getFileAttributeView(path, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS).setGroup(group)

        FileVisitResult.CONTINUE
      }.fold({
        case upnf: UserPrincipalNotFoundException => throw FilePermissionException(s"Group ${ depositPermissions.group } could not be found", upnf)
        case usoe: UnsupportedOperationException => throw FilePermissionException("Not on a POSIX supported file system", usoe)
        case cce: ClassCastException => throw FilePermissionException("No file permission elements in set", cce)
        case iae: IllegalArgumentException => throw FilePermissionException(s"Invalid privileges (${ depositPermissions.permissions })", iae)
        case fse: FileSystemException => throw FilePermissionException(s"Not able to set the group to ${ depositPermissions.group }. Probably the current user (${ System.getProperty("user.name") }) is not part of this group.", fse)
        case ioe: IOException => throw FilePermissionException(s"Could not set file permissions or group on $path", ioe)
        case se: SecurityException => throw FilePermissionException(s"Not enough privileges to set file permissions or group on $path", se)
        case NonFatal(e) => throw FilePermissionException(s"unexpected error occured on $path", e)
      }, fvr => fvr)
    }
  }
}
