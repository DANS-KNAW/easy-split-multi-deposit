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
        case cce: ClassCastException => logger.error("No file permission elements in set", cce); FileVisitResult.TERMINATE
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
