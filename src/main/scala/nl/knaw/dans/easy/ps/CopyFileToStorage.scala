package nl.knaw.dans.easy.ps

import java.net.MalformedURLException

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CopyFileToStorage(
  row: String,
  storageService: String,
  fileSip: String,
  fileStorageDatasetPath: Option[String],
  fileStorageFilePath: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  private val fileStorageService = s.resolve(storageService)

  private val sipFile = file(s.sipDir, fileSip)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    val isLocationFree = fileStorageDatasetPath
      .map(_ => checkStorageTargetLocationFree)
      .getOrElse(Failure(ActionException(row, "path on storage could not be determined")))
    val sipExists = if (sipFile.exists()) Success(())
                    else Failure(ActionException(row, s"Cannot find SIP file: ${sipFile.getPath}"))
    checkMultipleConditions(isLocationFree, sipExists)
  }

  // 1. Mandatory columns exist
  // 2. Enough data for a storage base path, provided or calculated
  // 3. Assert that storage location is free

  override def run(): Try[Unit] = {
    log.debug(s"Running $this")
    val path = List(fileStorageDatasetPath.get, fileStorageFilePath).mkString("/")
    val url = List(fileStorageService, fileStorageDatasetPath.get, fileStorageFilePath).mkString("/")
    log.debug(s"Copying $fileSip to url $url")
    if (ensureDirectoryExists(fileStorageService, path))
      s.storage.writeFileTo(file(s.sipDir, fileSip), url).recoverWith {
        case e => Failure(ActionException(row, s"Could not store $fileSip at $url: ${e.getMessage}"))
      }
    else
      Failure(ActionException(row, s"Could not find or create path $path on storage service $fileStorageService"))
  }

  override def rollback(): Try[Unit] = Success(Unit)

  private def checkStorageTargetLocationFree: Try[Unit] = {
    val url = List(fileStorageService, fileStorageDatasetPath.get, fileStorageFilePath).mkString("/")
    try
      s.storage.exists(url) match {
        case Success(exists) => if (!exists) Success(Unit) else Failure(ActionException(row, s"Storage location already occupied: $url"))
        case Failure(e) => Failure(ActionException(row, s"Unexpected response when checking if storage location $url was free:  ${e.getMessage}"))
      }
    catch {
      case e: MalformedURLException => Failure(ActionException(row, s"Storage URL invalid: ${e.getMessage}"))
      case e: Exception => Failure(ActionException(row, s"Unable to determine whether storage location $url is free: ${e.getMessage} (exception class: $e.getClass)"))
    }
  }

  private def ensureDirectoryExists(fileStorageService: String, directoryPath: String): Boolean = {
    val dirComponents = fileStorageService +: directoryPath.split("/")
    val parentDirs = dirComponents.indices.tail.map(i => dirComponents.slice(0, i).mkString("/") + "/")
    parentDirs.forall(url =>
      s.storage.exists(url) match {
        case Success(exists) => exists || s.storage.createDirectory(url).isSuccess
        case Failure(e) => false
      })
  }
}