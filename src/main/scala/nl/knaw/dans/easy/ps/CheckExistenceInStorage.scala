package nl.knaw.dans.easy.ps

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.slf4j.LoggerFactory

case class CheckExistenceInStorage(row: String, fileStorageService: String, fileStoragePath: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
     runAssertExistenceInStorage 
  }
  // If we only check for the existence of some file at the given storage URL we 
  // might as well do it here.

  def run(): Try[Unit] = {
    log.debug(s"Running $this")
    runAssertExistenceInStorage
  }
  // What do we check apart from the fact that some file exists there?
  // 1. size ?
  // 2. checksum ?
  // If so, those must be provided

  def rollback(): Try[Unit] = Success(Unit)
  // Nothing to roll back

  def runAssertExistenceInStorage: Try[Unit] = {
    s.storage.exists(List(s.resolve(fileStorageService), fileStoragePath).mkString("/")) match {
      case Success(exists) => if (exists) Success(Unit) else Failure(ActionException(row, "File not present in storage"))
      case Failure(e) => Failure(ActionException(row, s"Unexpected response ${e.getMessage}"))
    }
  }
}
