package nl.knaw.dans.easy.ps

import java.io.IOException

import org.apache.commons.io.FileUtils.{copyDirectory, copyFile}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CopyFileToDatasetIngestDir(row: String, datasetId: String, fileSip: String, fileDataset: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)
  val fileSource = file(s.sipDir, fileSip)
  val fileTarget = file(s.ebiuDir, datasetId, EBIU_FILEDATA_DIR, fileDataset)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    if (fileSource.exists) Success(Unit)
    else Failure(ActionException(row, s"Cannot copy $fileSource to $fileTarget because source file does not exist"))
  }
    

  override def run(): Try[Unit] =
    try {
      log.debug(s"Running $this")
      if(fileSource.isFile) Success(copyFile(fileSource, fileTarget))
      else Success(copyDirectory(fileSource, fileTarget))
    } catch {
      case e @ (_: IOException | _: NullPointerException) => Failure(e)
    }

  override def rollback(): Try[Unit] = Success(Unit)
}