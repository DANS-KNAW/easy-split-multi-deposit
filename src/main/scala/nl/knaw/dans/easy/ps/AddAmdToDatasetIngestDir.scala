package nl.knaw.dans.easy.ps

import org.apache.commons.io.FileUtils.copyFileToDirectory
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class AddAmdToDatasetIngestDir(row: String, datasetId: DatasetID)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)
  val AMD = "Administrative metadata"
  val fileSource = file(s.appHomeDir, PROCESS_SIP_RESOURCE_DIR, EBIU_AMD_FILE)
  val amdSchema = file(s.appHomeDir, PROCESS_SIP_RESOURCE_DIR, "amd.xsd")
  val dirTarget = file(s.ebiuDir, datasetId, EBIU_METADATA_DIR)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    if (!fileSource.exists) Failure(ActionException(row, s"$AMD template not found at $fileSource"))
    else if (!fileSource.isFile) Failure(ActionException(row, s"$AMD template not found at $fileSource. File specified is a directory"))
    else validateXml(fileSource, amdSchema).recoverWith {
      case e => Failure(ActionException(row, s"$AMD template does not conform to AMD schema, msg: ${e.getMessage}"))
    }
  }
  
  override def run(): Try[Unit] =
    try {
      log.debug(s"Running $this")  
      Success(copyFileToDirectory(fileSource, dirTarget))
    }
    catch {
      case e: Exception => Failure(ActionException(row, s"Runtime error trying to copy $AMD to dataset ingest directory: ${e.getMessage}"))
    }

  override def rollback(): Try[Unit] = {
    log.debug("Relying on CreateDatasetIngestDir.rollback to delete everything")
    Success(Unit)
  }
}
