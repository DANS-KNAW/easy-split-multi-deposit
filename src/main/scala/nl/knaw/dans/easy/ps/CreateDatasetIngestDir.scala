package nl.knaw.dans.easy.ps

import java.io.File
import scala.util.{ Failure, Success, Try }
import org.apache.commons.io.FileUtils.{ copyDirectory, deleteDirectory }
import org.apache.commons.io.filefilter.AbstractFileFilter
import org.slf4j.LoggerFactory

case class CreateDatasetIngestDir(row: String, dataset: Dataset, fpss: List[FileParameters])(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)
  val METADATA_DIR_NAME = "metadata"
  val FILEDATA_DIR_NAME = "filedata"

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    val datasetDirName = getDatasetDirName(dataset)
    val datasetDir = new File(s.ebiuDir, datasetDirName)
    if (!datasetDir.exists) Success(Unit)
    else Failure(ActionException(row, s"Dataset ${getDatasetId(dataset)}: Dataset Ingest Directory $datasetDir cannot be created, because it already exists"))
  }

  override def run(): Try[Unit] = {
    log.debug(s"Running $this")
    initialize(dataset, s.sipDir, s.ebiuDir) match {
      case Success(_) => Success(Unit)
      case Failure(e) => Failure(ActionException(row, s"Dataset ${getDatasetId(dataset)}: Dataset Ingest Directory creating failed: ${e.getMessage}"))
    }
  }

  override def rollback(): Try[Unit] = {
    log.debug("Deleting directory {}", getDatasetDir)
    try
      Success(deleteDirectory(getDatasetDir))
    catch {
      case e: Exception => Failure(ActionException(row, s"Could not delete $getDatasetDir, exception: $e"))
    }
  }

  def initialize(d: Dataset, sipDir: File, ebiuDir: File): Try[File] =
    createDatasetDir(d, ebiuDir).map(copyInstructionlessFiles(d, sipDir))

  def createDatasetDir(dataset: Dataset, ebiuDir: File): Try[File] = {
    val datasetDirName = getDatasetDirName(dataset)
    val datasetDir = new File(ebiuDir, datasetDirName)
    val metadataDir = new File(datasetDir, METADATA_DIR_NAME)
    val filedataDir = new File(datasetDir, FILEDATA_DIR_NAME)
    log.debug("Creating Dataset Ingest Directory at {} with metadata directory = {} and filedata directory = {}", datasetDir, metadataDir, filedataDir)
    if (datasetDir.mkdir() && metadataDir.mkdir() && filedataDir.mkdir())
      Success(datasetDir)
    else
      Failure(new RuntimeException(s"Could not create Dataset Ingest Directory at $datasetDir"))
  }

  def getDatasetDirName(d: Dataset): String = getDatasetId(d)

  def getDatasetId(d: Dataset): String = d("DATASET_ID").head.trim

  def getDatasetDir: File = new File(s.ebiuDir, getDatasetDirName(dataset))

  def copyInstructionlessFiles(dataset: Dataset, sipDir: File)(datasetDir: File): File = {
    val sipDataDir = new File(sipDir, getDatasetDirName(dataset))
    if (sipDataDir.exists) {
      log.debug("Copying instructionless files for SIP dataset directory {}", sipDataDir)
      val filePathExcludeList = fpss.collect {
        case (Some(line), Some(fileSip), _, _, _, _) => new File(sipDir, fileSip)
      }
      log.debug("Excluding files with specific instructions: {}", filePathExcludeList)
      copyDirectory(sipDataDir, new File(datasetDir, FILEDATA_DIR_NAME), new AbstractFileFilter {
        override def accept(file: File): Boolean = !filePathExcludeList.contains(file)
      })
    }
    datasetDir
  }
}
