package nl.knaw.dans.easy.multideposit.actions

import java.io.File

import nl.knaw.dans.easy.multideposit.actions.AddFileMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.{Action, Settings, _}

import scala.util.{Failure, Success, Try}
import scala.xml.PrettyPrinter

class AddFileMetadataToDeposit(row: Int, dataset: (DatasetID, Dataset))(implicit settings: Settings) extends Action(row) {
  def checkPreconditions = Success(())

  def run() = writeFileMetadataXml(row, dataset)

  def rollback() = Success(())
}
object AddFileMetadataToDeposit {

  val DATA_FOLDER = "data"

  def writeFileMetadataXml(row: Int, dataset: (DatasetID, Dataset))(implicit settings: Settings): Try[Unit] = {
    Try {
      val file = new File(outputDepositBagMetadataDir(settings, dataset._1), "files.xml")
      file.write(new PrettyPrinter(160, 2).format(datasetToFileXml(dataset)))
    } recoverWith {
      case e =>
        e.printStackTrace()
        Failure(ActionException(row, s"Could not write file meta data: $e"))
    }
  }

  def datasetToFileXml(dataset: (DatasetID, Dataset))(implicit settings: Settings) = {
    val bagDir = outputDepositBagDir(settings, dataset._1)

    <files>{
      new File(bagDir, DATA_FOLDER)
        .listRecursively
        .map(_.relativePath(DATA_FOLDER))
        .map(xmlPerPath(dataset._2))
    }</files>
  }

  def xmlPerPath(dataset: Dataset)(relativePath: String) = {
    <file filepath={relativePath}>{
      DDM.filesFields
        .map {
          case (name, xmlTag) => dataset.getOrElse(name, Nil)
            .filter(!_.isBlank)
            .map(value => <label>{value}</label>.copy(label = xmlTag))
        }
    }</file>
  }
}
