package nl.knaw.dans.easy.multiDeposit.actions

import java.io.File

import gov.loc.repository.bagit.BagFactory
import gov.loc.repository.bagit.BagFactory.Version
import gov.loc.repository.bagit.writer.impl.FileSystemWriter
import nl.knaw.dans.easy.multiDeposit.{DatasetID, Action, Settings, _}

import scala.collection.JavaConversions.seqAsJavaList

case class CreateBagAction(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action(row) {
  def checkPreconditions = ???

  def run() = ???

  def rollback() = ???
}
object CreateBagAction {

  // for examples see https://github.com/LibraryOfCongress/bagit-java/issues/18
  //              and http://www.mpcdf.mpg.de/services/data/annotate/downloads -> TacoHarvest
  def createBag(datasetID: DatasetID)(implicit settings: Settings) = {
    val multidepositDir = settings.multidepositDir

    val inputDir = new File(multidepositDir, datasetID) // the existence of this directory is a precondition
    val depositDir = depositDirBag(settings, datasetID)

    depositDir.mkdirs()

    val bagFactory = new BagFactory
    val preBag = bagFactory.createPreBag(depositDir)
    val bag = bagFactory.createBag(depositDir)

    bag.addFilesToPayload(inputDir.listFiles.toList)
    bag.makeComplete

    val fsw = new FileSystemWriter(bagFactory)
    fsw.write(bag, depositDir)

    preBag.makeBagInPlace(Version.V0_97, false)
  }
}
