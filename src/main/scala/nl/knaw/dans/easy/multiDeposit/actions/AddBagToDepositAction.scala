package nl.knaw.dans.easy.multiDeposit.actions

import gov.loc.repository.bagit.BagFactory
import gov.loc.repository.bagit.BagFactory.Version
import gov.loc.repository.bagit.writer.impl.FileSystemWriter
import nl.knaw.dans.easy.multiDeposit._
import nl.knaw.dans.easy.multiDeposit.actions.AddBagToDepositAction._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.seqAsJavaList
import scala.util.{Failure, Success, Try}

case class AddBagToDepositAction(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions = {
    log.debug(s"Checking preconditions for $this")

    val inputDir = multiDepositDir(settings, datasetID)
    if (inputDir.exists) Success(Unit)
    else Failure(ActionException(row, s"Dataset $datasetID: deposit directory does not exist"))
  }

  def run() = {
    log.debug(s"Running $this")

    createBag(datasetID)
  }

  def rollback() = Success(Unit)
}
object AddBagToDepositAction {
  // for examples see https://github.com/LibraryOfCongress/bagit-java/issues/18
  //              and http://www.mpcdf.mpg.de/services/data/annotate/downloads -> TacoHarvest
  def createBag(datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    Try {
      val inputDir = multiDepositDir(settings, datasetID)
      val depositDir = depositBagDir(settings, datasetID)

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
}
