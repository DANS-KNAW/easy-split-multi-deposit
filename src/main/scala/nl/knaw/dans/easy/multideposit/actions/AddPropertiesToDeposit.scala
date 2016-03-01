package nl.knaw.dans.easy.multideposit.actions

import java.io.{File, FileOutputStream}
import java.util.Properties

import nl.knaw.dans.easy.multideposit.{Settings, Action, _}
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Try}

case class AddPropertiesToDeposit(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action(row) {

  val log = LogFactory.getLog(getClass)

  def checkPreconditions = ???

  def run() = {
    log.debug(s"Running $this")

    ???
  }

  def rollback() = ???
}
object AddPropertiesToDeposit {

  def writeProperties(row: Int, datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    Try {
      val props = new Properties
      // set properties???
      props.store(new FileOutputStream(new File(outputDepositDir(settings, datasetID), "deposit.properties")), "")
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write properties to file: $e"))
    }
  }

}
