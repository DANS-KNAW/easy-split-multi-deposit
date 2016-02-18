package nl.knaw.dans.easy.multiDeposit.actions

import java.io.File

import nl.knaw.dans.easy.multiDeposit.{Action, ActionException, Settings, _}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CopyToSpringfieldInbox(row: Int, fileMd: String)(implicit settings: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions = {
    log.debug(s"Checking preconditions for $this")

    val file = new File(settings.mdDir, fileMd)

    if (file.exists) Success(Unit)
    else Failure(ActionException(row, s"Cannot find MD file: ${file.getPath}"))
  }

  def run() = {
    Try {
      log.debug(s"Running $this")

      val mdFile = new File(settings.mdDir, fileMd)
      val sfFile = new File(settings.springfieldInbox, fileMd)

      mdFile.copyFile(sfFile)
    }
  }

  def rollback() = Success(Unit)
}
