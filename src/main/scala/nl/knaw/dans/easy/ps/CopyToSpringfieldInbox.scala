package nl.knaw.dans.easy.ps

import java.io.IOException
import org.apache.commons.io.FileUtils._
import scala.util.{ Failure, Success, Try }
import org.slf4j.LoggerFactory

case class CopyToSpringfieldInbox(row: String, fileSip: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    val file = fileInSipDir(s, fileSip)
    if (file.exists) Success(())
    else Failure(ActionException(row, s"Cannot find SIP file: ${file.getPath}"))
  }

  override def run(): Try[Unit] =
    try {
      log.debug(s"Running $this")
      Success(copyFile(fileInSipDir(s, fileSip), fileInSpringfieldInbox(s, fileSip)))
    } catch {
      case e @ (_: IOException | _: NullPointerException) => Failure(e)
    }

  override def rollback(): Try[Unit] = Success(Unit)

}