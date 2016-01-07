/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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