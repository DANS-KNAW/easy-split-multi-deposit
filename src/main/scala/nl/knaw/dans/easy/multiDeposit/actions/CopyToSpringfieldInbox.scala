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
package nl.knaw.dans.easy.multiDeposit.actions

import java.io.{IOException, File}

import nl.knaw.dans.easy.multiDeposit._
import nl.knaw.dans.easy.multiDeposit.{ActionException, Settings, Action}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.util.{Try, Failure, Success}

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
