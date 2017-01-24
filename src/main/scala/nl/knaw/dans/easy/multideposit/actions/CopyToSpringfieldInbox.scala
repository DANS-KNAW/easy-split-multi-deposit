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
package nl.knaw.dans.easy.multideposit.actions

import nl.knaw.dans.easy.multideposit.{ Action, ActionException, Settings, _ }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

case class CopyToSpringfieldInbox(row: Int, fileMd: String)(implicit settings: Settings) extends Action with DebugEnhancedLogging {

  private val mdFile = multiDepositDir(settings, fileMd)

  override def checkPreconditions: Try[Unit] = {
    debug(s"Checking preconditions for $this")

    if (mdFile.exists) Success(Unit)
    else Failure(ActionException(row, s"Cannot find MD file: ${mdFile.getPath}"))
  }

  def execute(): Try[Unit] = {
    debug(s"Running $this")
    val sfFile = springfieldInboxDir(settings, fileMd)
    Try {
      mdFile.copyFile(sfFile)
    } recoverWith {
      case e => Failure(ActionException(row, s"Error in copying $mdFile to $sfFile: ${e.getMessage}", e))
    }
  }

  override def rollback(): Try[Unit] = {
    Try {
      debug(s"Rolling back $this")
      
      settings.springfieldInbox.listFiles.foreach(_.deleteDirectory())
    }
  }
}
