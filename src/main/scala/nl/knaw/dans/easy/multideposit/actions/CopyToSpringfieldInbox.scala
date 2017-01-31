/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

case class CopyToSpringfieldInbox(row: Int, fileMd: String)(implicit settings: Settings) extends Action {

  private val mdFile = multiDepositDir(fileMd)

  override def checkPreconditions: Try[Unit] = {
    for {
      _ <- super.checkPreconditions
      _ <- if (mdFile.exists) Success(Unit)
           else Failure(ActionException(row, s"Cannot find MD file: ${ mdFile.getPath }"))
    } yield ()
  }

  override def execute(): Try[Unit] = {
    for {
      _ <- super.execute()
      sfFile = springfieldInboxDir(fileMd)
      _ <- Try { mdFile.copyFile(sfFile) }
        .recoverWith { case NonFatal(e) => Failure(ActionException(row, s"Error in copying $mdFile to $sfFile: ${ e.getMessage }", e)) }
    } yield ()
  }

  override def rollback(): Try[Unit] = {
    for {
      _ <- super.rollback()
      _ <- Try { settings.springfieldInbox.listFiles.foreach(_.deleteDirectory()) }
    } yield ()
  }
}
