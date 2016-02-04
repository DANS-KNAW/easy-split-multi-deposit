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

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.slf4j.LoggerFactory

@Deprecated
case class CheckExistenceInStorage(row: String, fileStorageService: String, fileStoragePath: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
     runAssertExistenceInStorage 
  }
  // If we only check for the existence of some file at the given storage URL we 
  // might as well do it here.

  def run(): Try[Unit] = {
    log.debug(s"Running $this")
    runAssertExistenceInStorage
  }
  // What do we check apart from the fact that some file exists there?
  // 1. size ?
  // 2. checksum ?
  // If so, those must be provided

  def rollback(): Try[Unit] = Success(Unit)
  // Nothing to roll back

  def runAssertExistenceInStorage: Try[Unit] = {
    s.storage.exists(List(s.resolve(fileStorageService), fileStoragePath).mkString("/")) match {
      case Success(exists) => if (exists) Success(Unit) else Failure(ActionException(row, "File not present in storage"))
      case Failure(e) => Failure(ActionException(row, s"Unexpected response ${e.getMessage}"))
    }
  }
}
