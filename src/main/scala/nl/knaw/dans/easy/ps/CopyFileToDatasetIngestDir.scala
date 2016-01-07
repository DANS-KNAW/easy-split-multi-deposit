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

import org.apache.commons.io.FileUtils.{copyDirectory, copyFile}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CopyFileToDatasetIngestDir(row: String, datasetId: String, fileSip: String, fileDataset: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)
  val fileSource = file(s.sipDir, fileSip)
  val fileTarget = file(s.ebiuDir, datasetId, EBIU_FILEDATA_DIR, fileDataset)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    if (fileSource.exists) Success(Unit)
    else Failure(ActionException(row, s"Cannot copy $fileSource to $fileTarget because source file does not exist"))
  }
    

  override def run(): Try[Unit] =
    try {
      log.debug(s"Running $this")
      if(fileSource.isFile) Success(copyFile(fileSource, fileTarget))
      else Success(copyDirectory(fileSource, fileTarget))
    } catch {
      case e @ (_: IOException | _: NullPointerException) => Failure(e)
    }

  override def rollback(): Try[Unit] = Success(Unit)
}