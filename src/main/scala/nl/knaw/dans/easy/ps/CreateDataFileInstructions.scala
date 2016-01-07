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

import java.io.FileOutputStream
import java.util.Properties

import org.apache.commons.io.FilenameUtils.getName
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class CreateDataFileInstructions(
  row: String,
  fileSip: Option[String],
  datasetId: String,
  fileInDataset: String,
  fileStorageService: String,
  fileStorageDatasetPath: Option[String],
  fileStorageFilePath: String)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    val checkStoragePath = fileStorageDatasetPath match {
      case Some(_) => Success(())
      case None => Failure(ActionException(row, "path on storage could not be determined"))
    }

    // TODO: check if sipFile exists, then size and mime are detectable otherwise file size and mime-type should be provided...

    checkMultipleConditions(checkStoragePath)
  }

  override def run(): Try[Unit] = {
    log.debug(s"Running $this")
    runCreateDataFileInstructions()
  }

  override def rollback(): Try[Unit] = Success(Unit)

  def runCreateDataFileInstructions(): Try[Unit] = {
    val url = List(s.resolve(fileStorageService), fileStorageDatasetPath.get, fileStorageFilePath).mkString("/")

    val (mime, size) = fileSip
      .map(fs => fileInSipDir(s, fs))
      .map(file => (getMimeType(file).getOrElse("application/octet-stream"), file.length))
      .getOrElse(("video/mpeg", 1337l)) // FIXME: fetch from specified values

    val props = new Properties()
    props.setProperty("easy.data-file-instructions", "yes")
    props.setProperty("easy.file.data.url", url)
    props.setProperty("easy.file.name", getName(fileStorageFilePath))
    props.setProperty("easy.file.mime-type", mime)
    props.setProperty("easy.file.size", size.toString)
    fileInDatasetIngestDir(s, datasetId, fileInDataset).getParentFile.mkdirs()
    props.store(new FileOutputStream(fileInDatasetIngestDir(s, datasetId, fileInDataset + ".properties")), "")
    Success(Unit)
  }

}