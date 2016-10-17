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

import java.io.File
import java.net.URLConnection

import nl.knaw.dans.easy.multideposit.actions.AddFileMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.{Action, Settings, _}
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Try}

case class AddFileMetadataToDeposit(row: Int, datasetID: DatasetID)(implicit settings: Settings) extends Action {

  val log = LogFactory.getLog(getClass)

  def run() = {
    log.debug(s"Running $this")

    writeFileMetadataXml(row, datasetID)
  }
}
object AddFileMetadataToDeposit {

  def writeFileMetadataXml(row: Int, datasetID: DatasetID)(implicit settings: Settings): Try[Unit] = {
    Try {
      outputFileMetadataFile(settings, datasetID).writeXml(datasetToFileXml(datasetID))
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write file meta data: $e", e))
    }
  }

  def datasetToFileXml(datasetID: DatasetID)(implicit settings: Settings) = {
    val inputDir = multiDepositDir(settings, datasetID)

    <files xmlns:dcterms="http://purl.org/dc/terms/">{
      if (inputDir.exists && inputDir.isDirectory)
        inputDir.listRecursively.map(xmlPerPath(inputDir))
    }</files>
  }

  // TODO other fields need to be added here later
  def xmlPerPath(inputDir: File)(file: File) = {
    <file filepath={s"data/${inputDir.toPath.relativize(file.toPath)}"}>{
      <dcterms:format>{getMimeType(file.getPath)}</dcterms:format>
    }</file>
  }


  val fileExtensionMap = scala.collection.immutable.HashMap(
    // MS Office
    "doc" -> "application/msword",
    "dot" -> "application/msword",
    "docx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "dotx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.template",
    "docm" -> "application/vnd.ms-word.document.macroEnabled.12",
    "dotm" -> "application/vnd.ms-word.template.macroEnabled.12",
    "xls" -> "application/vnd.ms-excel",
    "xlt" -> "application/vnd.ms-excel",
    "xla" -> "application/vnd.ms-excel",
    "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xltx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
    "xlsm" -> "application/vnd.ms-excel.sheet.macroEnabled.12",
    "xltm" -> "application/vnd.ms-excel.template.macroEnabled.12",
    "xlam" -> "application/vnd.ms-excel.addin.macroEnabled.12",
    "xlsb" -> "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
    "ppt" -> "application/vnd.ms-powerpoint",
    "pot" -> "application/vnd.ms-powerpoint",
    "pps" -> "application/vnd.ms-powerpoint",
    "ppa" -> "application/vnd.ms-powerpoint",
    "pptx" -> "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "potx" -> "application/vnd.openxmlformats-officedocument.presentationml.template",
    "ppsx" -> "application/vnd.openxmlformats-officedocument.presentationml.slideshow",
    "ppam" -> "application/vnd.ms-powerpoint.addin.macroEnabled.12",
    "pptm" -> "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
    "potm" -> "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
    "ppsm" -> "application/vnd.ms-powerpoint.slideshow.macroEnabled.12",
    // Open Office
    "odt" -> "application/vnd.oasis.opendocument.text",
    "ott" -> "application/vnd.oasis.opendocument.text-template",
    "oth" -> "application/vnd.oasis.opendocument.text-web",
    "odm" -> "application/vnd.oasis.opendocument.text-master",
    "odg" -> "application/vnd.oasis.opendocument.graphics",
    "otg" -> "application/vnd.oasis.opendocument.graphics-template",
    "odp" -> "application/vnd.oasis.opendocument.presentation",
    "otp" -> "application/vnd.oasis.opendocument.presentation-template",
    "ods" -> "application/vnd.oasis.opendocument.spreadsheet",
    "ots" -> "application/vnd.oasis.opendocument.spreadsheet-template",
    "odc" -> "application/vnd.oasis.opendocument.chart",
    "odf" -> "application/vnd.oasis.opendocument.formula",
    "odb" -> "application/vnd.oasis.opendocument.database",
    "odi" -> "application/vnd.oasis.opendocument.image",
    "oxt" -> "application/vnd.openofficeorg.extension",
    // Other
    "txt" -> "text/plain",
    "rtf" -> "application/rtf",
    "pdf" -> "application/pdf"

  );
  def getMimeType(filename: String): String ={
    var mimetype = URLConnection.getFileNameMap.getContentTypeFor(filename);
    if (mimetype == null){
      val extension = filename.substring(filename.lastIndexOf('.') + 1, filename.length());
      mimetype = fileExtensionMap(extension);
    }
    return mimetype;
  }






}
