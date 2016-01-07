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
package nl.knaw.dans.easy

import java.io.{File, FileInputStream, FileNotFoundException, StringReader}
import java.net.{URI, URL}
import javax.xml.XMLConstants
import javax.xml.parsers.SAXParserFactory
import javax.xml.validation.SchemaFactory

import com.typesafe.config.ConfigValue
import org.apache.commons.lang.StringUtils.isBlank
import org.semanticdesktop.aperture.mime.identifier.magic.MagicMimeTypeIdentifierFactory
import org.semanticdesktop.aperture.util.IOUtil.readBytes
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.{InputSource, SAXParseException}
import rx.lang.scala.Observable

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object ps {
  /*
   * Constants
   */
  val PROCESS_SIP_CFG_DIR = "cfg"
  val PROCESS_SIP_RESOURCE_DIR = "res"
  val EBIU_METADATA_DIR = "metadata"
  val EBIU_FILEDATA_DIR = "filedata"
  val EBIU_AMD_FILE = "administrative-metadata.xml"
  val EBIU_EMD_FILE = "easymetadata.xml"
  val DDM_SCHEMA_URL = new URI("https://easy.dans.knaw.nl/schemas/md/2014/09/ddm.xsd").toURL

  /*
   * Global types
   */
  type DatasetID = String
  type MdKey = String
  type MdValues = List[String]
  type Dataset = mutable.HashMap[MdKey, MdValues]
  //  type Datasets = mutable.HashMap[DatasetID, Dataset]
  type Datasets = ListBuffer[(DatasetID, Dataset)]

  case class ActionException(row: String, message: String) extends RuntimeException(message)
  case class ActionExceptionList(aes: Seq[ActionException]) extends RuntimeException

  case class Settings(appHomeDir: File = null,
    sipDir: File = null,
    ebiuDir: File = null,
    springfieldInbox: File = null,
    springfieldStreamingBaseUrl: String = null,
    storageServices: Map[String, ConfigValue] = null,
    storage: StorageConnector = null) {
    override def toString: String =
      s"Settings(home=$appHomeDir, sip-dir=$sipDir, ebiu-dir=$ebiuDir, springfield-inbox=$springfieldInbox, " +
        s"springfield-streaming-baseurl=$springfieldStreamingBaseUrl, storage-services=$storageServices"

    def resolve(storageService: String): String =
      storageServices.get(storageService) match {
        case Some(url) => url.unwrapped match {
          case s: String => s
          case o => throw new RuntimeException(s"*** Programming error. Expected a String, got a ${o.getClass}")
        }
        case None => storageService
      }

  }

  def fileInSipDir(settings: Settings, fileSip: String): File = file(settings.sipDir, fileSip)
  def fileInDatasetIngestDir(settings: Settings, datasetId: String, fileInDataset: String): File = file(settings.ebiuDir, datasetId, EBIU_FILEDATA_DIR, fileInDataset)
  def fileInSpringfieldInbox(settings: Settings, path: String): File = file(settings.springfieldInbox, path)

  type FileParameters = (Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])
  def getStringOption(s: String) = if (isBlank(s)) None else Some(s)

  def getIntOption(s: String) =
    try {
      if (isBlank(s)) None else Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }

  def askUsername(): String = {
    print("Username: ")
    System.console.readLine()
  }

  def askPassword(): String = {
    print("Password: ")
    System.console.readPassword().mkString
  }

  implicit class ListExtensions(val thisActionList: List[Action]) extends AnyVal {
    def run(): Observable[Action] =
      Observable.from(thisActionList).flatMap(action => action.run() match {
        case Success(_) => Observable.just(action)
        case Failure(e) => Observable.just(action) ++ Observable.error(e)
      })
  }

  def file(dir: File, filename: String*): File = new File(dir, filename.mkString("/"))
  def file(name: String): File = new File(name)

  def datasetIngestDir(settings: Settings, dataset: Dataset): File =
    file(settings.ebiuDir, dataset("DATASET_ID").head)

  def id(dataset: Dataset): String = dataset("DATASET_ID").head

  def extractFileParametersList(d: Dataset): List[FileParameters] = {
    (d.get("ROW") ::
      d.get("FILE_SIP") ::
      d.get("FILE_DATASET") ::
      d.get("FILE_STORAGE_SERVICE") ::
      d.get("FILE_STORAGE_PATH") ::
      d.get("FILE_AUDIO_VIDEO") ::
      Nil).find(_.isDefined) match {
        case Some(Some(column)) =>
          (0 until column.size)
          .map(i => {
            def valueAt(mdValues: Option[MdValues]): Option[String] =
              mdValues.flatMap(values => getStringOption(values(i)))
            (
              valueAt(d.get("ROW")),
              valueAt(d.get("FILE_SIP")),
              valueAt(d.get("FILE_DATASET")),
              valueAt(d.get("FILE_STORAGE_SERVICE")),
              valueAt(d.get("FILE_STORAGE_PATH")),
              valueAt(d.get("FILE_AUDIO_VIDEO")))
          }).toList.filter {
            case (_, None, None, None, None, None) => false
            case _ => true
          }
        case Some(_) => Nil // To satify the compiler. This could only happen if the dataset had no lines
        case None => Nil
      }
  }

  def createErrorReporterException[T](heading: String, failures: Seq[Try[T]]): Exception = {
    val errorList = failures.flatMap {
      case Failure(ae: ActionException) => List(ae)
      case Failure(ActionExceptionList(aes)) => List() ++ aes
      case _ => throw new AssertionError("Only Failures of ActionException/ActionExceptionList expected here")
    }.sortWith {
      case ((ActionException(row1, _), ActionException(row2, _))) =>
        Integer.parseInt(row1) < Integer.parseInt(row2)
      case _ => throw new AssertionError("Only pairs of ActionsException expected here")
    }.map {
      case ActionException(row, msg) => s" - row $row: $msg"
    }.mkString("\n")
    new Exception(s"${if (isBlank(heading)) "" else s"$heading\n"}$errorList")
  }

  def validateXml(xml: File, xsd: File): Try[Unit] =
    if (xml.exists && xml.isFile)
      validateXml(Source.fromFile(xml, "UTF-8").mkString, xsd.toURI.toURL)
    else
      Failure(new FileNotFoundException(s"$xml was not found or is a directory"))

  def validateXml(xml: String, xsd: URL): Try[Unit] =
    Try {
      val schema =
        SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
          .newSchema(xsd.toURI.toURL)
      val saxParser = {
        val f = SAXParserFactory.newInstance()
        f.setNamespaceAware(true)
        f.setSchema(schema)
        f.newSAXParser()
      }
      saxParser.parse(new InputSource(new StringReader(xml)), new DefaultHandler() {
        override def error(e: SAXParseException) = throw e
      })
    }

  def checkMultipleConditions(conditions: Try[Unit]*): Try[Unit] =
    if (conditions.exists(_.isFailure))
      Failure(ActionExceptionList(conditions.filter(_.isFailure).map(_.asInstanceOf[Failure[Unit]] match {
        case Failure(ae: ActionException) => ae
        case Failure(e) =>
          throw new AssertionError("*** Programmer error: Failure must contain ActionException but contained ${e.getClass}")
      })))
    else
      Success(Unit)

  def getMimeType(file: File): Option[String] = {
    val is = new FileInputStream(file)
    try {
      val identifier = new MagicMimeTypeIdentifierFactory().get
      val fileBytes = readBytes(new FileInputStream(file))
      val mime = identifier.identify(fileBytes, file.getPath, null)
      if (mime == null) None else Some(mime)
    } finally {
      is.close()
    }
  }
}