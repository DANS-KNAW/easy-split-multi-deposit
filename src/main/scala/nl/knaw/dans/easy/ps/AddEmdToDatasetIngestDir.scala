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

import nl.knaw.dans.pf.language.ddm.api.Ddm2EmdCrosswalk
import nl.knaw.dans.pf.language.emd.EasyMetadata
import nl.knaw.dans.pf.language.emd.binding.EmdMarshaller
import nl.knaw.dans.pf.language.emd.types.{BasicIdentifier, EmdConstants}
import org.apache.commons.io.FileUtils.write
import org.slf4j.LoggerFactory
import org.xml.sax.{ErrorHandler, SAXParseException}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class AddEmdToDatasetIngestDir(row: String, dataset: Dataset)(implicit s: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  override def checkPreconditions: Try[Unit] = {
    log.debug(s"Checking preconditions for $this")
    val xml = DDM.datasetToXml(dataset)
    if(log.isDebugEnabled) log.debug("DDM for dataset:\n{}", xml)
    validateXml(xml, DDM_SCHEMA_URL).recoverWith {
      case e => Failure(ActionException(row, e.getMessage))
    }
  }

  override def run(): Try[Unit] = {
    log.debug(s"Running $this")
    val ddmXml = DDM.datasetToXml(dataset)
    val emd = new Ddm2EmdCrosswalk().createFromValidated(ddmXml)
    val marshaller = new EmdMarshaller(emd)
    marshaller.setIndent(2)
    marshaller.setEncoding("UTF-8")
    maybeAddStreamableVersionRelation(emd)
    Try {
      val emdFile = file(s.ebiuDir, id(dataset), EBIU_METADATA_DIR, EBIU_EMD_FILE)
      write(emdFile, marshaller.getXmlString, "UTF-8")
    }.recoverWith { case e => Failure(ActionException(row, e.getMessage)) }
  }

  override def rollback(): Try[Unit] = Success(Unit)

  private def maybeAddStreamableVersionRelation(emd: EasyMetadata): Try[Unit] =
    (getPathComponent("SF_DOMAIN"), getPathComponent("SF_USER"), getPathComponent("SF_COLLECTION"), getPathComponent("SF_PRESENTATION")) match {
      case (Some(domain), Some(user), Some(collection), Some(presentation)) =>
        Success(addStreamableVersionRelation(springfieldUrl(domain, user, collection, presentation), emd))
      case (None, None, None, None) => Success(Unit)
      case (d, u, c, p) => Failure(ActionException(row, s"The springfield (SF_*) columns must either all of them be provided OR none of them, found $d, $u, $c, $p"))
    }

  private def getPathComponent(name: String): Option[String] =
    dataset.get(name).flatMap(_.headOption)

  private def springfieldUrl(d: String, u: String, c: String, p: String): String =
    s"${s.springfieldStreamingBaseUrl}/domain/$d/user/$u/collection/$c/presentation/$p"

  private def addStreamableVersionRelation(url: String, emd: EasyMetadata): Unit = {
    val bi = new BasicIdentifier(url)
    bi.setScheme(EmdConstants.SCHEME_STREAMING_SURROGATE_RELATION)
    emd.getEmdRelation.getDcRelation.add(bi)
  }
}

class ErrorCollector extends ErrorHandler {
  var exceptions = ListBuffer[SAXParseException]()

  override def warning(e: SAXParseException): Unit = exceptions.append(e)
  override def error(e: SAXParseException): Unit = exceptions.append(e)
  override def fatalError(e: SAXParseException): Unit = exceptions.append(e)

}
