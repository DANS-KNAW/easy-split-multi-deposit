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
import org.apache.commons.lang.StringUtils._
import org.apache.commons.io.FileUtils._
import scala.xml.PrettyPrinter
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.Success
import scala.util.Failure
import org.slf4j.LoggerFactory

case class CreateSpringfieldActions(row: String, ds: Datasets)(implicit settings: Settings) extends Action(row) {
  val log = LoggerFactory.getLogger(getClass)

  override def checkPreconditions: Try[Unit] = Success(Unit)

  override def run(): Try[Unit] = {
    log.debug(s"Running $this")
    writeSpringfieldXml
  }

  override def rollback(): Try[Unit] = Success(Unit)

  case class Video(fileSip: Option[String], name: Option[String], subtitles: Option[String])

  def writeSpringfieldXml: Try[Unit] =
    try
      Success(writeStringToFile(file(settings.springfieldInbox, "springfield-actions.xml"), toXml.toString))
    catch {
      case e: Exception => Failure(ActionException(row, s"Could not write springfield actions file to springfield inbox: $e"))
    }

  def toXml = {
    new PrettyPrinter(80, 2).format(
      <actions>{
        ds.map(_._2).flatMap(d =>
          extractAddVideos(d)
            .entrySet
            .map(e => createAddElement(e.getKey, e.getValue)))
      }</actions>)
  }

  def createAddElement(target: String, vs: mutable.ListBuffer[Video]) = {
    <add target={ target }>{
      vs.map {
        case Video(Some(src), Some(name), Some(subs)) => <video src={ src } target={ name } subtitles={ subs }/>
        case Video(Some(src), Some(name), None) => <video src={ src } target={ name }/>
        case _ => throw new RuntimeException("Not implemented")
      }
    }</add>
  }

  def extractAddVideos(d: Dataset): mutable.Map[String, mutable.ListBuffer[Video]] = {
    val map = mutable.HashMap[String, mutable.ListBuffer[Video]]()
    getSpringfieldPath(d)(0) match {
      case Some(target) =>
        (0 to d.values.head.size - 1).map(i => {
          def sfp(key: String) = d(key)(i)
          if (!isBlank(sfp("FILE_AUDIO_VIDEO")) && sfp("FILE_AUDIO_VIDEO").matches("(?i)yes")) {
            val video = Video(getStringOption(sfp("FILE_SIP")),
              Some(i.toString),
              getStringOption(sfp("FILE_SUBTITLES")))
            val videos = map.getOrElse(target, mutable.ListBuffer[Video]())
            map.put(target, videos :+ video)
          }
        })
      case None => // Leave map empty
    }
    map
  }

  def getSpringfieldPath(d: Dataset)(i: Int): Option[String] = {
    val domain = getStringOption(d("SF_DOMAIN")(i))
    val user = getStringOption(d("SF_USER")(i))
    val collection = getStringOption(d("SF_COLLECTION")(i))
    val presentation = getStringOption(d("SF_PRESENTATION")(i))
    (domain, user, collection, presentation) match {
      case (Some(d), Some(u), Some(c), Some(p)) => Some(s"/domain/$d/user/$u/collection/$c/presentation/$p")
      case _ => None
    }
  }
}
