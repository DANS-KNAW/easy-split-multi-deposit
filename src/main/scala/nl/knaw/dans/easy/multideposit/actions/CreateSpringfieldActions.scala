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

import nl.knaw.dans.easy.multideposit.actions.CreateSpringfieldActions._
import nl.knaw.dans.easy.multideposit.{Dataset, _}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}
import scala.xml.PrettyPrinter

case class Video(name: String, fileSip: Option[String], subtitles: Option[String])

case class CreateSpringfieldActions(row: Int, datasets: Datasets)(implicit settings: Settings) extends Action {

  val log = LoggerFactory.getLogger(getClass)

  def run() = {
    log.debug(s"Running $this")

    writeSpringfieldXml(row, datasets)
  }
}
object CreateSpringfieldActions {
  def writeSpringfieldXml(row: Int, datasets: Datasets)(implicit settings: Settings): Try[Unit] = {
    Try {
      toXML(datasets).foreach(springfieldInboxActionsFile(settings).write)
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write Springfield actions file to Springfield inbox: $e", e))
    }
  }

  def toXML(datasets: Datasets): Option[String] = {
    val elems = for {
      (_, dataset) <- datasets
      (target, videos) <- extractVideos(dataset)
    } yield createAddElement(target, videos)

    if (elems.nonEmpty)
      Some(new PrettyPrinter(160, 2).format(<actions>{elems}</actions>))
    else
      None
  }

  def createAddElement(target: String, videos: List[Video]) = {
    <add target={ target }>{
      videos.map {
        case Video(name, Some(src), Some(subtitles)) => <video src={src} target={name} subtitles={subtitles}/>
        case Video(name, Some(src), None) => <video src={src} target={name}/>
        case v => throw new RuntimeException(s"Invalid video object: $v")
      }
    }</add>
  }

  def extractVideos(dataset: Dataset): Map[String, List[Video]] = {
    def emptyMap = Map[String, List[Video]]()

    getSpringfieldPath(dataset, 0)
      .map(target => (emptyMap /: dataset.values.head.indices) {
        (map, i) => {
          (for {
            fileAudioVideo <- dataset.getValue("FILE_AUDIO_VIDEO")(i)
            if !fileAudioVideo.isBlank && fileAudioVideo.matches("(?i)yes")
            video = Video(i.toString, dataset.getValue("FILE_SIP")(i), dataset.getValue("FILE_SUBTITLES")(i))
            videos = map.getOrElse(target, Nil)
          } yield map + (target -> (videos :+ video)))
            .getOrElse(map)
        }
      })
      .getOrElse(emptyMap)
  }

  def getSpringfieldPath(dataset: Dataset, i: Int): Option[String] = {
    for {
      domain <- dataset.getValue("SF_DOMAIN")(i)
      user <- dataset.getValue("SF_USER")(i)
      collection <- dataset.getValue("SF_COLLECTION")(i)
      presentation <- dataset.getValue("SF_PRESENTATION")(i)
    } yield s"/domain/$domain/user/$user/collection/$collection/presentation/$presentation"
  }
}
