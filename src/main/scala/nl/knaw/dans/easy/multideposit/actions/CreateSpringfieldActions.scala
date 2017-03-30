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

import nl.knaw.dans.easy.multideposit.{ Dataset, _ }
import nl.knaw.dans.lib.error.{ CompositeException, TraversableTryExtensions }

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.Elem

case class Video(name: String, fileSip: Option[String], subtitles: Option[String])

/*
TODO For this Action all datasets need to be in memory at the same time. However, only the SF fields
     from each dataset are used (if any). This can be optimized to where we only provide the useful
     fields to this Action. We can collect these fields in a case class in Main (or anywhere else).
     With this, we can eventually make the whole application reactive by also parsing the csv lazily
     and reactive.
 */
case class CreateSpringfieldActions(row: Int, datasets: Datasets)(implicit settings: Settings) extends UnitAction[Unit] {

  override def execute(): Try[Unit] = CreateSpringfieldActions.writeSpringfieldXml(row, datasets)
}
object CreateSpringfieldActions {

  type SpringfieldPath = String

  def writeSpringfieldXml(row: Int, datasets: Datasets)(implicit settings: Settings): Try[Unit] = {
    toXML(datasets)
      .map(_.map(springfieldInboxActionsFile(settings).writeXml(_)))
      .getOrElse(Success(()))
      .recoverWith {
        case e@CompositeException(es) => Failure(ActionException(row, s"Could not write Springfield actions file to Springfield inbox: ${es.mkString(", ")}", e))
        case NonFatal(e) => Failure(ActionException(row, s"Could not write Springfield actions file to Springfield inbox: $e", e))
      }
  }

  def toXML(datasets: Datasets): Option[Try[Elem]] = {
    val elems = for {
      (_, dataset) <- datasets
      (target, videos) <- extractVideos(dataset)
    } yield createAddElement(target, videos)

    if (elems.nonEmpty)
      // @formatter:off
      Some(elems.collectResults.map(es => <actions>{es}</actions>))
      // @formatter:on
    else
      None
  }

  def extractVideos(dataset: Dataset): Map[SpringfieldPath, List[Video]] = {
    def emptyMap = Map.empty[SpringfieldPath, List[Video]]

    getSpringfieldPath(dataset, 0)
      .map(path => dataset.values.head.indices.foldRight(emptyMap)((i, map) => {
        val maybePathToVideos = for {
          fileAudioVideo <- dataset.getValue("FILE_AUDIO_VIDEO")(i)
          if !fileAudioVideo.isBlank && fileAudioVideo.matches("(?i)yes")
          video = Video(i.toString, dataset.getValue("FILE_SIP")(i), dataset.getValue("FILE_SUBTITLES")(i))
          videos = map.getOrElse(path, Nil)
        } yield map + (path -> (video :: videos))

        maybePathToVideos.getOrElse(map)
      }))
      .getOrElse(emptyMap)
  }

  def getSpringfieldPath(dataset: Dataset, i: Int): Option[SpringfieldPath] = {
    for {
      domain <- dataset.getValue("SF_DOMAIN")(i)
      user <- dataset.getValue("SF_USER")(i)
      collection <- dataset.getValue("SF_COLLECTION")(i)
    } yield s"/domain/$domain/user/$user/collection/$collection/presentation/$$sdo-id"
  }

  def createAddElement(target: SpringfieldPath, videos: List[Video]): Try[Elem] = {
    videos
      .map {
        case Video(name, Some(src), Some(subtitles)) => Try(<video src={src} target={name} subtitles={subtitles}/>)
        case Video(name, Some(src), None) => Try(<video src={src} target={name}/>)
        case v => Failure(new RuntimeException(s"Invalid video object: $v"))
      }
      .collectResults
      .map(xmls => <add target={target}>{xmls}</add>)
  }
}
