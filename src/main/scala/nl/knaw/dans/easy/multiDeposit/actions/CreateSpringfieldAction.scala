package nl.knaw.dans.easy.multiDeposit.actions

import java.io.File

import nl.knaw.dans.easy.multiDeposit.Dataset
import nl.knaw.dans.easy.multiDeposit._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import scala.xml.PrettyPrinter

import nl.knaw.dans.easy.multiDeposit.actions.CreateSpringfieldAction._

case class Video(name: String, fileSip: Option[String], subtitles: Option[String])

case class CreateSpringfieldAction(row: Int, datasets: Datasets)(implicit settings: Settings) extends Action(row) {

  val log = LoggerFactory.getLogger(getClass)

  def checkPreconditions = Success(Unit)

  def run() = {
    log.debug(s"Running $this")
    writeSpringfieldXml(row, datasets)
  }

  def rollback() = Success(Unit)
}
object CreateSpringfieldAction {
  def writeSpringfieldXml(row: Int, datasets: Datasets)(implicit settings: Settings): Try[Unit] = {
    Try {
      val file = new File(settings.springfieldInbox, "springfield-actions.xml")
      Success(file.write(toXML(datasets)))
    }.onError(e => Failure(ActionException(row, s"Could not write Springfield actions file to Springfield inbox: $e")))
  }

  def toXML(datasets: Datasets) = {
    new PrettyPrinter(80, 2).format(
      <actions> {
        for {
          dataset <- datasets.map(_._2)
          (target, videos) <- extractVideos(dataset)
        } yield createAddElement(target, videos)
      }</actions>)
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
      .map(target => (emptyMap /: (0 until dataset.values.head.size)) {
        (map, i) => {
          (for {
            fileAudioVideo <- dataset.getValue("FILE_AUDIO_VIDEO")(i)
            if !fileAudioVideo.isBlank && fileAudioVideo.matches("(?i)yes")
            video = Video(i.toString, dataset.getValue("FILE_SIP")(i), dataset.getValue("FILE_SUBTITLES")(i))
            videos = map.getOrElse(target, Nil)
          } yield map + (target -> (videos :+ video)))
            .getOrElse(emptyMap)
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