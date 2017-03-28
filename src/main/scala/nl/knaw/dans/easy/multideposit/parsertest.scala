package nl.knaw.dans.easy.multideposit

import java.io.File

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.actions.FileAccessRights
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.joda.time.DateTime
import resource._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

object parsertest extends App {

  sealed abstract class Creator
  case class CreatorOrganization(organization: String) extends Creator
  case class CreatorPerson(titles: Option[String],
                           initials: String,
                           insertions: Option[String],
                           surname: String,
                           dai: Option[String]) extends Creator

  sealed abstract class Contributer
  case class ContributerOrganization(organization: String) extends Contributer
  case class ContributerPerson(titles: Option[String],
                           initials: String,
                           insertions: Option[String],
                           surname: String,
                           dai: Option[String]) extends Contributer

  sealed abstract class Relation
  case class QualifiedLinkRelation(qualifier: String, link: String) extends Relation
  case class QualifiedTitleRelation(qualifier: String, title: String) extends Relation
  case class LinkRelation(link: String) extends Relation
  case class TitleRelation(title: String) extends Relation

  case class Subject(subject: String, scheme: Option[String])
  case class Temporal(temporal: String, scheme: Option[String])

  case class SpatialPoint(x: String, y: String, scheme: String)
  case class SpatialBox(north: String, south: String, east: String, west: String, scheme: String)

  case class Springfield(domain: String, user: String, collection: String)
  case class Subtitles(path: String, language: Option[String])
  case class AVFile(path: String, title: Option[String], subtitles: Option[Subtitles])

  case class Profile(titles: List[String],
                     descriptions: List[String],
                     creators: List[Creator],
                     created: DateTime,
                     audiences: List[String], // or List[enum values]?
                     accessright: List[AccessCategory]) // only one allowed? not yet in validation
  case class Metadata(available: DateTime, // only one allowed? not yet in validation
                      alternatives: List[String],
                      publishers: List[String],
                      types: List[String],
                      formats: List[String],
                      identifiers: List[String],
                      sources: List[String],
                      languages: List[String],
                      spatials: List[String],
                      rightsholder: List[String],
                      relations: List[Relation],
                      contributers: List[Contributer],
                      subjects: List[Subject],
                      spatialPoints: List[SpatialPoint],
                      spatialBoxes: List[SpatialBox],
                      temporal: List[Temporal])
  case class AudioVideo(springfield: Springfield,
                        accessibility: FileAccessRights.Value,
                        avFiles: List[AVFile])

  case class Dataset(id: DatasetID,
                     row: String,
                     depositorID: String,
                     profile: Profile,
                     metadata: Metadata,
                     audioVideo: AudioVideo)

  type MultiDepositKey = String
  type DatasetID = String

  type DatasetRow = Map[MultiDepositKey, String]
  type DatasetRowView = Seq[DatasetRow]
  type DatasetsRowView = Map[DatasetID, DatasetRowView]

  type DatasetColumn = Seq[String]
  type DatasetColumnView = Map[MultiDepositKey, DatasetColumn]
  type DatasetsColumnView = Map[DatasetID, DatasetColumnView]

  def read(file: File): Try[mutable.Buffer[List[String]]] = {
    managed(CSVParser.parse(file, encoding, CSVFormat.RFC4180))
      .map(parse)
      .tried
      .map(data => ("ROW" :: data.head) +=: data.tail.zipWithIndex.map { case (row, index) => (index + 2).toString :: row })
  }

  def parse(parser: CSVParser): mutable.Buffer[List[String]] = {
    for {
      record <- parser.getRecords.asScala
      if record.size() > 0
      if !record.get(0).isBlank
    } yield record.asScala.toList.map(_.trim)
  }

  val file = new File("src/test/resources/allfields/input/instructions.csv")
  val r = read(file).get

  val (hs: mutable.Buffer[List[MultiDepositKey]], data: mutable.Buffer[List[String]]) = r.splitAt(1)
  val headers = hs.head
//  println(headers)

  val dsIndex = headers.indexOf("DATASET")
  val groupedData: Map[DatasetID, mutable.Buffer[List[String]]] = data.groupBy(row => row(dsIndex))
//  println(groupedData.mkString("\n"))

  val rowView: DatasetsRowView = groupedData.mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))
//  for ((datasetId, datasetData) <- rowView) {
//    println(datasetId)
//    for (row <- datasetData) {
//      println(row.mkString("\n"))
//      println("--------------------------")
//    }
//    println("==========================")
//  }

  val columnView: DatasetsColumnView = groupedData.mapValues(data =>
    headers.zipWithIndex
      .map { case (header, index) => header -> data.map(_(index)) }
      .toMap)
  for ((datasetId, datasetcolumns) <- columnView) {
    println(datasetId)
    for ((key, column) <- datasetcolumns) {
      println(s"$key -> ${column.mkString("[", ", ", "]")}")
    }
    println("==========================")
  }

  for ((datasetId, datasetColumns) <- columnView) {
    val depositorId = datasetColumns
  }
}
