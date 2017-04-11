package nl.knaw.dans.easy.multideposit

import java.io.File

import nl.knaw.dans.lib.error._
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.actions.FileAccessRights
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.joda.time.DateTime
import resource._

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

object parsertest extends App {

  // inspired by http://stackoverflow.com/questions/28223692/what-is-the-optimal-way-not-using-scalaz-to-type-require-a-non-empty-list
  type NonEmptyList[A] = ::[A]
  implicit def listToNEL[A](list: List[A]): ::[A] = {
    assert(list.nonEmpty)
    ::(list.head, list.tail)
  }

  sealed abstract class Creator
  case class CreatorOrganization(organization: String) extends Creator
  case class CreatorPerson(titles: Option[String],
                           initials: String,
                           insertions: Option[String],
                           surname: String,
                           organization: Option[String],
                           dai: Option[String]) extends Creator

  sealed abstract class Contributer
  case class ContributerOrganization(organization: String) extends Contributer
  case class ContributerPerson(titles: Option[String],
                               initials: String,
                               insertions: Option[String],
                               surname: String,
                               organization: Option[String],
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

  case class Profile(titles: NonEmptyList[String],
                     descriptions: NonEmptyList[String],
                     creators: NonEmptyList[Creator],
                     created: DateTime,
                     available: DateTime = DateTime.now(),
                     audiences: NonEmptyList[String], // or List[enum values]?
                     accessright: AccessCategory) // only one allowed? not yet in validation
  case class Metadata(alternatives: List[String],
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
                     row: Int,
                     depositorID: String,
                     profile: Profile,
                     metadata: Metadata,
                     audioVideo: AudioVideo)

  type MultiDepositKey = String
  type DatasetID = String

  type DatasetRow = Map[MultiDepositKey, String]
  type DatasetRowView = Seq[DatasetRow]
  type DatasetsRowView = Map[DatasetID, DatasetRowView]

  def read(file: File): Try[(List[String], List[List[String]])] = {
    managed(CSVParser.parse(file, encoding, CSVFormat.RFC4180))
      .map(parse)
      .tried
      .map {
        case Nil => throw new Exception("not expected to happen")
        case head :: Nil => ("ROW" :: head, Nil)
        case head :: rows => ("ROW" :: head, rows.zipWithIndex.map { case (row, index) => (index + 2).toString :: row })
      }
  }

  def parse(parser: CSVParser): List[List[String]] = {
    for {
      record <- parser.getRecords.asScala.toList
      if record.size() > 0
      if !record.get(0).isBlank
    } yield record.asScala.toList.map(_.trim)
  }

  val instructionsFile: File = new File("src/test/resources/allfields/input/instructions.csv")
  val (headers: Seq[String], data: List[List[String]]) = read(instructionsFile).get

  val dsIndex: Int = headers.indexOf("DATASET")
  val groupedData: Map[DatasetID, List[List[String]]] = data.groupBy(row => row(dsIndex))

  val rowView: DatasetsRowView = groupedData.mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))

  for ((datasetId, rows) <- rowView.toSeq.sortBy(_._1)) {
    println(s"datasetId: $datasetId")

    val rowNum = rows.map(row => row("ROW")).collect { case s if !s.isBlank => s.toInt }.min
    println(s"  row: $rowNum")

    val depositorId: Try[String] = extractRow(rows, rowNum, "DEPOSITOR_ID")
      .flatMap(depositorIds => {
        val uniqueDepositorIds = depositorIds.toSet

        if (uniqueDepositorIds.size > 1)
          Failure(ActionException(rowNum, s"""There are multiple distinct depositorIDs in dataset "$datasetId": $uniqueDepositorIds""".stripMargin))
        else
          Success(uniqueDepositorIds.head)
      })
      // TODO depositorIsActive in preconditions of AddPropertiesToDeposit
    println(s"  depositorId: $depositorId")

    val profile: Try[Profile] = extractProfile(rows, rowNum)
    println(s"  profile: $profile")
  }

  def extractRow(view: DatasetRowView, row: Int, name: String): Try[NonEmptyList[String]] = {
    val res = view.flatMap(_.get(name).filterNot(_.isBlank))
    if (res.isEmpty)
      Failure(ActionException(row, s"There should be at least one non-empty value for $name"))
    else
      Success(res.toList)
  }

  def extractCreators(view: DatasetRowView, rowNum: Int): Try[NonEmptyList[Creator]] = {
    def extractCreator(row: DatasetRow): Option[Try[Creator]] = {
      val titles = row.get("DCX_CREATOR_TITLES").filterNot(_.isBlank)
      val initials = row.get("DCX_CREATOR_INITIALS").filterNot(_.isBlank)
      val insertions = row.get("DCX_CREATOR_INSERTIONS").filterNot(_.isBlank)
      val surname = row.get("DCX_CREATOR_SURNAME").filterNot(_.isBlank)
      val organization = row.get("DCX_CREATOR_ORGANIZATION").filterNot(_.isBlank)
      val dai = row.get("DCX_CREATOR_DAI").filterNot(_.isBlank)

      (titles, initials, insertions, surname, organization, dai) match {
        case (None, None, None, None, None, None) => None
        case (None, None, None, None, Some(org), None) => Some(Success(CreatorOrganization(org)))
        case (_, Some(init), _, Some(sur), _, _) => Some(Success(CreatorPerson(titles, init, insertions, sur, organization, dai)))
        case (_, Some(_), _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_SURNAME")))
        case (_, None, _, Some(_), _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_INITIALS")))
        case (_, None, _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing values for: [DCX_CREATOR_INITIALS, DCX_CREATOR_SURNAME]")))
      }
    }

    view.flatMap(extractCreator).collectResults.map(_.toList)
  }

  def extractProfile(rows: DatasetRowView, rowNum: Int): Try[Profile] = {
    def extract(name: String): Try[NonEmptyList[String]] = {
      rows.flatMap(_.get(name).filterNot(_.isBlank)) match {
        case Seq() => Failure(ActionException(rowNum, s"There should be at least one non-empty value for $name"))
        case xs => Success(xs.toList)
      }
    }

    val titles = extract("DC_TITLE")
    val descriptions = extract("DC_DESCRIPTION")
    val creators = extractCreators(rows, rowNum)
    val dateCreated = extract("DDM_CREATED").flatMap {
      case date :: Nil => Try { DateTime.parse(date) }.recoverWith {
        case _: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_CREATED value '$date' does not represent a date"))
      }
      case _ :: _ => Failure(ActionException(rowNum, "More than one value is defined for DDM_CREATED"))
    }
    val dateAvailable = rows.flatMap(_.get("DDM_AVAILABLE").filterNot(_.isBlank)) match {
      case Seq() => Success(None)
      case Seq(date) => Try { Some(DateTime.parse(date)) }.recoverWith {
        case _: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_AVAILABLE value '$date' does not represent a date"))
      }
      case Seq(_, _@_*) => Failure(ActionException(rowNum, "More than one value is defined for DDM_AVAILABLE"))
    }
    val audiences = extract("DDM_AUDIENCE")
    // TODO validate DDM_AUDIENCE and DDM_ACCESSRIGHTS as in AddDatasetMetadataToDeposit.checkAccessRights
    val accessRight = extract("DDM_ACCESSRIGHTS").flatMap {
      case acc :: Nil => Try { AccessCategory.valueOf(acc) }
        .recoverWith {
          case _: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_ACCESSRIGHTS value '$acc' does not represent an accessright"))
        }
      case _ :: _ => Failure(ActionException(rowNum, "More than one value is defined for DDM_ACCESSRIGHTS"))
    }

    Try((Profile(_, _, _, _, _, _, _)).curried)
      .combine(titles)
      .combine(descriptions)
      .combine(creators)
      .combine(dateCreated)
      .combine(dateAvailable.map(_.getOrElse(DateTime.now())))
      .combine(audiences)
      .combine(accessRight)
  }
}
