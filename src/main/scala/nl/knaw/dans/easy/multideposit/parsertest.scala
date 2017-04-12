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

  sealed abstract class Contributor
  case class ContributorOrganization(organization: String) extends Contributor
  case class ContributorPerson(titles: Option[String],
                               initials: String,
                               insertions: Option[String],
                               surname: String,
                               organization: Option[String],
                               dai: Option[String]) extends Contributor

  sealed abstract class Relation
  case class QualifiedLinkRelation(qualifier: String, link: String) extends Relation
  case class QualifiedTitleRelation(qualifier: String, title: String) extends Relation
  case class LinkRelation(link: String) extends Relation
  case class TitleRelation(title: String) extends Relation

  case class Subject(scheme: Option[String], subject: String = "")
  case class Temporal(scheme: Option[String], temporal: String = "")

  case class SpatialPoint(x: String, y: String, scheme: Option[String])
  case class SpatialBox(north: String, south: String, east: String, west: String, scheme: Option[String])

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
                      contributors: List[Contributor],
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

    val metadata = extractMetadata(rows, rowNum)
    println(s"  metadata: $metadata")
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

  def extractRelations(view: DatasetRowView, rowNum: Int): Try[List[Relation]] = {
    def extractRelation(row: DatasetRow): Option[Try[Relation]] = {
      val qualifier = row.get("DCX_RELATION_QUALIFIER").filterNot(_.isBlank)
      val link = row.get("DCX_RELATION_LINK").filterNot(_.isBlank)
      val title = row.get("DCX_RELATION_TITLE").filterNot(_.isBlank)

      (qualifier, link, title) match {
        case (Some(_), Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "Only one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] can be filled in per row")))
        case (Some(q), Some(l), None) => Some(Success(QualifiedLinkRelation(q, l)))
        case (Some(q), None, Some(t)) => Some(Success(QualifiedTitleRelation(q, t)))
        case (Some(_), None, None) => Some(Failure(ActionException(rowNum, "At least one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be filled in per row")))
        case (None, Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "Only one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] can be filled in per row")))
        case (None, Some(l), None) => Some(Success(LinkRelation(l)))
        case (None, None, Some(t)) => Some(Success(TitleRelation(t)))
        case (None, None, None) => None
      }
    }

    view.flatMap(extractRelation).collectResults.map(_.toList)
  }

  // TODO partial duplicate of extractCreators
  def extractContributors(view: DatasetRowView, rowNum: Int): Try[List[Contributor]] = {
    def extractContributor(row: DatasetRow): Option[Try[Contributor]] = {
      val titles = row.get("DCX_CONTRIBUTOR_TITLES").filterNot(_.isBlank)
      val initials = row.get("DCX_CONTRIBUTOR_INITIALS").filterNot(_.isBlank)
      val insertions = row.get("DCX_CONTRIBUTOR_INSERTIONS").filterNot(_.isBlank)
      val surname = row.get("DCX_CONTRIBUTOR_SURNAME").filterNot(_.isBlank)
      val organization = row.get("DCX_CONTRIBUTOR_ORGANIZATION").filterNot(_.isBlank)
      val dai = row.get("DCX_CONTRIBUTOR_DAI").filterNot(_.isBlank)

      (titles, initials, insertions, surname, organization, dai) match {
        case (None, None, None, None, None, None) => None
        case (None, None, None, None, Some(org), None) => Some(Success(ContributorOrganization(org)))
        case (_, Some(init), _, Some(sur), _, _) => Some(Success(ContributorPerson(titles, init, insertions, sur, organization, dai)))
        case (_, Some(_), _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_SURNAME")))
        case (_, None, _, Some(_), _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_INITIALS")))
        case (_, None, _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing values for: [DCX_CREATOR_INITIALS, DCX_CREATOR_SURNAME]")))
      }
    }

    view.flatMap(extractContributor).collectResults.map(_.toList)
  }

  def extractSubjects(view: DatasetRowView, rowNum: Int): Try[List[Subject]] = {
    def extractSubject(row: DatasetRow): Option[Try[Subject]] = {
      val subject = row.get("DC_SUBJECT").filterNot(_.isBlank)
      val scheme = row.get("DC_SUBJECT_SCHEME").filterNot(_.isBlank)

      (subject, scheme) match {
        case (Some(sub), Some(sch)) if sch == "abr:ABRcomplex" => Some(Success(Subject(Some(sch), sub)))
        case (Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'")))
        case (Some(sub), None) => Some(Success(Subject(None, sub)))
        case (None, sch@Some(_)) => Some(Success(Subject(sch)))
        case (None, None) => None
      }
    }

    view.flatMap(extractSubject).collectResults.map(_.toList)
  }

  def extractSpatialPoints(view: DatasetRowView, rowNum: Int): Try[List[SpatialPoint]] = {
    def extractSpatialPoint(row: DatasetRow): Option[Try[SpatialPoint]] = {
      val maybeX = row.get("DCX_SPATIAL_X").filterNot(_.isBlank)
      val maybeY = row.get("DCX_SPATIAL_Y").filterNot(_.isBlank)
      val maybeScheme = row.get("DCX_SPATIAL_SCHEME").filterNot(_.isBlank)

      (maybeX, maybeY, maybeScheme) match {
        case (Some(x), Some(y), scheme) => Some(Success(SpatialPoint(x, y, scheme)))
        case (None, None, None) => None
        case _ => Some(Failure(ActionException(rowNum, "In a spatial point both DCX_SPATIAL_X and DCX_SPATIAL_Y should be filled in per row")))
      }
    }

    view.flatMap(extractSpatialPoint).collectResults.map(_.toList)
  }

  def extractSpatialBoxes(view: DatasetRowView, rowNum: Int): Try[List[SpatialBox]] = {
    def extractSpatialBox(row: DatasetRow): Option[Try[SpatialBox]] = {
      val west = row.get("DCX_SPATIAL_WEST").filterNot(_.isBlank)
      val east = row.get("DCX_SPATIAL_EAST").filterNot(_.isBlank)
      val south = row.get("DCX_SPATIAL_SOUTH").filterNot(_.isBlank)
      val north = row.get("DCX_SPATIAL_NORTH").filterNot(_.isBlank)
      val maybeScheme = row.get("DCX_SPATIAL_SCHEME").filterNot(_.isBlank)

      (west, east, south, north, maybeScheme) match {
        case (Some(w), Some(e), Some(s), Some(n), scheme) => Some(Success(SpatialBox(n, s, e, w, scheme)))
        case (None, None, None, None, None) => None
        case _ => Some(Failure(ActionException(rowNum, "In a spatial box all of DCX_SPATIAL_WEST, DCX_SPATIAL_EAST, DCX_SPATIAL_NORTH and DCX_SPATIAL_WEST should be filled in per row")))
      }
    }

    view.flatMap(extractSpatialBox).collectResults.map(_.toList)
  }

  def extractTemporals(view: DatasetRowView, rowNum: Int): Try[List[Temporal]] = {
    def extractTemporal(row: DatasetRow): Option[Try[Temporal]] = {
      val subject = row.get("DC_SUBJECT").filterNot(_.isBlank)
      val scheme = row.get("DCT_TEMPORAL_SCHEME").filterNot(_.isBlank)

      (subject, scheme) match {
        case (Some(sub), Some(sch)) if sch == "abr:ABRperiode" => Some(Success(Temporal(Some(sch), sub)))
        case (Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'")))
        case (Some(sub), None) => Some(Success(Temporal(None, sub)))
        case (None, sch@Some(_)) => Some(Success(Temporal(sch)))
        case (None, None) => None
      }
    }

    view.flatMap(extractTemporal).collectResults.map(_.toList)
  }

  def extractMetadata(rows: DatasetRowView, rowNum: Int): Try[Metadata] = {
    def extract(name: String): List[String] = {
      rows.flatMap(_.get(name).filterNot(_.isBlank)).toList
    }

    val alternatives: List[String] = extract("DCT_ALTERNATIVE")
    val publishers: List[String] = extract("DC_PUBLISHER")
    val types: List[String] = extract("DC_TYPE")
    val formats: List[String] = extract("DC_FORMAT")
    val identifiers: List[String] = extract("DC_IDENTIFIER")
    val sources: List[String] = extract("DC_SOURCE")
    val languages: List[String] = extract("DC_LANGUAGE")
    val spatials: List[String] = extract("DCT_SPATIAL")
    val rightsholders: List[String] = extract("DCT_RIGHTSHOLDER")
    val relations: Try[List[Relation]] = extractRelations(rows, rowNum)
    val contributors: Try[List[Contributor]] = extractContributors(rows, rowNum)
    val subjects: Try[List[Subject]] = extractSubjects(rows, rowNum)
    val spatialPoints: Try[List[SpatialPoint]] = extractSpatialPoints(rows, rowNum)
    val spatialBoxes: Try[List[SpatialBox]] = extractSpatialBoxes(rows, rowNum)
    val temporals: Try[List[Temporal]] = extractTemporals(rows, rowNum)

    Try { (Metadata(alternatives, publishers, types, formats, identifiers, sources, languages, spatials, rightsholders, _, _, _, _, _, _)).curried }
      .combine(relations)
      .combine(contributors)
      .combine(subjects)
      .combine(spatialPoints)
      .combine(spatialBoxes)
      .combine(temporals)
  }
}
