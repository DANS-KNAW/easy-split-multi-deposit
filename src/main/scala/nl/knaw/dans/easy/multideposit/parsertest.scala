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

  val settings = Settings(multidepositDir = new File("src/test/resources/allfields/input/"))

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

  case class Springfield(user: String, collection: String, domain: String = "dans")
  case class Subtitles(file: File, language: Option[String])
  case class AVFile(file: File, title: Option[String], subtitles: Option[Subtitles])

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
                        accessibility: Option[FileAccessRights.Value],
                        avFiles: Map[File, List[AVFile]])

  case class Dataset(id: DatasetID,
                     row: Int,
                     depositorID: String,
                     profile: Profile,
                     metadata: Metadata,
                     audioVideo: Option[AudioVideo])

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
    val rowNum: Int = rows.map(row => row("ROW")).collect { case s if !s.isBlank => s.toInt }.min

    val depositorId: Try[String] = extractRow(rows, rowNum, "DEPOSITOR_ID")
      .flatMap(depositorIds => {
        val uniqueDepositorIds = depositorIds.toSet

        if (uniqueDepositorIds.size > 1)
          Failure(ActionException(rowNum, s"""There are multiple distinct depositorIDs in dataset "$datasetId": $uniqueDepositorIds""".stripMargin))
        else
          Success(uniqueDepositorIds.head)
        // TODO depositorIsActive in preconditions of AddPropertiesToDeposit
      })

    val profile: Try[Profile] = extractProfile(rows, rowNum)

    val metadata: Try[Metadata] = extractMetadata(rows, rowNum)

    val audioVideo: Try[Option[AudioVideo]] = extractAudioVideo(rows, rowNum)

    val dataset: Try[Dataset] = Try { (Dataset(_, _, _, _, _, _)).curried }
      .map(_(datasetId))
      .map(_(rowNum))
      .combine(depositorId)
      .combine(profile)
      .combine(metadata)
      .combine(audioVideo)
    println(s"dataset: $dataset")
  }

  def extractRow(view: DatasetRowView, row: Int, name: String): Try[NonEmptyList[String]] = {
    val res = view.flatMap(_.find(name))
    if (res.isEmpty)
      Failure(ActionException(row, s"There should be at least one non-empty value for $name"))
    else
      Success(res.toList)
  }

  def extractNEL[T](view: DatasetRowView, rowNum: Int)(f: Int => DatasetRow => Option[Try[T]]): Try[NonEmptyList[T]] = {
    view.flatMap(f(rowNum)(_)).collectResults.map(_.toList)
  }

  def extractNEL(rows: DatasetRowView, rowNum: Int, name: String): Try[NonEmptyList[String]] = {
    rows.flatMap(_.find(name)) match {
      case Seq() => Failure(ActionException(rowNum, s"There should be at least one non-empty value for $name"))
      case xs => Success(xs.toList)
    }
  }

  def extractList[T](view: DatasetRowView, rowNum: Int)(f: Int => DatasetRow => Option[Try[T]]): Try[List[T]] = {
    view.flatMap(f(rowNum)(_)).collectResults.map(_.toList)
  }

  def extractList(rows: DatasetRowView, rowNum: Int, name: String): List[String] = {
    rows.flatMap(_.find(name)).toList
  }

  def creator(rowNum: Int)(row: DatasetRow): Option[Try[Creator]] = {
    val titles = row.find("DCX_CREATOR_TITLES")
    val initials = row.find("DCX_CREATOR_INITIALS")
    val insertions = row.find("DCX_CREATOR_INSERTIONS")
    val surname = row.find("DCX_CREATOR_SURNAME")
    val organization = row.find("DCX_CREATOR_ORGANIZATION")
    val dai = row.find("DCX_CREATOR_DAI")

    (titles, initials, insertions, surname, organization, dai) match {
      case (None, None, None, None, None, None) => None
      case (None, None, None, None, Some(org), None) => Some(Success(CreatorOrganization(org)))
      case (_, Some(init), _, Some(sur), _, _) => Some(Success(CreatorPerson(titles, init, insertions, sur, organization, dai)))
      case (_, Some(_), _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_SURNAME")))
      case (_, None, _, Some(_), _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_INITIALS")))
      case (_, None, _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing values for: [DCX_CREATOR_INITIALS, DCX_CREATOR_SURNAME]")))
    }
  }

  def extractProfile(rows: DatasetRowView, rowNum: Int): Try[Profile] = {
    val dateCreated = extractNEL(rows, rowNum, "DDM_CREATED").flatMap {
      case date :: Nil => Try { DateTime.parse(date) }.recoverWith {
        case _: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_CREATED value '$date' does not represent a date"))
      }
      case _ :: _ => Failure(ActionException(rowNum, "More than one value is defined for DDM_CREATED"))
    }
    val dateAvailable = rows.flatMap(_.find("DDM_AVAILABLE")) match {
      case Seq() => Success(None)
      case Seq(date) => Try { Some(DateTime.parse(date)) }.recoverWith {
        case _: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_AVAILABLE value '$date' does not represent a date"))
      }
      case Seq(_, _@_*) => Failure(ActionException(rowNum, "More than one value is defined for DDM_AVAILABLE"))
    }
    // TODO validate DDM_AUDIENCE and DDM_ACCESSRIGHTS as in AddDatasetMetadataToDeposit.checkAccessRights
    val accessRight = extractNEL(rows, rowNum, "DDM_ACCESSRIGHTS").flatMap {
      case acc :: Nil => Try { AccessCategory.valueOf(acc) }
        .recoverWith {
          case _: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_ACCESSRIGHTS value '$acc' does not represent an accessright"))
        }
      case _ :: _ => Failure(ActionException(rowNum, "More than one value is defined for DDM_ACCESSRIGHTS"))
    }

    Try((Profile(_, _, _, _, _, _, _)).curried)
      .combine(extractNEL(rows, rowNum, "DC_TITLE"))
      .combine(extractNEL(rows, rowNum, "DC_DESCRIPTION"))
      .combine(extractNEL(rows, rowNum)(creator))
      .combine(dateCreated)
      .combine(dateAvailable.map(_.getOrElse(DateTime.now())))
      .combine(extractNEL(rows, rowNum, "DDM_AUDIENCE"))
      .combine(accessRight)
  }

  def relation(rowNum: Int)(row: DatasetRow): Option[Try[Relation]] = {
    val qualifier = row.find("DCX_RELATION_QUALIFIER")
    val link = row.find("DCX_RELATION_LINK")
    val title = row.find("DCX_RELATION_TITLE")

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

  // TODO partial duplicate of creators
  def contributor(rowNum: Int)(row: DatasetRow): Option[Try[Contributor]] = {
    val titles = row.find("DCX_CONTRIBUTOR_TITLES")
    val initials = row.find("DCX_CONTRIBUTOR_INITIALS")
    val insertions = row.find("DCX_CONTRIBUTOR_INSERTIONS")
    val surname = row.find("DCX_CONTRIBUTOR_SURNAME")
    val organization = row.find("DCX_CONTRIBUTOR_ORGANIZATION")
    val dai = row.find("DCX_CONTRIBUTOR_DAI")

    (titles, initials, insertions, surname, organization, dai) match {
      case (None, None, None, None, None, None) => None
      case (None, None, None, None, Some(org), None) => Some(Success(ContributorOrganization(org)))
      case (_, Some(init), _, Some(sur), _, _) => Some(Success(ContributorPerson(titles, init, insertions, sur, organization, dai)))
      case (_, Some(_), _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_SURNAME")))
      case (_, None, _, Some(_), _, _) => Some(Failure(ActionException(rowNum, s"Missing value for: DCX_CREATOR_INITIALS")))
      case (_, None, _, None, _, _) => Some(Failure(ActionException(rowNum, s"Missing values for: [DCX_CREATOR_INITIALS, DCX_CREATOR_SURNAME]")))
    }
  }

  def subject(rowNum: Int)(row: DatasetRow): Option[Try[Subject]] = {
    val subject = row.find("DC_SUBJECT")
    val scheme = row.find("DC_SUBJECT_SCHEME")

    (subject, scheme) match {
      case (Some(sub), Some(sch)) if sch == "abr:ABRcomplex" => Some(Success(Subject(Some(sch), sub)))
      case (Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'")))
      case (Some(sub), None) => Some(Success(Subject(None, sub)))
      case (None, sch@Some(_)) => Some(Success(Subject(sch)))
      case (None, None) => None
    }
  }

  def spatialPoint(rowNum: Int)(row: DatasetRow): Option[Try[SpatialPoint]] = {
    val maybeX = row.find("DCX_SPATIAL_X")
    val maybeY = row.find("DCX_SPATIAL_Y")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (maybeX, maybeY, maybeScheme) match {
      case (Some(x), Some(y), scheme) => Some(Success(SpatialPoint(x, y, scheme)))
      case (None, None, _) => None
      case _ => Some(Failure(ActionException(rowNum, "In a spatial point both DCX_SPATIAL_X and DCX_SPATIAL_Y should be filled in per row")))
    }
  }

  def spatialBox(rowNum: Int)(row: DatasetRow): Option[Try[SpatialBox]] = {
    val west = row.find("DCX_SPATIAL_WEST")
    val east = row.find("DCX_SPATIAL_EAST")
    val south = row.find("DCX_SPATIAL_SOUTH")
    val north = row.find("DCX_SPATIAL_NORTH")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (west, east, south, north, maybeScheme) match {
      case (Some(w), Some(e), Some(s), Some(n), scheme) => Some(Success(SpatialBox(n, s, e, w, scheme)))
      case (None, None, None, None, _) => None
      case _ => Some(Failure(ActionException(rowNum, "In a spatial box all of DCX_SPATIAL_WEST, DCX_SPATIAL_EAST, DCX_SPATIAL_NORTH and DCX_SPATIAL_WEST should be filled in per row")))
    }
  }

  def temporal(rowNum: Int)(row: DatasetRow): Option[Try[Temporal]] = {
    val subject = row.find("DCT_TEMPORAL")
    val scheme = row.find("DCT_TEMPORAL_SCHEME")

    (subject, scheme) match {
      case (Some(sub), Some(sch)) if sch == "abr:ABRperiode" => Some(Success(Temporal(Some(sch), sub)))
      case (Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'")))
      case (Some(sub), None) => Some(Success(Temporal(None, sub)))
      case (None, sch@Some(_)) => Some(Success(Temporal(sch)))
      case (None, None) => None
    }
  }

  def extractMetadata(rows: DatasetRowView, rowNum: Int): Try[Metadata] = {
    Try { (Metadata(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)).curried }
      .map(_(extractList(rows, rowNum, "DCT_ALTERNATIVE")))
      .map(_(extractList(rows, rowNum, "DC_PUBLISHER")))
      .map(_(extractList(rows, rowNum, "DC_TYPE")))
      .map(_(extractList(rows, rowNum, "DC_FORMAT")))
      .map(_(extractList(rows, rowNum, "DC_IDENTIFIER")))
      .map(_(extractList(rows, rowNum, "DC_SOURCE")))
      .map(_(extractList(rows, rowNum, "DC_LANGUAGE")))
      .map(_(extractList(rows, rowNum, "DCT_SPATIAL")))
      .map(_(extractList(rows, rowNum, "DCT_RIGHTSHOLDER")))
      .combine(extractList(rows, rowNum)(relation))
      .combine(extractList(rows, rowNum)(contributor))
      .combine(extractList(rows, rowNum)(subject))
      .combine(extractList(rows, rowNum)(spatialPoint))
      .combine(extractList(rows, rowNum)(spatialBox))
      .combine(extractList(rows, rowNum)(temporal))
  }

  // TODO if A/V files are in the dataset, Springfield must be defined
  def springfield(rowNum: Int)(row: DatasetRow): Option[Try[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")

    (domain, user, collection) match {
      case (Some(d), Some(u), Some(c)) => Some(Success(Springfield(u, c, d)))
      case (None, Some(u), Some(c)) => Some(Success(Springfield(u, c)))
      case (_, Some(_), None) => Some(Failure(ActionException(rowNum, "Missing value for: SF_COLLECTION")))
      case (_, None, Some(_)) => Some(Failure(ActionException(rowNum, "Missing value for: SF_USER")))
      case (_, None, None) => None
    }
  }

  def avFile(rowNum: Int)(row: DatasetRow): Option[Try[AVFile]] = {
    val file = row.find("AV_FILE").map(new File(settings.multidepositDir, _))
    val title = row.find("AV_FILE_TITLE")
    val subtitle = row.find("AV_SUBTITLES").map(new File(settings.multidepositDir, _))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, title, subtitle, subtitleLang) match {
      case (Some(p), t, Some(sub), subLang) if p.exists() && sub.exists() => Some(Success(AVFile(p, t, Some(Subtitles(sub, subLang)))))
      case (Some(p), _, Some(_), _) if !p.exists() => Some(Failure(ActionException(rowNum, s"AV_FILE '$p' does not exist")))
      case (Some(_), _, Some(sub), _) if !sub.exists() => Some(Failure(ActionException(rowNum, s"AV_SUBTITLES '$sub' does not exist")))
      case (Some(_), _, None, Some(_)) => Some(Failure(ActionException(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subtitleLang'")))
      case (Some(p), t, None, None) => Some(Success(AVFile(p, t, None)))
      case (None, None, None, None) => None
      case (None, _, _, _) => Some(Failure(ActionException(rowNum, "No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined")))
    }
  }

  def extractAudioVideo(rows: DatasetRowView, rowNum: Int): Try[Option[AudioVideo]] = {
    val maybeSpringfield = extractList(rows, rowNum)(springfield).flatMap {
      case Nil => Success(None)
      case sf :: Nil => Success(Some(sf))
      case _ => Failure(ActionException(rowNum, "")) // TODO error message
    }
    val maybeFileAccessRight: Try[Option[FileAccessRights.Value]] = extractList(rows, rowNum, "SF_ACCESSIBILITY") match {
      case Nil => Success(None)
      case acc :: Nil => Try { Some(FileAccessRights.valueOf(acc).getOrElse { throw ActionException(rowNum, s"Value '$acc' is not a valid file access right") }) }
      case _ :: _ => Failure(ActionException(rowNum, "More than one value is defined for SF_ACCESSIBILITY"))
    }
    val avFiles: Try[Map[File, List[AVFile]]] = extractList(rows, rowNum)(avFile).map(_.groupBy(_.file))

    Try {
      ((springf: Option[Springfield], acc: Option[FileAccessRights.Value], avFiles: Map[File, List[AVFile]]) => {
        (springf, acc, avFiles) match {
          case (Some(s), a, fs) => Success(Some(AudioVideo(s, a, fs)))
          case (None, _, fs) if fs.isEmpty => Success(None)
          case (None, _, _) => Failure(ActionException(rowNum, "The column 'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not"))
        }
      }).curried
    }
      .combine(maybeSpringfield)
      .combine(maybeFileAccessRight)
      .combine(avFiles)
      .flatten
  }
}
