package nl.knaw.dans.easy.multideposit.parser

import java.io.File

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.actions.FileAccessRights
import nl.knaw.dans.easy.multideposit.{ ActionException, Settings, StringExtensions, encoding }
import nl.knaw.dans.lib.error._
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.joda.time.DateTime
import resource._

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

object MultiDepositParser extends App {

  val settings = Settings(multidepositDir = new File("src/test/resources/allfields/input/"))
  val instructionsFile: File = new File("src/test/resources/allfields/input/instructions.csv")
  parse(instructionsFile).foreach(_.foreach(println))

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

  def parse(file: File): Try[Seq[Dataset]] = {
    for {
      (headers, content) <- read(file)
      result <- content.groupBy(_ (headers.indexOf("DATASET")))
        .mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))
        .map((extractDataset _).tupled)
        .toSeq
        .collectResults
    } yield result
  }

  def getRowNum(row: DatasetRow): Int = row("ROW").toInt

  def extractNEL[T](rows: DatasetRows)(f: (=> Int) => DatasetRow => Option[Try[T]]): Try[NonEmptyList[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).collectResults.map(_.toList)
  }

  def extractNEL(rows: DatasetRows, rowNum: Int, name: String): Try[NonEmptyList[String]] = {
    rows.flatMap(_.find(name)) match {
      case Seq() => Failure(ActionException(rowNum, s"There should be at least one non-empty value for $name"))
      case xs => Success(xs.toList)
    }
  }

  def extractList[T](rows: DatasetRows)(f: (=> Int) => DatasetRow => Option[Try[T]]): Try[List[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).collectResults.map(_.toList)
  }

  def extractList(rows: DatasetRows, name: String): List[String] = {
    rows.flatMap(_.find(name)).toList
  }

  def atMostOne[T](rowNum: => Int, columnNames: String*)(values: List[T]): Try[Option[T]] = {
    values match {
      case Nil => Success(None)
      case t :: Nil => Success(Some(t))
      case _ if columnNames.size == 1 => Failure(ActionException(rowNum, "Only one row is allowed " +
        s"to contain a value for the column: '${ columnNames.head }'"))
      case _ => Failure(ActionException(rowNum, "Only one row is allowed to contain a value for " +
        s"these columns: ${ columnNames.mkString("[", ", ", "]") }"))
    }
  }

  def exactlyOne[T](rowNum: => Int, columnNames: String*)(values: List[T]): Try[T] = {
    values match {
      case Nil if columnNames.size == 1 => Failure(ActionException(rowNum, "One row has to contain " +
        s"a value for the column: '${ columnNames.head }'"))
      case Nil => Failure(ActionException(rowNum, "One row has to contain a value for these " +
        s"columns: ${ columnNames.mkString("[", ", ", "]") }"))
      case t :: Nil => Success(t)
      case _ if columnNames.size == 1 => Failure(ActionException(rowNum, "Only one row is allowed " +
        s"to contain a value for the column: '${ columnNames.head }'"))
      case _ => Failure(ActionException(rowNum, "Only one row is allowed to contain a value for " +
        s"these columns: ${ columnNames.mkString("[", ", ", "]") }"))
    }
  }

  def extractDataset(datasetId: DatasetID, rows: DatasetRows): Try[Dataset] = {
    val rowNum = rows.map(getRowNum).min

    // TODO depositorIsActive in preconditions of AddPropertiesToDeposit
    val depositorId: Try[String] = extractNEL(rows, rowNum, "DEPOSITOR_ID")
      .flatMap {
        case depositorIds if depositorIds.toSet.size > 1 =>
          Failure(ActionException(rowNum, "There are multiple distinct depositorIDs in dataset " +
            s"'$datasetId': ${ depositorIds.toSet.mkString("[", ", ", "]") }"))
        case depId :: _ => Success(depId)
      }

    Try { (Dataset(_, _, _, _, _, _)).curried }
      .map(_ (datasetId))
      .map(_ (rowNum))
      .combine(depositorId)
      .combine(extractProfile(rows, rowNum))
      .combine(extractMetadata(rows))
      .combine(extractAudioVideo(rows, rowNum))
  }

  def extractProfile(rows: DatasetRows, rowNum: Int): Try[Profile] = {
    // TODO validate DDM_AUDIENCE and DDM_ACCESSRIGHTS as in AddDatasetMetadataToDeposit.checkAccessRights
    Try((Profile(_, _, _, _, _, _, _)).curried)
      .combine(extractNEL(rows, rowNum, "DC_TITLE"))
      .combine(extractNEL(rows, rowNum, "DC_DESCRIPTION"))
      .combine(extractNEL(rows)(creator))
      .combine(extractList(rows)(date("DDM_CREATED")).flatMap(exactlyOne(rowNum, "DDM_CREATED")))
      .combine(extractList(rows)(date("DDM_AVAILABLE")).flatMap(atMostOne(rowNum, "DDM_AVAILABLE")).map(_.getOrElse(DateTime.now())))
      .combine(extractNEL(rows, rowNum, "DDM_AUDIENCE"))
      .combine(extractList(rows)(accessCategory).flatMap(exactlyOne(rowNum, "DDM_ACCESSRIGHTS")))
  }

  def extractMetadata(rows: DatasetRows): Try[Metadata] = {
    Try { (Metadata(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)).curried }
      .map(_ (extractList(rows, "DCT_ALTERNATIVE")))
      .map(_ (extractList(rows, "DC_PUBLISHER")))
      .map(_ (extractList(rows, "DC_TYPE")))
      .map(_ (extractList(rows, "DC_FORMAT")))
      .map(_ (extractList(rows, "DC_IDENTIFIER")))
      .map(_ (extractList(rows, "DC_SOURCE")))
      .map(_ (extractList(rows, "DC_LANGUAGE")))
      .map(_ (extractList(rows, "DCT_SPATIAL")))
      .map(_ (extractList(rows, "DCT_RIGHTSHOLDER")))
      .combine(extractList(rows)(relation))
      .combine(extractList(rows)(contributor))
      .combine(extractList(rows)(subject))
      .combine(extractList(rows)(spatialPoint))
      .combine(extractList(rows)(spatialBox))
      .combine(extractList(rows)(temporal))
  }

  def extractAudioVideo(rows: DatasetRows, rowNum: Int): Try[Option[AudioVideo]] = {
    Try {
      ((springf: Option[Springfield], acc: Option[FileAccessRights.Value], avFiles: Map[File, List[AVFile]]) => {
        (springf, acc, avFiles) match {
          case (Some(s), a, fs) => Success(Some(AudioVideo(s, a, fs)))
          case (None, _, fs) if fs.isEmpty => Success(None)
          case (None, _, _) => Failure(ActionException(rowNum, "The column 'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not"))
        }
      }).curried
    }
      .combine(extractList(rows)(springfield).flatMap(atMostOne(rowNum, "SF_DOMAIN", "SF_USER", "SF_COLLECTION")))
      .combine(extractList(rows)(fileAccessRight).flatMap(atMostOne(rowNum, "SF_ACCESSIBILITY")))
      .combine(extractList(rows)(avFile).map(_.groupBy(_.file)))
      .flatten
  }

  def date(columnName: String)(rowNum: => Int)(row: DatasetRow): Option[Try[DateTime]] = {
    row.find(columnName)
      .map(date => Try { DateTime.parse(date) }.recoverWith {
        case e: IllegalArgumentException => Failure(ActionException(rowNum, s"$columnName value '$date' does not represent a date", e))
      })
  }

  def accessCategory(rowNum: => Int)(row: DatasetRow): Option[Try[AccessCategory]] = {
    row.find("DDM_ACCESSRIGHTS")
      .map(acc => Try { AccessCategory.valueOf(acc) }
        .recoverWith {
          case e: IllegalArgumentException => Failure(ActionException(rowNum, s"DDM_ACCESSRIGHTS value '$acc' does not represent an accessright", e))
        })
  }

  def creator(rowNum: => Int)(row: DatasetRow): Option[Try[Creator]] = {
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

  // TODO partial duplicate of creators
  def contributor(rowNum: => Int)(row: DatasetRow): Option[Try[Contributor]] = {
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

  def relation(rowNum: => Int)(row: DatasetRow): Option[Try[Relation]] = {
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

  def subject(rowNum: => Int)(row: DatasetRow): Option[Try[Subject]] = {
    val subject = row.find("DC_SUBJECT")
    val scheme = row.find("DC_SUBJECT_SCHEME")

    (subject, scheme) match {
      case (Some(sub), Some(sch)) if sch == "abr:ABRcomplex" => Some(Success(Subject(Some(sch), sub)))
      case (Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'")))
      case (Some(sub), None) => Some(Success(Subject(None, sub)))
      case (None, sch @ Some(_)) => Some(Success(Subject(sch)))
      case (None, None) => None
    }
  }

  def temporal(rowNum: => Int)(row: DatasetRow): Option[Try[Temporal]] = {
    val subject = row.find("DCT_TEMPORAL")
    val scheme = row.find("DCT_TEMPORAL_SCHEME")

    (subject, scheme) match {
      case (Some(sub), Some(sch)) if sch == "abr:ABRperiode" => Some(Success(Temporal(Some(sch), sub)))
      case (Some(_), Some(_)) => Some(Failure(ActionException(rowNum, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'")))
      case (Some(sub), None) => Some(Success(Temporal(None, sub)))
      case (None, sch @ Some(_)) => Some(Success(Temporal(sch)))
      case (None, None) => None
    }
  }

  def spatialPoint(rowNum: => Int)(row: DatasetRow): Option[Try[SpatialPoint]] = {
    val maybeX = row.find("DCX_SPATIAL_X")
    val maybeY = row.find("DCX_SPATIAL_Y")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (maybeX, maybeY, maybeScheme) match {
      case (Some(x), Some(y), scheme) => Some(Success(SpatialPoint(x, y, scheme)))
      case (None, None, _) => None
      case _ => Some(Failure(ActionException(rowNum, "In a spatial point both DCX_SPATIAL_X and DCX_SPATIAL_Y should be filled in per row")))
    }
  }

  def spatialBox(rowNum: => Int)(row: DatasetRow): Option[Try[SpatialBox]] = {
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

  // TODO if A/V files are in the dataset, Springfield must be defined
  def springfield(rowNum: => Int)(row: DatasetRow): Option[Try[Springfield]] = {
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

  def avFile(rowNum: => Int)(row: DatasetRow): Option[Try[AVFile]] = {
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

  def fileAccessRight(rowNum: => Int)(row: DatasetRow): Option[Try[FileAccessRights.Value]] = {
    row.find("SF_ACCESSIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ActionException(rowNum, s"Value '$acc' is not a valid file access right"))))
  }
}
