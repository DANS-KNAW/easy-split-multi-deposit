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
package nl.knaw.dans.easy.multideposit.parser

import java.io.File
import java.util.Locale

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.joda.time.DateTime
import resource._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class MultiDepositParser(implicit settings: Settings) extends DebugEnhancedLogging {

  def read(file: File): Try[(List[MultiDepositKey], List[List[String]])] = {
    managed(CSVParser.parse(file, encoding, CSVFormat.RFC4180))
      .map(parse)
      .tried
      .flatMap {
        case Nil => Failure(EmptyInstructionsFileException(file))
        case headers :: rows =>
          validateDatasetHeaders(headers)
            .map(_ => ("ROW" :: headers, rows.zipWithIndex.collect {
              case (row, index) if row.nonEmpty => (index + 2).toString :: row
            }))
      }
  }

  private def parse(parser: CSVParser): List[List[String]] = {
    parser.getRecords.asScala.toList
      .map(_.asScala.toList.map(_.trim))
      .map {
        case "" :: Nil => Nil // blank line
        case xs => xs
      }
  }

  private def validateDatasetHeaders(headers: List[MultiDepositKey]): Try[Unit] = {
    val validHeaders = Headers.validHeaders
    val invalidHeaders = headers.filterNot(validHeaders.contains)
    lazy val uniqueHeaders = headers.distinct

    if (invalidHeaders.nonEmpty)
      Failure(ParseException(0, "SIP Instructions file contains unknown headers: " +
        s"${ invalidHeaders.mkString("[", ", ", "]") }. Please, check for spelling errors and " +
        s"consult the documentation for the list of valid headers."))
    else if (headers.size != uniqueHeaders.size)
      Failure(ParseException(0, "SIP Instructions file contains duplicate headers: " +
        s"${ headers.diff(uniqueHeaders).mkString("[", ", ", "]") }"))
    else
      Success(())
  }

  private def recoverParsing(t: Throwable): Failure[Nothing] = {
    @tailrec
    def flattenException(es: List[Throwable], result: List[Throwable] = Nil): List[Throwable] = {
      es match {
        case Nil => result
        case CompositeException(ths) :: exs => flattenException(ths.toList ::: exs, result)
        case NonFatal(ex) :: exs => flattenException(exs, ex :: result)
      }
    }

    def generateReport(header: String = "", throwable: Throwable, footer: String = ""): String = {
      header.toOption.fold("")(_ + "\n") +
        flattenException(List(throwable))
          .sortBy {
            case ParseException(row, _, _) => row
            case _ => -1
          }
          .map {
            case ParseException(row, msg, _) => s" - row $row: $msg"
            case e => s" - unexpected: ${e.getMessage}"
          }
          .mkString("\n") +
        footer.toOption.fold("")("\n" + _)
    }

    Failure(ParserFailedException(
      report = generateReport(
        header = "CSV failures:",
        throwable = t,
        footer = "Due to these errors in the 'instructions.csv', nothing was done."),
      cause = t))
  }

  def parse(file: File): Try[Seq[Dataset]] = {
    logger.info(s"Parsing $file")
    
    val datasets = for {
      (headers, content) <- read(file)
      datasetIdIndex = headers.indexOf("DATASET")
      _ <- detectEmptyDatasetCells(content.map(_(datasetIdIndex)))
      result <- content.groupBy(_ (datasetIdIndex))
        .mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))
        .map((extractDataset _).tupled)
        .toSeq
        .collectResults
    } yield result
    
    datasets.recoverWith { case NonFatal(e) => recoverParsing(e) }
  }

  def detectEmptyDatasetCells(datasetIds: List[String]): Try[Unit] = {
    datasetIds.zipWithIndex
      .collect { case (s, i) if s.isBlank => i + 2 }
      .map(index => Failure(ParseException(index, s"Row $index does not have a datasetId in column DATASET")): Try[Unit])
      .collectResults
      .map(_ => ())
  }

  def getRowNum(row: DatasetRow): Int = row("ROW").toInt

  def extractNEL[T](rows: DatasetRows)(f: (=> Int) => DatasetRow => Option[Try[T]]): Try[NonEmptyList[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).collectResults.map(_.toList)
  }

  def extractNEL(rows: DatasetRows, rowNum: Int, name: MultiDepositKey): Try[NonEmptyList[String]] = {
    rows.flatMap(_.find(name)) match {
      case Seq() => Failure(ParseException(rowNum, s"There should be at least one non-empty value for $name"))
      case xs => Try { xs.toList }
    }
  }

  def extractList[T](rows: DatasetRows)(f: (=> Int) => DatasetRow => Option[Try[T]]): Try[List[T]] = {
    rows.flatMap(row => f(getRowNum(row))(row)).collectResults.map(_.toList)
  }

  def extractList(rows: DatasetRows, name: MultiDepositKey): List[String] = {
    rows.flatMap(_.find(name)).toList
  }

  def atMostOne[T](rowNum: => Int, columnNames: => NonEmptyList[MultiDepositKey])(values: List[T]): Try[Option[T]] = {
    values.distinct match {
      case Nil => Success(None)
      case t :: Nil => Success(Some(t))
      case ts if columnNames.size == 1 => Failure(ParseException(rowNum, "Only one row is allowed " +
        s"to contain a value for the column '${ columnNames.head }'. Found: ${ ts.mkString("[", ", ", "]") }"))
      case ts => Failure(ParseException(rowNum, "Only one row is allowed to contain a value for " +
        s"these columns: ${ columnNames.mkString("[", ", ", "]") }. Found: ${ ts.mkString("[", ", ", "]") }"))
    }
  }

  def exactlyOne[T](rowNum: => Int, columnNames: => NonEmptyList[MultiDepositKey])(values: List[T]): Try[T] = {
    values.distinct match {
      case t :: Nil => Success(t)
      case Nil if columnNames.size == 1 => Failure(ParseException(rowNum, "One row has to contain " +
        s"a value for the column: '${ columnNames.head }'"))
      case Nil => Failure(ParseException(rowNum, "One row has to contain a value for these " +
        s"columns: ${ columnNames.mkString("[", ", ", "]") }"))
      case ts if columnNames.size == 1 => Failure(ParseException(rowNum, "Only one row is allowed " +
        s"to contain a value for the column '${ columnNames.head }'. Found: ${ ts.mkString("[", ", ", "]") }"))
      case ts => Failure(ParseException(rowNum, "Only one row is allowed to contain a value for " +
        s"these columns: ${ columnNames.mkString("[", ", ", "]") }. Found: ${ ts.mkString("[", ", ", "]") }"))
    }
  }

  def checkValidChars(rowNum: => Int, column: => MultiDepositKey, value: String): Try[String] = {
    val invalidCharacters = "[^a-zA-Z0-9_-]".r.findAllIn(value).toSeq.distinct
    if (invalidCharacters.isEmpty) Success(value)
    else Failure(ParseException(rowNum, s"The column '$column' contains the following invalid characters: ${ invalidCharacters.mkString("{", ", ", "}") }"))
  }

  def missingRequired[T](rowNum: Int, row: DatasetRow, required: Set[String]): Failure[T] = {
    val blankRequired = row.collect { case (key, value) if value.isBlank && required.contains(key) => key }
    val missingColumns = required.diff(row.keySet)
    val missing = blankRequired.toSet ++ missingColumns
    require(missing.nonEmpty, "the list of missing elements is supposed to be non-empty")
    Failure(ParseException(rowNum, s"Missing value(s) for: ${missing.mkString("[", ", ", "]")}"))
  }

  def extractDataset(datasetId: DatasetId, rows: DatasetRows): Try[Dataset] = {
    val rowNum = rows.map(getRowNum).min

    Try { Dataset.curried }
      .combine(checkValidChars(rowNum, "DATASET", datasetId))
      .map(_ (rowNum))
      .combine(extractNEL(rows, rowNum, "DEPOSITOR_ID").flatMap(exactlyOne(rowNum, List("DEPOSITOR_ID"))))
      .combine(extractProfile(rows, rowNum))
      .combine(extractMetadata(rows))
      .combine(extractAudioVideo(rows, rowNum))
  }

  def extractProfile(rows: DatasetRows, rowNum: Int): Try[Profile] = {
    Try { Profile.curried }
      .combine(extractNEL(rows, rowNum, "DC_TITLE"))
      .combine(extractNEL(rows, rowNum, "DC_DESCRIPTION"))
      .combine(extractList(rows)(creator)
        .flatMap {
          case Seq() => Failure(ParseException(rowNum, "There should be at least one non-empty value for the creator fields"))
          case xs => Success(listToNEL(xs))
        })
      .combine(extractList(rows)(date("DDM_CREATED"))
        .flatMap(exactlyOne(rowNum, List("DDM_CREATED"))))
      .combine(extractList(rows)(date("DDM_AVAILABLE"))
        .flatMap(atMostOne(rowNum, List("DDM_AVAILABLE")))
        .map(_.getOrElse(DateTime.now())))
      .combine(extractNEL(rows, rowNum, "DDM_AUDIENCE"))
      .combine(extractList(rows)(accessCategory)
        .flatMap(exactlyOne(rowNum, List("DDM_ACCESSRIGHTS"))))
      .flatMap {
        case Profile(_, _, _, _, _, audiences, AccessCategory.GROUP_ACCESS) if !audiences.contains("D37000") =>
          Failure(ParseException(rowNum, "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE " +
            s"should be D37000 (Archaeology), but it contains: ${ audiences.mkString("[", ", ", "]") }"))
        case profile => Success(profile)
      }
  }

  def extractMetadata(rows: DatasetRows): Try[Metadata] = {
    Try { Metadata.curried }
      .map(_ (extractList(rows, "DCT_ALTERNATIVE")))
      .map(_ (extractList(rows, "DC_PUBLISHER")))
      .combine(extractList(rows)(dcType).map(_ defaultIfEmpty DcType.DATASET))
      .map(_ (extractList(rows, "DC_FORMAT")))
      .map(_ (extractList(rows, "DC_IDENTIFIER")))
      .map(_ (extractList(rows, "DC_SOURCE")))
      .combine(extractList(rows)(iso639_2Language("DC_LANGUAGE")))
      .map(_ (extractList(rows, "DCT_SPATIAL")))
      .map(_ (extractList(rows, "DCT_RIGHTSHOLDER")))
      .combine(extractList(rows)(relation))
      .combine(extractList(rows)(contributor))
      .combine(extractList(rows)(subject))
      .combine(extractList(rows)(spatialPoint))
      .combine(extractList(rows)(spatialBox))
      .combine(extractList(rows)(temporal))
  }

  def extractAudioVideo(rows: DatasetRows, rowNum: Int): Try[AudioVideo] = {
    Try {
      ((springf: Option[Springfield], acc: Option[FileAccessRights.Value], avFiles: Set[AVFile]) => {
        (springf, acc, avFiles) match {
          case (None, _, fs) if fs.nonEmpty => Failure(ParseException(rowNum, "The column " +
            "'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not"))
          case (s, a, fs) => Try { AudioVideo(s, a, fs) }
        }
      }).curried
    }
      .combine(extractList(rows)(springfield)
        .flatMap(ss => atMostOne(rowNum, List("SF_DOMAIN", "SF_USER", "SF_COLLECTION"))(ss.map { case Springfield(d, u, c) => (d, u, c) }).map(_.map(Springfield.tupled))))
      .combine(extractList(rows)(fileAccessRight)
        .flatMap(atMostOne(rowNum, List("SF_ACCESSIBILITY"))))
      .combine(extractList(rows)(avFile)
        .flatMap(_.groupBy { case (file, _, _) => file }
          .map {
            case (file, (instrPerFile: Seq[(File, Option[String], Option[Subtitles])])) =>
              val fileTitle = instrPerFile.collect { case (_, Some(title), _) => title } match {
                case Seq() => Success(None)
                case Seq(title) => Success(Some(title))
                case Seq(_, _@_*) => Failure(ParseException(rowNum, s"The column 'AV_FILE_TITLE' " +
                  s"can only have one value for file '$file'"))
              }
              val subtitles = instrPerFile.collect { case (_, _, Some(instr)) => instr }

              fileTitle.map(AVFile(file, _, subtitles))
          }.collectResults.map(_.toSet)))
      .flatten
  }

  def date(columnName: MultiDepositKey)(rowNum: => Int)(row: DatasetRow): Option[Try[DateTime]] = {
    row.find(columnName)
      .map(date => Try { DateTime.parse(date) }.recoverWith {
        case e: IllegalArgumentException => Failure(ParseException(rowNum, s"$columnName value " +
          s"'$date' does not represent a date", e))
      })
  }

  def accessCategory(rowNum: => Int)(row: DatasetRow): Option[Try[AccessCategory]] = {
    row.find("DDM_ACCESSRIGHTS")
      .map(acc => Try { AccessCategory.valueOf(acc) }
        .recoverWith {
          case e: IllegalArgumentException => Failure(ParseException(rowNum, s"Value '$acc' is " +
            s"not a valid accessright", e))
        })
  }

  private lazy val iso639_2Languages = Locale.getISOLanguages.map(new Locale(_).getISO3Language).toSet

  def iso639_2Language(columnName: MultiDepositKey)(rowNum: => Int)(row: DatasetRow): Option[Try[String]] = {
    row.find(columnName)
      .map(lang => {
        // Most ISO 639-2/T languages are contained in the iso639_2Languages Set.
        // However, some of them are not and need to be checked using the second predicate.
        // The latter also allows to check ISO 639-2/B language codes.
        lazy val b0 = lang.length == 3
        lazy val b1 = iso639_2Languages.contains(lang)
        lazy val b2 = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

        if (b0 && (b1 || b2)) Success(lang)
        else Failure(ParseException(rowNum, s"Value '$lang' is not a valid value for $columnName"))
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
      case (None, None, None, None, Some(org), None) => Some(Try { CreatorOrganization(org) })
      case (_, Some(init), _, Some(sur), _, _) => Some(Try { CreatorPerson(titles, init, insertions, sur, organization, dai) })
      case (_, _, _, _, _, _) => Some(missingRequired(rowNum, row, Set("DCX_CREATOR_INITIALS", "DCX_CREATOR_SURNAME")))
    }
  }

  def contributor(rowNum: => Int)(row: DatasetRow): Option[Try[Contributor]] = {
    val titles = row.find("DCX_CONTRIBUTOR_TITLES")
    val initials = row.find("DCX_CONTRIBUTOR_INITIALS")
    val insertions = row.find("DCX_CONTRIBUTOR_INSERTIONS")
    val surname = row.find("DCX_CONTRIBUTOR_SURNAME")
    val organization = row.find("DCX_CONTRIBUTOR_ORGANIZATION")
    val dai = row.find("DCX_CONTRIBUTOR_DAI")

    (titles, initials, insertions, surname, organization, dai) match {
      case (None, None, None, None, None, None) => None
      case (None, None, None, None, Some(org), None) => Some(Try { ContributorOrganization(org) })
      case (_, Some(init), _, Some(sur), _, _) => Some(Try { ContributorPerson(titles, init, insertions, sur, organization, dai) })
      case (_, _, _, _, _, _) => Some(missingRequired(rowNum, row, Set("DCX_CONTRIBUTOR_INITIALS", "DCX_CONTRIBUTOR_SURNAME")))
    }
  }

  def dcType(rowNum: => Int)(row: DatasetRow): Option[Try[DcType.Value]] = {
    row.find("DC_TYPE")
      .map(t => DcType.valueOf(t)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$t' is not a valid type"))))
  }

  /*
    qualifier   link   title   valid
        1        1       1       0
        1        1       0       1
        1        0       1       1
        1        0       0       0
        0        1       1       0
        0        1       0       1
        0        0       1       1
        0        0       0       1

    observation: if the qualifier is present, either DCX_RELATION_LINK or DCX_RELATION_TITLE must be defined
                 if the qualifier is not defined, DCX_RELATION_LINK and DCX_RELATION_TITLE must not both be defined
   */
  def relation(rowNum: => Int)(row: DatasetRow): Option[Try[Relation]] = {
    val qualifier = row.find("DCX_RELATION_QUALIFIER")
    val link = row.find("DCX_RELATION_LINK")
    val title = row.find("DCX_RELATION_TITLE")

    (qualifier, link, title) match {
      case (Some(_), Some(_), Some(_)) | (None, Some(_), Some(_)) => Some(Failure(ParseException(rowNum, "Only one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined")))
      case (Some(q), Some(l), None) => Some(Try { QualifiedLinkRelation(q, l) })
      case (Some(q), None, Some(t)) => Some(Try { QualifiedTitleRelation(q, t) })
      case (Some(_), None, None) => Some(Failure(ParseException(rowNum, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well")))
      case (None, Some(l), None) => Some(Try { LinkRelation(l) })
      case (None, None, Some(t)) => Some(Try { TitleRelation(t) })
      case (None, None, None) => None
    }
  }

  def subject(rowNum: => Int)(row: DatasetRow): Option[Try[Subject]] = {
    val subject = row.find("DC_SUBJECT")
    val scheme = row.find("DC_SUBJECT_SCHEME")

    (subject, scheme) match {
      case (Some(subj), Some(sch)) if sch == "abr:ABRcomplex" => Some(Try { Subject(subj, Some(sch)) })
      case (Some(_), Some(_)) => Some(Failure(ParseException(rowNum, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'")))
      case (Some(subj), None) => Some(Try { Subject(subj) })
      case (None, Some(_)) => Some(Try { Subject(scheme = scheme) })
      case (None, None) => None
    }
  }

  def temporal(rowNum: => Int)(row: DatasetRow): Option[Try[Temporal]] = {
    val temporal = row.find("DCT_TEMPORAL")
    val scheme = row.find("DCT_TEMPORAL_SCHEME")

    (temporal, scheme) match {
      case (Some(temp), Some(sch)) if sch == "abr:ABRperiode" => Some(Try { Temporal(temp, Some(sch)) })
      case (Some(_), Some(_)) => Some(Failure(ParseException(rowNum, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'")))
      case (Some(temp), None) => Some(Try { Temporal(temp, None) })
      case (None, Some(_)) => Some(Try { Temporal(scheme = scheme) })
      case (None, None) => None
    }
  }

  def spatialPoint(rowNum: => Int)(row: DatasetRow): Option[Try[SpatialPoint]] = {
    val maybeX = row.find("DCX_SPATIAL_X")
    val maybeY = row.find("DCX_SPATIAL_Y")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (maybeX, maybeY, maybeScheme) match {
      case (Some(x), Some(y), scheme) => Some(Try { SpatialPoint(x, y, scheme) })
      case (None, None, _) => None
      case _ => Some(missingRequired(rowNum, row, Set("DCX_SPATIAL_X", "DCX_SPATIAL_Y")))
    }
  }

  def spatialBox(rowNum: => Int)(row: DatasetRow): Option[Try[SpatialBoxx]] = {
    val west = row.find("DCX_SPATIAL_WEST")
    val east = row.find("DCX_SPATIAL_EAST")
    val south = row.find("DCX_SPATIAL_SOUTH")
    val north = row.find("DCX_SPATIAL_NORTH")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (west, east, south, north, maybeScheme) match {
      case (Some(w), Some(e), Some(s), Some(n), scheme) => Some(Try { SpatialBoxx(n, s, e, w, scheme) })
      case (None, None, None, None, _) => None
      case _ => Some(missingRequired(rowNum, row, Set("DCX_SPATIAL_WEST", "DCX_SPATIAL_EAST", "DCX_SPATIAL_SOUTH", "DCX_SPATIAL_NORTH")))
    }
  }

  def springfield(rowNum: => Int)(row: DatasetRow): Option[Try[Springfield]] = {
    val domain = row.find("SF_DOMAIN")
    val user = row.find("SF_USER")
    val collection = row.find("SF_COLLECTION")

    def springfield(domain: String, user: String, collection: String): Try[Springfield] = {
      Try { Springfield.curried }
        .combine(checkValidChars(rowNum, "SF_DOMAIN", domain))
        .combine(checkValidChars(rowNum, "SF_USER", user))
        .combine(checkValidChars(rowNum, "SF_COLLECTION", collection))
    }

    def springfieldWithDefaultDomain(user: String, collection: String): Try[Springfield] = {
      Try { ((user: String, collection: String) => Springfield(user = user, collection = collection)).curried }
        .combine(checkValidChars(rowNum, "SF_USER", user))
        .combine(checkValidChars(rowNum, "SF_COLLECTION", collection))
    }

    (domain, user, collection) match {
      case (Some(d), Some(u), Some(c)) => Some(springfield(d, u, c))
      case (None, Some(u), Some(c)) => Some(springfieldWithDefaultDomain(u, c))
      case (_, Some(_), None) => Some(Failure(ParseException(rowNum, "Missing value for: SF_COLLECTION")))
      case (_, None, Some(_)) => Some(Failure(ParseException(rowNum, "Missing value for: SF_USER")))
      case (_, None, None) => None
    }
  }

  def avFile(rowNum: => Int)(row: DatasetRow): Option[Try[(File, Option[String], Option[Subtitles])]] = {
    val file = row.find("AV_FILE").map(new File(settings.multidepositDir, _))
    val title = row.find("AV_FILE_TITLE")
    val subtitle = row.find("AV_SUBTITLES").map(new File(settings.multidepositDir, _))
    val subtitleLang = row.find("AV_SUBTITLES_LANGUAGE")

    (file, title, subtitle, subtitleLang) match {
      case (Some(p), t, Some(sub), subLang) if p.exists() && sub.exists() => Some(Try { (p, t, Some(Subtitles(sub, subLang))) })
      case (Some(p), _, Some(_), _) if !p.exists() => Some(Failure(ParseException(rowNum, s"AV_FILE file '$p' does not exist")))
      case (Some(_), _, Some(sub), _) if !sub.exists() => Some(Failure(ParseException(rowNum, s"AV_SUBTITLES file '$sub' does not exist")))
      case (Some(_), _, None, Some(subLang)) => Some(Failure(ParseException(rowNum, s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: '$subLang'")))
      case (Some(p), t, None, None) if p.exists() => Some(Success((p, t, None)))
      case (Some(p), _, None, None) => Some(Failure(ParseException(rowNum, s"AV_FILE file '$p' does not exist")))
      case (None, None, None, None) => None
      case (None, _, _, _) => Some(Failure(ParseException(rowNum, "No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined")))
    }
  }

  def fileAccessRight(rowNum: => Int)(row: DatasetRow): Option[Try[FileAccessRights.Value]] = {
    row.find("SF_ACCESSIBILITY")
      .map(acc => FileAccessRights.valueOf(acc)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$acc' is not a valid file accessright"))))
  }
}
