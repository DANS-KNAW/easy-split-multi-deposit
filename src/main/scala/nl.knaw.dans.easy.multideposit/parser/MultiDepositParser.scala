/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.parser

import java.util.UUID

import better.files.File
import cats.data.NonEmptyChain
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.model.{ Deposit, DepositId, Instructions }
import nl.knaw.dans.easy.multideposit.parser.Headers.{ BaseRevision, Dataset, DepositorId, Header }
import nl.knaw.dans.easy.multideposit.{ ParseFailed, encoding }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.string._
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import resource.managed

import scala.collection.JavaConverters._

trait MultiDepositParser extends ParserUtils with InputPathExplorer
  with AudioVideoParser
  with FileDescriptorParser
  with FileMetadataParser
  with MetadataParser
  with ProfileParser
  with ParserValidation
  with DebugEnhancedLogging {

  def parse: Either[ParseFailed, List[Deposit]] = {
    logger.info(s"Reading data in $multiDepositDir")

    val instructions = instructionsFile
    if (instructions.exists) {
      logger.info(s"Parsing $instructions")

      read(instructions)
        .andThen {
          case (headers, content) =>
            val depositIdIndex = headers.indexOf(Headers.Dataset)
            detectEmptyDepositCells(content.map { case (_, cs) => cs(depositIdIndex) })
              .productR(createDeposits(depositIdIndex)(headers, content))
        }
        .leftMap(parserErrorReport)
        .toEither
    }
    else
      ParseFailed(s"Could not find a file called 'instructions.csv' in $multiDepositDir").asLeft
  }

  private type Index = Int
  private type CsvHeaderRow = List[Header]
  private type CsvRow = List[String]

  private def createDeposits(depositIdIndex: Int)
                            (headers: CsvHeaderRow, content: List[(Index, CsvRow)]): Validated[List[Deposit]] = {
    content.groupBy { case (_, cs) => cs(depositIdIndex) }
      .mapValues(_.map { case (rowNum, data) => createDepositRow(headers)(rowNum, data) })
      .toList
      .traverse { case (depositId, rows) => extractDeposit(multiDepositDir)(depositId, rows) }
  }

  private def createDepositRow(headers: CsvHeaderRow)(rowNum: Index, data: CsvRow): DepositRow = {
    val content = headers.zip(data)
      .filterNot { case (_, value) => value.isBlank }
      .toMap

    DepositRow(rowNum, content)
  }

  def read(instructions: File): Validated[(CsvHeaderRow, List[(Index, CsvRow)])] = {
    managed(CSVParser.parse(instructions.toJava, encoding, CSVFormat.RFC4180))
      .map(csvParse)
      .tried
      .toEither
      .toValidated
      .leftMap(e => ParseError(-1, e.getMessage).chained)
      .andThen {
        case Nil => EmptyInstructionsFileError(instructions).toInvalid
        case (_, headers) :: rows => validateDepositHeaders(headers).map(_ -> rows)
      }
  }

  private def csvParse(parser: CSVParser): List[(Index, CsvRow)] = {
    parser.iterator().asScala
      .map(_.asScala.toList.map(_.trim))
      .zipWithIndex
      .collect { case (row, index) if !row.forall(_.trim.isEmpty) => index + 1 -> row }
      .toList
  }

  private def validateDepositHeaders(headers: CsvRow): Validated[CsvHeaderRow] = {
    val validHeaders = Headers.values.map(_.toString)
    val invalidHeaders = headers.filterNot(validHeaders.contains)
    lazy val uniqueHeaders = headers.distinct

    invalidHeaders match {
      case Nil =>
        headers match {
          case hs if hs.size != uniqueHeaders.size =>
            ParseError(0, "SIP Instructions file contains duplicate headers: " + headers.diff(uniqueHeaders).mkString("[", ", ", "]")).toInvalid
          case _ => headers.map(Headers.withName).toValidated
        }
      case invalids =>
        ParseError(0, "SIP Instructions file contains unknown headers: " +
          s"${ invalids.mkString("[", ", ", "]") }. Please, check for spelling errors and " +
          "consult the documentation for the list of valid headers.").toInvalid
    }
  }

  def detectEmptyDepositCells(depositIds: List[String]): Validated[Unit] = {
    depositIds.zipWithIndex
      .traverse {
        case (s, i) if s.isBlank =>
          val index = i + 2
          ParseError(index, s"Row $index does not have a depositId in column ${ Headers.Dataset }").toInvalid
        case _ => ().toValidated
      }
      .map(_ => ())
  }

  def extractDeposit(multiDepositDirectory: File)(depositId: DepositId, rows: DepositRows): Validated[Deposit] = {
    val rowNum = rows.map(_.rowNum).min

    checkValidChars(depositId, rowNum, Dataset)
      .andThen(depositId =>
        extractInstructions(depositId, rowNum, rows)
          .tupleRight(multiDepositDirectory / depositId)
      )
      .andThen {
        case (instructions, depositDir) =>
          extractFileMetadata(depositDir, instructions)
            .map(instructions.toDeposit)
      }
      .andThen(deposit => validateDeposit(deposit).as(deposit))
  }

  def extractInstructions(depositId: DepositId, rowNum: Int, rows: DepositRows): Validated[Instructions] = {
    // @formatter:off
    (
      depositId.toValidated,
      rowNum.toValidated,
      extractExactlyOne(rowNum, DepositorId, rows),
      extractProfile(rowNum, rows),
      extractBaseRevision(rowNum, rows),
      extractMetadata(rowNum, rows),
      extractFileDescriptors(depositId, rowNum, rows),
      extractAudioVideo(depositId, rowNum, rows),
    ).mapN(Instructions)
    // @formatter:on
  }

  private def extractBaseRevision(rowNum: Int, rows: DepositRows): Validated[Option[UUID]] = {
    extractAtMostOne(rowNum, BaseRevision, rows)
      .andThen {
        case Some(value) => uuid(rowNum, BaseRevision)(value)
        case None => none.toValidated
      }
  }

  def uuid(rowNum: => Int, columnName: => Header)(s: String): Validated[Option[UUID]] = {
    s.toUUID
      .map(Option(_))
      .leftMap(_ => ParseError(rowNum, s"$columnName value '$s' does not conform to the UUID format"))
      .toValidatedNec
  }

  private def parserErrorReport(errors: NonEmptyChain[ParserError]): ParseFailed = {
    val summary = errors.toNonEmptyList.toList
      .sortBy {
        case EmptyInstructionsFileError(_) => -1
        case ParseError(row, _) => row
      }
      .map {
        case EmptyInstructionsFileError(file) => s" - unexpected: The given instructions file in '$file' is empty"
        case ParseError(row, msg) => s" - row $row: $msg"
      }
      .mkString("\n")

    ParseFailed(
      s"""CSV failures:
         |$summary
         |Due to these errors in the 'instructions.csv', nothing was done.""".stripMargin
    )
  }
}

object MultiDepositParser {
  def parse(md: File, licenses: Set[String]): Either[ParseFailed, List[Deposit]] = new MultiDepositParser {
    val multiDepositDir: File = md
    val userLicenses: Set[String] = licenses
  }.parse
}
