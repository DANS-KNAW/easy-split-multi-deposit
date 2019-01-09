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
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.encoding
import nl.knaw.dans.easy.multideposit.model.{ Deposit, DepositId, Instructions, MultiDepositKey }
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

  def parse: Either[ParseFailed, Seq[Deposit]] = {
    logger.info(s"Reading data in $multiDepositDir")

    val instructions = instructionsFile
    if (instructions.exists) {
      logger.info(s"Parsing $instructions")

      val deposits = for {
        res <- read(instructions).toEitherNec
        (headers, content) = res
        depositIdIndex = headers.indexOf("DATASET")
        _ <- detectEmptyDepositCells(content.map(_ (depositIdIndex)))
        result <- content.groupBy(_ (depositIdIndex))
          .mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))
          .map { case (depositId, rows) => extractDeposit(multiDepositDir)(depositId, rows).toValidated }
          .toList
          .sequence[Validated, Deposit]
          .toEither
      } yield result

      deposits.leftMap(chain => parserErrorReport(chain))
    }
    else
      ParseFailed(s"Could not find a file called 'instructions.csv' in $multiDepositDir").asLeft
  }

  def read(instructions: File): Either[ParserError, (List[MultiDepositKey], List[List[String]])] = {
    managed(CSVParser.parse(instructions.toJava, encoding, CSVFormat.RFC4180))
      .map(csvParse)
      .tried
      .toEither
      .leftMap(e => ParseError(-1, e.getMessage))
      .flatMap {
        case Nil => EmptyInstructionsFileError(instructions).asLeft
        case headers :: rows =>
          validateDepositHeaders(headers)
            .map(_ => ("ROW" :: headers, rows.zipWithIndex.collect {
              case (row, index) if row.nonEmpty => (index + 2).toString :: row
            }))
      }
  }

  private def csvParse(parser: CSVParser): List[List[String]] = {
    parser.getRecords.asScala.toList
      .map(_.asScala.toList.map(_.trim))
      .map {
        case "" :: Nil => Nil // blank line
        case xs => xs
      }
  }

  private def validateDepositHeaders(headers: List[MultiDepositKey]): FailFast[Unit] = {
    val validHeaders = Headers.validHeaders
    val invalidHeaders = headers.filterNot(validHeaders.contains)
    lazy val uniqueHeaders = headers.distinct

    invalidHeaders match {
      case Nil =>
        headers match {
          case hs if hs.size != uniqueHeaders.size =>
            ParseError(0, "SIP Instructions file contains duplicate headers: " + headers.diff(uniqueHeaders).mkString("[", ", ", "]")).asLeft
          case _ => ().asRight
        }
      case invalids =>
        ParseError(0, "SIP Instructions file contains unknown headers: " +
          s"${ invalids.mkString("[", ", ", "]") }. Please, check for spelling errors and " +
          "consult the documentation for the list of valid headers.").asLeft
    }
  }

  def detectEmptyDepositCells(depositIds: List[String]): Either[NonEmptyChain[ParseError], Unit] = {
    depositIds.zipWithIndex
      .map {
        case (s, i) if s.isBlank =>
          val index = i + 2
          ParseError(index, s"Row $index does not have a depositId in column DATASET").toInvalid
        case _ => ().toValidated
      }
      .sequence[Validated, Unit]
      .map(_ => ())
      .toEither
  }

  def extractDeposit(multiDepositDirectory: File)
                    (depositId: DepositId, rows: DepositRows): Either[NonEmptyChain[ParseError], Deposit] = {
    val rowNum = rows.map(getRowNum).min

    for {
      depositId <- checkValidChars(depositId, rowNum, "DATASET").toEitherNec
      instructions <- extractInstructions(depositId, rowNum, rows).toEither
      depositDir = multiDepositDirectory / depositId
      fileMetadata <- extractFileMetadata(depositDir, instructions).toEither
      deposit = instructions.toDeposit(fileMetadata)
      _ <- validateDeposit(deposit).toEither
    } yield deposit
  }

  def extractInstructions(depositId: DepositId, rowNum: Int, rows: DepositRows): Validated[Instructions] = {
    (
      depositId.toValidated,
      rowNum.toValidated,
      extractExactlyOne(rowNum, "DEPOSITOR_ID", rows).toValidated,
      extractProfile(rowNum, rows),
      extractAtMostOne(rowNum, "BASE_REVISION", rows)
        .flatMap(_.map(uuid(rowNum, "BASE_REVISION")).getOrElse(none.asRight))
        .toValidated,
      extractMetadata(rowNum, rows),
      extractFileDescriptors(depositId, rowNum, rows),
      extractAudioVideo(depositId, rowNum, rows),
    ).mapN(Instructions)
  }

  def uuid(rowNum: => Int, columnName: => String)(s: String): FailFast[Option[UUID]] = {
    Either.catchOnly[IllegalArgumentException] { Option(UUID.fromString(s)) }
      .leftMap(_ => ParseError(rowNum, s"$columnName value '$s' does not conform to the UUID format"))
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
  def parse(md: File, licenses: Set[String]): Either[ParseFailed, Seq[Deposit]] = new MultiDepositParser {
    val multiDepositDir: File = md
    val userLicenses: Set[String] = licenses
  }.parse
}
