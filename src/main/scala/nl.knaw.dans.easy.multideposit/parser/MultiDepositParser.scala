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

import java.nio.file.NoSuchFileException
import java.util.UUID

import better.files.File
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.encoding
import nl.knaw.dans.easy.multideposit.model.{ Deposit, DepositId, Instructions, MultiDepositKey, listToNEL }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.string.StringExtensions
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import resource.managed

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait MultiDepositParser extends ParserUtils with InputPathExplorer
  with AudioVideoParser
  with FileDescriptorParser
  with FileMetadataParser
  with MetadataParser
  with ProfileParser
  with ParserValidation
  with DebugEnhancedLogging {

  def parse: Try[Seq[Deposit]] = {
    logger.info(s"Reading data in $multiDepositDir")

    val instructions = instructionsFile
    if (instructions.exists) {
      logger.info(s"Parsing $instructions")

      val deposits = for {
        (headers, content) <- read(instructions)
        depositIdIndex = headers.indexOf("DATASET")
        _ <- detectEmptyDepositCells(content.map(_ (depositIdIndex)))
        result <- content.groupBy(_ (depositIdIndex))
          .mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))
          .map { case (depositId, rows) => extractDeposit(multiDepositDir)(depositId, rows) }
          .toSeq
          .collectResults
      } yield result

      deposits.recoverWith { case NonFatal(e) => recoverParsing(e) }
    }
    else
      Failure(new NoSuchFileException(s"Could not find a file called 'instructions.csv' in $multiDepositDir"))
  }

  def read(instructions: File): Try[(List[MultiDepositKey], List[List[String]])] = {
    managed(CSVParser.parse(instructions.toJava, encoding, CSVFormat.RFC4180))
      .map(csvParse)
      .tried
      .flatMap {
        case Nil => Failure(EmptyInstructionsFileException(instructions))
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

  private def validateDepositHeaders(headers: List[MultiDepositKey]): Try[Unit] = {
    val validHeaders = Headers.validHeaders
    val invalidHeaders = headers.filterNot(validHeaders.contains)
    lazy val uniqueHeaders = headers.distinct

    invalidHeaders match {
      case Nil =>
        headers match {
          case hs if hs.size != uniqueHeaders.size =>
            Failure(ParseException(0, "SIP Instructions file contains duplicate headers: " +
              s"${ headers.diff(uniqueHeaders).mkString("[", ", ", "]") }"))
          case _ => Success(())
        }
      case invalids =>
        Failure(ParseException(0, "SIP Instructions file contains unknown headers: " +
          s"${ invalids.mkString("[", ", ", "]") }. Please, check for spelling errors and " +
          "consult the documentation for the list of valid headers."))
    }
  }

  def detectEmptyDepositCells(depositIds: List[String]): Try[Unit] = {
    depositIds.zipWithIndex
      .collect { case (s, i) if s.isBlank => i + 2 }
      .map(index => Failure(ParseException(index, s"Row $index does not have a depositId in column DATASET")): Try[Unit])
      .collectResults
      .map(_ => ())
  }

  private def extractDeposit(multiDepositDirectory: File)
                            (depositId: DepositId, rows: DepositRows): Try[Deposit] = {
    for {
      instructions <- extractInstructions(depositId, rows)
      depositDir = multiDepositDirectory / depositId
      fileMetadata <- extractFileMetadata(depositDir, instructions)
      deposit = instructions.toDeposit(fileMetadata)
      _ <- validateDeposit(deposit)
    } yield deposit
  }

  def extractInstructions(depositId: DepositId, rows: DepositRows): Try[Instructions] = {
    val rowNum = rows.map(getRowNum).min
    checkValidChars(depositId, rowNum, "DATASET")
      .flatMap(dsId => Try { Instructions.curried }
        .map(_ (dsId))
        .map(_ (rowNum))
        .combine(extractNEL(rows, rowNum, "DEPOSITOR_ID").flatMap(exactlyOne(rowNum, List("DEPOSITOR_ID"))))
        .combine(extractProfile(rows, rowNum))
        .combine(extractList(rows)(uuid("BASE_REVISION")).flatMap(atMostOne(rowNum, List("BASE_REVISION"))))
        .combine(extractMetadata(rows, rowNum))
        .combine(extractFileDescriptors(rows, rowNum, depositId))
        .combine(extractAudioVideo(rows, rowNum, depositId)))
  }

  def uuid(columnName: MultiDepositKey)(rowNum: => Int)(row: DepositRow): Option[Try[UUID]] = {
    row.find(columnName)
      .map(uuid => Try { UUID.fromString(uuid) }.recoverWith {
        case e: IllegalArgumentException => Failure(ParseException(rowNum, s"$columnName value " +
          s"base revision '$uuid' does not conform to the UUID format", e))
      })
  }

  private def recoverParsing(t: Throwable): Failure[Nothing] = {
    def generateReport(header: String = "", throwable: Throwable, footer: String = ""): String = {
      header.toOption.fold("")(_ + "\n") +
        List(throwable)
          .flatMap {
            case es: CompositeException => es.throwables
            case e => Seq(e)
          }
          .sortBy {
            case ParseException(row, _, _) => row
            case _ => -1
          }
          .map {
            case ParseException(row, msg, _) => s" - row $row: $msg"
            case e => s" - unexpected: ${ e.getMessage }"
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
}

object MultiDepositParser {
  def parse(md: File, licenses: Set[String]): Try[Seq[Deposit]] = new MultiDepositParser {
    val multiDepositDir: File = md
    val userLicenses: Set[String] = licenses
  }.parse
}
