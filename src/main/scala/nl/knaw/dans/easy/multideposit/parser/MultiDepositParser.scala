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

import java.io.File

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import resource._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait MultiDepositParser extends ParserUtils with AudioVideoParser with MetadataParser with ProfileParser with DebugEnhancedLogging {

  def parse(file: File): Try[Seq[Deposit]] = {
    logger.info(s"Parsing $file")

    val deposits = for {
      (headers, content) <- read(file)
      depositIdIndex = headers.indexOf("DATASET")
      _ <- detectEmptyDepositCells(content.map(_ (depositIdIndex)))
      result <- content.groupBy(_ (depositIdIndex))
        .mapValues(_.map(headers.zip(_).filterNot { case (_, value) => value.isBlank }.toMap))
        .map((extractDeposit _).tupled)
        .toSeq
        .collectResults
    } yield result

    deposits.recoverWith { case NonFatal(e) => recoverParsing(e) }
  }

  def read(file: File): Try[(List[MultiDepositKey], List[List[String]])] = {
    managed(CSVParser.parse(file, encoding, CSVFormat.RFC4180))
      .map(csvParse)
      .tried
      .flatMap {
        case Nil => Failure(EmptyInstructionsFileException(file))
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

    if (invalidHeaders.nonEmpty)
      Failure(ParseException(0, "SIP Instructions file contains unknown headers: " +
        s"${ invalidHeaders.mkString("[", ", ", "]") }. Please, check for spelling errors and " +
        s"consult the documentation for the list of valid headers."))
    else if (headers.size != uniqueHeaders.size) {
      Failure(ParseException(0, "SIP Instructions file contains duplicate headers: " +
        s"${ headers.diff(uniqueHeaders).mkString("[", ", ", "]") }"))
    }
    else
      Success(())
  }

  def detectEmptyDepositCells(datasetIds: List[String]): Try[Unit] = {
    datasetIds.zipWithIndex
      .collect { case (s, i) if s.isBlank => i + 2 }
      .map(index => Failure(ParseException(index, s"Row $index does not have a datasetId in column DATASET")): Try[Unit])
      .collectResults
      .map(_ => ())
  }

  def extractDeposit(datasetId: DatasetId, rows: DepositRows): Try[Deposit] = {
    val rowNum = rows.map(getRowNum).min

    checkValidChars(rowNum, "DATASET", datasetId)
      .flatMap(dsId => Try { Deposit.curried }
        .map(_ (dsId))
        .map(_ (rowNum))
        .combine(extractNEL(rows, rowNum, "DEPOSITOR_ID").flatMap(exactlyOne(rowNum, List("DEPOSITOR_ID"))))
        .combine(extractProfile(rows, rowNum))
        .combine(extractMetadata(rows))
        .combine(extractAudioVideo(rows, rowNum, datasetId)))
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
  def apply()(implicit ss: Settings): MultiDepositParser = new MultiDepositParser {
    val settings: Settings = ss
  }
}
