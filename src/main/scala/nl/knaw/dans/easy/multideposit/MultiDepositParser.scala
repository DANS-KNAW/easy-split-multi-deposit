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
package nl.knaw.dans.easy.multideposit

import java.io.File

import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import resource._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{ Failure, Success, Try }

object MultiDepositParser extends DebugEnhancedLogging {

  type CsvValues = List[(String, String)]

  /**
   * Transforms a `File` into a `Datasets` instance by parsing the csv.
   *
   * @param file the csv file to be parsed
   * @return a collection of datasets coming from the file
   */
  def parse(file: File): Try[Datasets] = {
    debug(s"Start parsing SIP Instructions at $file")

    for {
      content <- read(file)
      _ <- content.headOption.map(validateDatasetHeaders).getOrElse(Failure(EmptyInstructionsFileException(file)))
    } yield {
      debug("Successfully loaded CSV file")
      content.tail
        .map(content.head.zip(_))
        .zipWithIndex
        .foldRight(Datasets) {
          // +2 because we want the second line in the csv (element 0) to have index 2
          case ((values: CsvValues, row: Int), dss: Datasets) => updateDatasets(dss, values, row + 2)
        }
    }
  }

  private def read(file: File) = {
    managed(Source.fromFile(file)(encoding)).map(_.mkString)
      .flatMap(rawContent => managed(CSVParser.parse(rawContent, CSVFormat.RFC4180)))
      .map(parse)
      .tried
  }

  private def parse(parser: CSVParser): mutable.Buffer[List[String]] = {
    for {
      record <- parser.getRecords.asScala
      if record.size() > 0
      if !record.get(0).isBlank
    } yield record.asScala.toList.map(_.trim)
  }

  /**
   * Tests whether the given list of headers is valid. If so, `Success(Unit)` is returned,
   * else a `Failure` with a detailed `ActionException` is returned.
   *
   * @param headers the headers to be validated
   * @return `Success` if the `headers` are valid, `Failure` if the `headers` are invalid
   */
  // TODO valid headers listing as sub-command???
  def validateDatasetHeaders(headers: List[String]): Try[Unit] = {
    val validHeaders = Headers.validHeaders
    val invalidHeaders = headers.filterNot(validHeaders.contains)
    if (invalidHeaders.isEmpty) Success(Unit)
    else Failure(ActionException(0, s"SIP Instructions file contains unknown headers: ${invalidHeaders.mkString(", ")}. "
      + "Please, check for spelling errors and consult the documentation for the list of valid headers."))
  }

  /**
   * Updates the `datasets` with the given `values` and `row`.
   *
   * @param datasets the `datasets` to which values need to be added
   * @param values   the `values` to be added
   * @param row      the `row` number that will be added to the `dataset`
   * @return the same `datasets`
   */
  def updateDatasets(datasets: Datasets, values: CsvValues, row: Int): Datasets = {
    val id = values.find(_._1 equals "DATASET")
      .map(_._2)
      .getOrElse {
        throw new Exception("No dataset ID found")
      }

    datasets.find(_._1 == id)
      .map { case (_, dataset) => updateDataset(dataset, values, row) }
      .getOrElse {
        debug("Found new Dataset")
        val newDataset = new Dataset
        datasets += id -> newDataset
        updateDataset(newDataset, values, row)
      }

    datasets
  }

  /**
   * Adds the `values` to the `dataset`, as well as an extra value for the row number.
   *
   * @param dataset the `dataset` to which values need to be added
   * @param values  the `values` to be added
   * @param row     the `row` number that will be added to the `dataset`
   * @return the `dataset`
   */
  def updateDataset(dataset: Dataset, values: CsvValues, row: Int): Dataset = {
    def addToDataset(dataset: Dataset)(kvPair: (MultiDepositKey, String)): Unit = {
      val (key, value) = kvPair
      dataset.put(key, value :: dataset.getOrElse(key, List.empty))
    }

    debug(s"Processing SIP Intructions line: $values}")
    values.foreach(addToDataset(dataset))
    addToDataset(dataset)(("ROW", row.toString))

    dataset
  }
}
