/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable

import scala.collection.JavaConversions.{asScalaBuffer, iterableAsScalaIterable}
import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try}

object MultiDepositParser {

  type CsvValues = List[(String, String)]

  val log = LoggerFactory.getLogger(MultiDepositParser.getClass)

  /**
    * Transforms a `File` into a stream of `Datasets` by parsing the csv.
    *
    * @param file the csv file to be parsed
    * @return a stream of datasets coming from the file
    */
  def parse(file: File): Observable[Datasets] = {
    Observable(subscriber => {
      log.debug("Start parsing SIP Instructions at {}", file)
      Try {
        val rawContent = Source.fromFile(file)(Codec(encoding)).mkString
        val parser = CSVParser.parse(rawContent, CSVFormat.RFC4180)
        val output = parser.getRecords.filter(r => r.size() > 0 && !r.get(0).isBlank).map(_.toList)
        validateDatasetHeaders(output.head)
          .map(_ => {
            val csvData = output.tail.map(output.head zip _)
            log.debug("Successfully loaded CSV file")
            Observable.from(csvData)
              .zipWithIndex
              .map { case (xs, i) => (xs, i + 2) } // +2 because we want the second line (element 0) to have index 2
              .foldLeft(new Datasets)((dss, tuple) => updateDatasets(dss, tuple))
          })
          .onError(Observable.error)
          .subscribe(subscriber)
      }.onError(subscriber.onError)
    })
  }

  /**
    * Tests whether the given list of headers is valid. If so, `Success(Unit)` is returned,
    * else a `Failure` with a detailed `ActionException` is returned.
    *
    * @param headers the headers to be validated
    * @return `Success` if the `headers` are valid, `Failure` if the `headers` are invalid
    */
  def validateDatasetHeaders(headers: List[String]): Try[Unit] = {
    val validHeaders = List("DATASET", "FILE_SIP", "FILE_DATASET", "FILE_AUDIO_VIDEO",
      "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_SUBTITLES",
      "SF_DOMAIN", "SF_USER", "SF_COLLECTION", "SF_PRESENTATION", "SF_SUBTITLES") ++ DDM.allFields
    if (headers.forall(validHeaders.contains)) Success(Unit)
    else Failure(new ActionException(0, "SIP Instructions file contains unknown headers: "
      + headers.filter(!validHeaders.contains(_)).mkString(", ") + ". "
      + "Please, check for spelling errors and consult the documentation for the list of valid headers."))
  }

  private def updateDatasets(dss: Datasets, tuple: (CsvValues, Int)): Datasets = {
    updateDatasets(dss, tuple._1, tuple._2)
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
        log.debug("Found new Dataset")
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
    def addToDataset(dataset: Dataset)(kvPair: (String, String)): Unit = {
      val (key, value) = kvPair
      dataset.put(key, dataset.getOrElseUpdate(key, List()) :+ value)
    }

    log.debug(s"Processing SIP Intructions line: $values}", values)
    values.foreach(addToDataset(dataset))
    addToDataset(dataset)(("ROW", row.toString))

    dataset
  }
}
