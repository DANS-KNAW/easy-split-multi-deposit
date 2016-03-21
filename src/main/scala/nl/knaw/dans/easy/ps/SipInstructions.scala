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
package nl.knaw.dans.easy.ps

import java.io.File
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable
import scala.collection.JavaConversions.{ asScalaBuffer, iterableAsScalaIterable }
import scala.collection.mutable
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import scala.collection.mutable.ListBuffer

// sip == multi deposit
object SipInstructions {
  type CsvValues = List[(String, String)]

  val log = LoggerFactory.getLogger(SipInstructions.getClass)

  def parse(f: File): Observable[Datasets] = {
    log.debug("Start parsing SIP Instructions at {}", f)
    val rawContent = scala.io.Source.fromFile(f).mkString
    val parser = CSVParser.parse(rawContent, CSVFormat.RFC4180)
    val csv = parser.getRecords.map(_.toList)
    verifyDatasetHasValidHeaders(csv.head) match {
      case Success(_) =>
        val csvTuples = csv.tail.map(csv.head.zip(_))
        log.debug("Successfully loaded CSV file")
        val initialDatasets: Datasets = ListBuffer[(DatasetID, mutable.HashMap[MdKey, MdValues])]()
        var row: Int = 1
        Observable.from(csvTuples)
          .foldLeft(initialDatasets)((datasets, csvValues) => { row += 1; updateDatasets(datasets, csvValues, row) })
      case Failure(e) => Observable.error(e)
    }
  }

  def verifyDatasetHasValidHeaders(headers: List[String]): Try[Unit] = {
    val validHeaders = "DATASET_ID" ::
      "FILE_SIP" :: "FILE_DATASET" :: "FILE_AUDIO_VIDEO" :: "FILE_STORAGE_SERVICE" :: "FILE_STORAGE_PATH" :: "FILE_SUBTITLES" ::
      "SF_DOMAIN" :: "SF_USER" :: "SF_COLLECTION" :: "SF_PRESENTATION" :: "SF_SUBTITLES" ::
      DDM.allFields
    val invalidHeaders = headers.filter { !validHeaders.contains(_) }
    if (invalidHeaders.isEmpty)
      Success(Unit)
    else
      Failure(new ActionException("0", s"SIP Instructions file contains unknown headers: ${invalidHeaders.mkString(", ")}. "
        + "Please, check for spelling errors and consult the documentation for the list of valid headers."))
  }

  def updateDatasets(datasets: Datasets, values: CsvValues, row: Int): Datasets = {
    val id = values.find(kv => kv._1.equals("DATASET_ID")) match {
      case Some(keyValue) => keyValue._2
      case None => throw new Exception("No dataset ID found")
    }

    datasets.find(_._1 == id) match {
      case Some((_, dataset)) => updateDataset(dataset, values, row)
      case None =>
        log.debug("Found new Dataset")
        val newDataset: Dataset = new mutable.HashMap()
        datasets += ((id, newDataset))
        updateDataset(newDataset, values, row)
    }

    datasets
  }

  def updateDataset(dataset: Dataset, values: CsvValues, row: Int): Dataset = {
    log debug ("Processing SIP Intructions line: {}", values)
    values.foreach { case (key, value) => dataset.put(key, dataset.getOrElseUpdate(key, List()) :+ value) }
    dataset.put("ROW", dataset.getOrElseUpdate("ROW", List()) :+ row.toString)
    dataset
  }
}
