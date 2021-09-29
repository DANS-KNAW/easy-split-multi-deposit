/*
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
package nl.knaw.dans.easy.multideposit.actions

import better.files.{ Dispose, _ }
import cats.syntax.either._
import nl.knaw.dans.easy.multideposit.PathExplorer.OutputPathExplorer
import nl.knaw.dans.easy.multideposit.actions.ReportDatasets._
import nl.knaw.dans.easy.multideposit.model.Deposit
import nl.knaw.dans.easy.multideposit.{ ActionError, FailFast, encoding }
import org.apache.commons.csv.{ CSVFormat, CSVPrinter }

class ReportDatasets {

  def report(deposits: Seq[Deposit])(implicit output: OutputPathExplorer): FailFast[Unit] = {
    Either.catchNonFatal {
      for (printer <- csvPrinter(output.reportFile);
           deposit <- deposits)
        printRecord(deposit, printer)
    }.leftMap(e => ActionError("Could not write the dataset report", e))
  }

  private def csvPrinter(file: File): Dispose[CSVPrinter] = {
    file.bufferedWriter(charset = encoding)
      .flatMap[CSVPrinter, Dispose](writer => new Dispose(csvFormat.print(writer)))
  }

  private def printRecord(deposit: Deposit, printer: CSVPrinter): Unit = {
    printer.printRecord(
      deposit.depositId,
      deposit.bagId,
      deposit.baseUUID.getOrElse(deposit.bagId)
    )
  }
}

object ReportDatasets {
  private val csvFormat = CSVFormat.RFC4180.withHeader("DATASET", "UUID", "BASE-REVISION")
}
