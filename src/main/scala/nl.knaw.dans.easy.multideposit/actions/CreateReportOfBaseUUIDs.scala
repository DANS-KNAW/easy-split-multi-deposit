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
package nl.knaw.dans.easy.multideposit.actions

import java.{ util => ju }

import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model.{ BaseUUID, DepositId }
import nl.knaw.dans.easy.multideposit.parser.MultiDepositParser
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.csv.CSVParser
import org.joda.time.DateTime

import scala.collection.mutable.Map
import scala.util.Properties.userHome
import scala.util.Try

class CreateReportOfBaseUUIDs extends DebugEnhancedLogging {

  //

  def createMap(depositId: DepositId, created: DateTime, base: Option[BaseUUID])(implicit input: InputPathExplorer, stage: StagingPathExplorer): Try[Unit] = {
    var multiValueMap: Map[String, List[String]] = Map(depositId -> List())
    Try {
      base.foreach(uuid => multiValueMap.put(depositId, List(depositId, "", uuid.toString)))
    }

  }



  /*def con(paths: PathExplorers, datamanagerId: Datamanager): Try[Unit] = {
    implicit val input: InputPathExplorer = paths
    implicit val staging: StagingPathExplorer = paths
    implicit val output: OutputPathExplorer = paths

    val csvFormat: CSVFormat = CSVFormat.RFC4180.withHeader("DATASET", "UUID", "BASE_UUID").withDelimiter(',')
    val out: Appendable = new StringBuffer()
    val printer: CSVPrinter = csvFormat.print(out)
    val csvPrinterToFile = new CSVPrinter(new FileWriter(s"$userHome/instructions.csv"), csvFormat.withDelimiter(','))
    //  val csvPrinterToFile = new CSVPrinter(new FileWriter("./instructions.csv"), csvFormat.withDelimiter(','))
    val printerToFile: CSVPrinter = csvFormat.print(out)



    //MultiDepositParser.parse(input.multiDepositDir).foreach{i =>


    def printBaseUUID(depositId: DepositId, created: DateTime, base: Option[BaseUUID])(implicit input: InputPathExplorer, stage: StagingPathExplorer): Try[Unit] = {
      Try {

        val csvFormat: CSVFormat = CSVFormat.RFC4180.withHeader("DATASET", "UUID", "BASE_UUID").withDelimiter(',')
        val out: Appendable = new StringBuffer()
        val printer: CSVPrinter = csvFormat.print(out)
        val csvPrinterToFile = new CSVPrinter(new FileWriter(s"$userHome/instructions.csv"), csvFormat.withDelimiter(','))
        //  val csvPrinterToFile = new CSVPrinter(new FileWriter("./instructions.csv"), csvFormat.withDelimiter(','))
        val printerToFile: CSVPrinter = csvFormat.print(out)

        print(userHome)
        logger.info(userHome)
        printer.print(base)

        base.foreach(uuid => csvPrinterToFile.printRecord(depositId, "", uuid.toString))

        printer.flush()
        csvPrinterToFile.flush()
        out.toString()
        //printer.close()
      }


    }


  //printer.flush()
  //csvPrinterToFile.flush()
  //out.toString()
  //printer.close()

*/

}
