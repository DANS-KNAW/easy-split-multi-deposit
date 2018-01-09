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
package nl.knaw.dans.easy.multideposit2

import java.nio.file.{ Files, Paths }

import scala.util.Failure
import nl.knaw.dans.easy.multideposit.FileExtensions

object Test extends App {

  val configuration = Configuration(Paths.get("src/main/assembly/dist"))
  val smd = Paths.get("src/test/resources/allfields/input")
  val sd = Paths.get("target/stagingDir")
  val od = Paths.get("target/outputDir")

  sd.deleteDirectory()
  od.deleteDirectory()

  Files.createDirectories(sd)
  Files.createDirectories(od)

  val app = SplitMultiDepositApp(smd, sd, od, configuration.formats)
  app.convert()
    .recoverWith { case e => e.printStackTrace(); Failure(e) }
    .foreach(_ => println("success"))
}
