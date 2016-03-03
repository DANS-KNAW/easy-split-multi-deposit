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
package nl.knaw.dans.easy.multideposit.actions

import java.io.File

import nl.knaw.dans.easy.multideposit.{Settings, UnitSpec, _}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter}

import scala.util.Success

class AddPropertiesToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    outputDepositDir = new File(testDir, "dd")
  )
  val datasetID = "ds1"

  before {
    new File(settings.outputDepositDir, s"md-$datasetID").mkdirs
  }

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "run" should "generate the properties file" in {
    AddPropertiesToDeposit(1, datasetID).run() shouldBe a[Success[_]]

    new File(outputDepositDir(settings, datasetID), "deposit.properties") should exist
  }

  "writeProperties" should "generate the properties file and write the properties in it" in {
    AddPropertiesToDeposit(1, datasetID).run() shouldBe a[Success[_]]

    val props = outputPropertiesFile(settings, datasetID)
    val content = props.read
    content should include ("state.label")
    content should include ("state.description")
    content should include ("depositor.userId")
  }
}
