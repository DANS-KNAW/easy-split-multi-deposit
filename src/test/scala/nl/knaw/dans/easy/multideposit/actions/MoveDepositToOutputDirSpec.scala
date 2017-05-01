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

import java.io.File

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }

import scala.util.{ Failure, Success }

class MoveDepositToOutputDirSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "input"),
    stagingDir = new File(testDir, "sd"),
    outputDepositDir = new File(testDir, "dd")
  )

  override def beforeAll(): Unit = testDir.mkdirs

  before {
    // create stagingDir content
    val baseDir = settings.stagingDir
    baseDir.mkdir()
    baseDir should exist

    new File(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyDir(stagingDir("ruimtereis01"))
    new File(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyDir(stagingDir("ruimtereis02"))

    stagingDir("ruimtereis01") should exist
    stagingDir("ruimtereis02") should exist
  }

  after {
    // clean up stuff after the test is done
    val stagingDir = settings.stagingDir
    val outputDepositDir = settings.outputDepositDir

    for (dir <- List(stagingDir, outputDepositDir)) {
      dir.deleteDirectory()
      dir shouldNot exist
    }
  }

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "verify that the deposit does not yet exist in the outputDepositDir" in {
    MoveDepositToOutputDir(1, "ruimtereis01").checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the deposit already exists in the outputDepositDir" in {
    val datasetID = "ruimtereis01"
    stagingDir(datasetID).copyDir(outputDepositDir(datasetID))
    outputDepositDir(datasetID) should exist

    inside(MoveDepositToOutputDir(1, datasetID).checkPreconditions) {
      case Failure(ActionException(1, msg, null)) => msg should include(s"The deposit for dataset $datasetID already exists")
    }
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    val datasetID = "ruimtereis01"
    MoveDepositToOutputDir(1, datasetID).execute() shouldBe a[Success[_]]

    stagingDir(datasetID) shouldNot exist
    outputDepositDir(datasetID) should exist

    stagingDir("ruimtereis02") should exist
    outputDepositDir("ruimtereis02") shouldNot exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    val datasetID = "ruimtereis01"
    MoveDepositToOutputDir(1, datasetID).execute() shouldBe a[Success[_]]

    stagingDir("ruimtereis02") should exist
    outputDepositDir("ruimtereis02") shouldNot exist
  }
}
