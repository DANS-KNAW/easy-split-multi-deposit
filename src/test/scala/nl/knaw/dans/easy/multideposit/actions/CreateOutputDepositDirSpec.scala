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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.util.{Failure, Success}

class CreateOutputDepositDirSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    outputDepositDir = new File(testDir, "dd")
  )
  val datasetID = "ds1"

  override def beforeAll: Unit = testDir.mkdirs

  before {
    // create depositDir base directory
    val baseDir = settings.outputDepositDir
    baseDir.mkdir
    baseDir should exist
  }

  after {
    // clean up stuff after the test is done
    val baseDir = settings.outputDepositDir
    baseDir.deleteDirectory()
    baseDir should not (exist)
  }

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "succeed if the output directories do not yet exist" in {
    // directories do not exist before
    outputDepositDir(settings, datasetID) should not (exist)
    outputDepositBagDir(settings, datasetID) should not (exist)
    outputDepositBagMetadataDir(settings, datasetID) should not (exist)

    // creation of directories
    CreateOutputDepositDir(1, datasetID).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if either one of the output directories does already exist" in {
    outputDepositBagDir(settings, datasetID).mkdirs()

    // some directories do already exist before
    outputDepositDir(settings, datasetID) should exist
    outputDepositBagDir(settings, datasetID) should exist
    outputDepositBagMetadataDir(settings, datasetID) should not (exist)

    // creation of directories
    inside(CreateOutputDepositDir(1, datasetID).checkPreconditions) {
      case Failure(ActionException(row, msg, _)) =>
        row shouldBe 1
        msg should include s"The deposit for dataset $datasetID already exists"
    }
  }

  "execute" should "create the directories" in {
    // test is in seperate function,
    // since we want to reuse the code
    executeTest()
  }

  "rollback" should "delete the directories that were created in execute" in {
    // setup for this test
    executeTest()

    // roll back the creation of the directories
    CreateOutputDepositDir(1, datasetID).rollback() shouldBe a[Success[_]]

    // test that the directories are really not there anymore
    outputDepositDir(settings, datasetID) should not (exist)
    outputDepositBagDir(settings, datasetID) should not (exist)
    outputDepositBagMetadataDir(settings, datasetID) should not (exist)
  }

  def executeTest(): Unit = {
    // directories do not exist before
    outputDepositDir(settings, datasetID) should not (exist)
    outputDepositBagDir(settings, datasetID) should not (exist)
    outputDepositBagMetadataDir(settings, datasetID) should not (exist)

    // creation of directories
    CreateOutputDepositDir(1, datasetID).execute shouldBe a[Success[_]]

    // test existance after creation
    outputDepositDir(settings, datasetID) should exist
    outputDepositBagDir(settings, datasetID) should exist
    outputDepositBagMetadataDir(settings, datasetID) should exist
  }
}
