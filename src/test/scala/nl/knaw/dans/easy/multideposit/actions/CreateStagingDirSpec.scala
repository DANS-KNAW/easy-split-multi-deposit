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
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }

class CreateStagingDirSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "sd")
  )
  val datasetID = "ds1"

  before {
    // create depositDir base directory
    val baseDir = settings.stagingDir
    baseDir.mkdir
    baseDir should exist
  }

  after {
    // clean up stuff after the test is done
    val baseDir = settings.stagingDir
    baseDir.deleteDirectory()
    baseDir shouldNot exist
  }

  "checkPreconditions" should "succeed if the output directories do not yet exist" in {
    // directories do not exist before
    stagingDir(datasetID) shouldNot exist
    stagingBagDir(datasetID) shouldNot exist
    stagingBagMetadataDir(datasetID) shouldNot exist

    // creation of directories
    CreateStagingDir(1, datasetID).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if either one of the output directories does already exist" in {
    stagingBagDir(datasetID).mkdirs()

    // some directories do already exist before
    stagingDir(datasetID) should exist
    stagingBagDir(datasetID) should exist
    stagingBagMetadataDir(datasetID) shouldNot exist

    // creation of directories
    inside(CreateStagingDir(1, datasetID).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include (s"The deposit for dataset $datasetID already exists")
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
    CreateStagingDir(1, datasetID).rollback() shouldBe a[Success[_]]

    // test that the directories are really not there anymore
    stagingDir(datasetID) shouldNot exist
    stagingBagDir(datasetID) shouldNot exist
    stagingBagMetadataDir(datasetID) shouldNot exist
  }

  def executeTest(): Unit = {
    // directories do not exist before
    stagingDir(datasetID) shouldNot exist
    stagingBagDir(datasetID) shouldNot exist
    stagingBagMetadataDir(datasetID) shouldNot exist

    // creation of directories
    CreateStagingDir(1, datasetID).execute shouldBe a[Success[_]]

    // test existance after creation
    stagingDir(datasetID) should exist
    stagingBagDir(datasetID) should exist
    stagingBagMetadataDir(datasetID) should exist
  }
}
