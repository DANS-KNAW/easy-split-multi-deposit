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

import java.nio.file.Files

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }

class CreateDirectoriesSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = testDir.resolve("md"),
    stagingDir = testDir.resolve("sd")
  )
  private val depositId = "ds1"
  private val action = CreateDirectories(stagingDir(depositId), stagingBagDir(depositId))(1, depositId)

  before {
    // create depositDir base directory
    val baseDir = settings.stagingDir
    Files.createDirectory(baseDir)
    baseDir.toFile should exist
  }

  "checkPreconditions" should "succeed if the output directories do not yet exist" in {
    // directories do not exist before
    stagingDir(depositId).toFile shouldNot exist
    stagingBagDir(depositId).toFile shouldNot exist

    // creation of directories
    action.checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if either one of the output directories does already exist" in {
    Files.createDirectories(stagingBagDir(depositId))

    // some directories do already exist before
    stagingDir(depositId).toFile should exist
    stagingBagDir(depositId).toFile should exist

    // creation of directories
    inside(action.checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include(s"The deposit for dataset $depositId already exists")
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
    action.rollback() shouldBe a[Success[_]]

    // test that the directories are really not there anymore
    stagingDir(depositId).toFile shouldNot exist
    stagingBagDir(depositId).toFile shouldNot exist
  }

  def executeTest(): Unit = {
    // directories do not exist before
    stagingDir(depositId).toFile shouldNot exist
    stagingBagDir(depositId).toFile shouldNot exist

    // creation of directories
    action.execute shouldBe a[Success[_]]

    // test existance after creation
    stagingDir(depositId).toFile should exist
    stagingBagDir(depositId).toFile should exist
  }
}
