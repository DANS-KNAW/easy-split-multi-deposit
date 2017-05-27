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

import java.nio.file.{ Files, Paths }

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }

import scala.util.{ Failure, Success }

class MoveDepositToOutputDirSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = testDir.resolve("input"),
    stagingDir = testDir.resolve("sd"),
    outputDepositDir = testDir.resolve("dd")
  )

  override def beforeAll(): Unit = Files.createDirectories(testDir)

  before {
    // create stagingDir content
    val baseDir = settings.stagingDir
    Files.createDirectory(baseDir)
    baseDir.toFile should exist

    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyDir(stagingDir("ruimtereis01"))
    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyDir(stagingDir("ruimtereis02"))

    stagingDir("ruimtereis01").toFile should exist
    stagingDir("ruimtereis02").toFile should exist
  }

  after {
    // clean up stuff after the test is done
    val stagingDir = settings.stagingDir
    val outputDepositDir = settings.outputDepositDir

    for (dir <- List(stagingDir, outputDepositDir)) {
      dir.deleteDirectory()
      dir.toFile shouldNot exist
    }
  }

  override def afterAll: Unit = testDir.getParent.deleteDirectory()

  "checkPreconditions" should "verify that the deposit does not yet exist in the outputDepositDir" in {
    MoveDepositToOutputDir(1, "ruimtereis01").checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the deposit already exists in the outputDepositDir" in {
    val depositId = "ruimtereis01"
    stagingDir(depositId).copyDir(outputDepositDir(depositId))
    outputDepositDir(depositId).toFile should exist

    inside(MoveDepositToOutputDir(1, depositId).checkPreconditions) {
      case Failure(ActionException(1, msg, null)) => msg should include(s"The deposit for dataset $depositId already exists")
    }
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    val depositId = "ruimtereis01"
    MoveDepositToOutputDir(1, depositId).execute() shouldBe a[Success[_]]

    stagingDir(depositId).toFile shouldNot exist
    outputDepositDir(depositId).toFile should exist

    stagingDir("ruimtereis02").toFile should exist
    outputDepositDir("ruimtereis02").toFile shouldNot exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    val depositId = "ruimtereis01"
    MoveDepositToOutputDir(1, depositId).execute() shouldBe a[Success[_]]

    stagingDir("ruimtereis02").toFile should exist
    outputDepositDir("ruimtereis02").toFile shouldNot exist
  }
}
