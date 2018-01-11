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
package nl.knaw.dans.easy.multideposit2.actions

import java.nio.file.{ Files, Paths }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import org.scalatest.BeforeAndAfterEach

import scala.util.Success

class MoveDepositToOutputDirSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val depositId = "ruimtereis01"
  private val action = new MoveDepositToOutputDir

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create stagingDir content
    stagingDir.deleteDirectory()
    Files.createDirectory(stagingDir)
    stagingDir.toFile should exist

    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyDir(stagingDir("ruimtereis01"))
    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyDir(stagingDir("ruimtereis02"))

    stagingDir("ruimtereis01").toFile should exist
    stagingDir("ruimtereis02").toFile should exist

    outputDepositDir.deleteDirectory()
    Files.createDirectory(outputDepositDir)
    outputDepositDir.toFile should exist
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    action.moveDepositsToOutputDir(depositId) shouldBe a[Success[_]]

    stagingDir(depositId).toFile shouldNot exist
    outputDepositDir(depositId).toFile should exist

    stagingDir("ruimtereis02").toFile should exist
    outputDepositDir("ruimtereis02").toFile shouldNot exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    action.moveDepositsToOutputDir(depositId) shouldBe a[Success[_]]

    stagingDir("ruimtereis02").toFile should exist
    outputDepositDir("ruimtereis02").toFile shouldNot exist
  }
}
