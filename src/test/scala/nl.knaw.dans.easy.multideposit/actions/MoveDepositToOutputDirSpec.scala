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

import better.files.File
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import org.scalatest.BeforeAndAfterEach

import scala.util.Success

class MoveDepositToOutputDirSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val depositId = "ruimtereis01"
  private val action = new MoveDepositToOutputDir

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create stagingDir content
    if (stagingDir.exists) stagingDir.delete()
    stagingDir.createDirectory()
    stagingDir.toJava should exist

    File(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyTo(stagingDir("ruimtereis01"))
    File(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyTo(stagingDir("ruimtereis02"))

    stagingDir("ruimtereis01").toJava should exist
    stagingDir("ruimtereis02").toJava should exist

    if (outputDepositDir.exists) outputDepositDir.delete()
    outputDepositDir.createDirectory()
    outputDepositDir.toJava should exist
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    action.moveDepositsToOutputDir(depositId) shouldBe a[Success[_]]

    stagingDir(depositId).toJava shouldNot exist
    outputDepositDir(depositId).toJava should exist

    stagingDir("ruimtereis02").toJava should exist
    outputDepositDir("ruimtereis02").toJava shouldNot exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    action.moveDepositsToOutputDir(depositId) shouldBe a[Success[_]]

    stagingDir("ruimtereis02").toJava should exist
    outputDepositDir("ruimtereis02").toJava shouldNot exist
  }
}
