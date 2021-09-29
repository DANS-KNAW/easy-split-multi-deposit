/*
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

import java.util.UUID

import better.files.File
import nl.knaw.dans.easy.multideposit.{ ActionError, TestSupportFixture }
import org.scalatest.BeforeAndAfterEach

class MoveDepositToOutputDirSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val depositId1 = "ruimtereis01"
  private val depositId2 = "ruimtereis02"
  private val action = new MoveDepositToOutputDir

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create stagingDir content
    if (stagingDir.exists) stagingDir.delete()
    stagingDir.createDirectory()
    stagingDir.toJava should exist

    File(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyTo(stagingDir(depositId1))
    File(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyTo(stagingDir(depositId2))

    stagingDir(depositId1).toJava should exist
    stagingDir(depositId2).toJava should exist

    if (outputDepositDir.exists) outputDepositDir.delete()
    outputDepositDir.createDirectory()
    outputDepositDir.toJava should exist
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    val bagId = UUID.randomUUID()

    action.moveDepositsToOutputDir(depositId1, bagId) shouldBe right[Unit]

    stagingDir(depositId1).toJava shouldNot exist
    outputDepositDir(bagId).toJava should exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    val bagId = UUID.randomUUID()

    action.moveDepositsToOutputDir(depositId1, bagId) shouldBe right[Unit]

    stagingDir(depositId2).toJava should exist
    // even though ruimtereis02 is staged as well, it is not moved to the outputDepositDir
    outputDepositDir.list.toList should contain only outputDepositDir(bagId)
  }

  it should "fail when the output directory already exists" in {
    val bagId = UUID.randomUUID()

    outputDepositDir(bagId).toJava shouldNot exist
    outputDepositDir(bagId).createIfNotExists(asDirectory = true, createParents = true)
    outputDepositDir(bagId).toJava should exist

    inside(action.moveDepositsToOutputDir(depositId1, bagId).leftValue) {
      case ActionError(msg, None) =>
        msg should startWith(s"Could not move ${ stagingDir(depositId1) } to " +
          s"${ outputDepositDir(bagId) }. The target directory already exists.")
    }
  }
}
