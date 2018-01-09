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

import java.nio.file.{ Files, Path }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import nl.knaw.dans.easy.multideposit2.model.Metadata
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

class ValidatePreconditionsSpec extends TestSupportFixture with BeforeAndAfterEach {
  self =>

  private val depositId = "dsId1"
  private val action = new ValidatePreconditions with StagingPathExplorer with InputPathExplorer {
    override val multiDepositDir: Path = self.multiDepositDir
    override val stagingDir: Path = self.stagingDir
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create depositDir base directory
    stagingDir.deleteDirectory()
    Files.createDirectory(stagingDir)
    stagingDir.toFile should exist
  }

  "checkDirectoriesDoNotExist" should "succeed if the directories do not yet exist" in {
    val dir = stagingBagDir(depositId)
    dir.toFile shouldNot exist

    action.checkDirectoriesDoNotExist(depositId)(dir) shouldBe a[Success[_]]
  }

  it should "fail if any of the directories already exist" in {
    val dir = stagingBagDir(depositId)
    Files.createDirectories(dir)
    dir.toFile should exist

    inside(action.checkDirectoriesDoNotExist(depositId)(dir)) {
      case Failure(ActionException(msg, _)) => msg should include(s"The deposit for dataset $depositId already exists")
    }
  }

  "checkSpringFieldDepositHasAVformat" should "fail if the deposit contains SF_* fields, but no AV DC_FORMAT is given" in {
    val deposit = testInstructions1.toDeposit(Seq.empty).copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("text/plain")
      )
    )
    inside(action.checkSpringFieldDepositHasAVformat(deposit)) {
      case Failure(ActionException(message, _)) =>
        message should include("No audio/video format found for this column: [DC_FORMAT]")
    }
  }

  it should "succeed if the deposit contains SF_* fields, and the DC_FORMAT contains audio/" in {
    val deposit = testInstructions1.toDeposit(Seq.empty).copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("audio/mpeg3")
      )
    )
    action.checkSpringFieldDepositHasAVformat(deposit) shouldBe a[Success[_]]
  }
}
