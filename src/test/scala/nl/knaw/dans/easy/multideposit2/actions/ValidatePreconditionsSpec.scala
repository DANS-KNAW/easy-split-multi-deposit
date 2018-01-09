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

import java.nio.file.{ Files, Path, Paths }

import nl.knaw.dans.easy.multideposit.FileExtensions
import nl.knaw.dans.easy.multideposit2.PathExplorer.{ InputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit2.TestSupportFixture
import nl.knaw.dans.easy.multideposit2.model.{ AVFileMetadata, Audio, FileAccessRights, Metadata, Springfield, Video }
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
    val deposit = testInstructions1.toDeposit().copy(
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
    val deposit = testInstructions1.toDeposit().copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("audio/mpeg3")
      )
    )
    action.checkSpringFieldDepositHasAVformat(deposit) shouldBe a[Success[_]]
  }

  val avFileReferences = Seq(
    AVFileMetadata(
      filepath = testDir.resolve("md/ruimtereis01/reisverslag/centaur.mpg"),
      mimeType = "video/mpeg",
      vocabulary = Video,
      title = "flyby of centaur",
      accessibleTo = FileAccessRights.ANONYMOUS
    ))

  "checkSFColumnsIfDepositContainsAVFiles" should "succeed if the deposit contains the SF_* fields in case an A/V file is found" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option(Springfield("domain", "user", "collection"))
    )
    action.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  it should "fail if the deposit contains A/V files but the SF_* fields are not present" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option.empty
    )
    inside(action.checkSFColumnsIfDepositContainsAVFiles(deposit)) {
      case Failure(ActionException(message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg")
        }
    }
  }

  it should "succeed if the deposit contains no A/V files and the SF_* fields are not present" in {
    val depositId = "ruimtereis02"
    val deposit = testInstructions2.toDeposit().copy(
      depositId = depositId,
      springfield = Option.empty
    )
    action.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  it should "fail if the deposit contains no A/V files and any of the SF_* fields are present" in {
    val depositId = "ruimtereis02"
    val deposit = testInstructions2.toDeposit().copy(
      row = 1,
      depositId = depositId,
      springfield = Option(Springfield(user = "user", collection = "collection"))
    )
    inside(action.checkSFColumnsIfDepositContainsAVFiles(deposit)) {
      case Failure(ActionException(message, _)) =>
        message should {
          include("Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]") and
            include("these columns should be empty because there are no audio/video files found in this deposit")
        }
    }
  }

  it should "create an empty list of file metadata if the deposit directory corresponding with the depositId does not exist and therefore succeed" in {
    val depositId = "ruimtereis03"
    val deposit = testInstructions2.copy(depositId = depositId).toDeposit()
    depositDir(depositId).toFile should not(exist)
    action.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  it should "fail if a dataset has both audio and video material in it" in {
    val depositId = "ruimtereis01"
    val deposit = testInstructions1.copy(depositId = depositId)
      .toDeposit(avFileReferences :+ AVFileMetadata(
        filepath = Paths.get(""),
        mimeType = "audio/mpeg",
        vocabulary = Audio,
        title = "mytitle",
        accessibleTo = FileAccessRights.ANONYMOUS
      ))

    inside(action.checkEitherVideoOrAudio(deposit)) {
      case Failure(ActionException(message, _)) =>
        message shouldBe "Found both audio and video in this dataset. Only one of them is allowed."
    }
  }
}
