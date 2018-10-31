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
package nl.knaw.dans.easy.multideposit.parser

import better.files.File
import better.files.File.currentWorkingDirectory
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

class ParserValidationSpec extends TestSupportFixture with BeforeAndAfterEach with MockFactory {

  private val depositId = "dsId1"
  private val validation = new ParserValidation {}

  override def beforeEach(): Unit = {
    super.beforeEach()

    // create depositDir base directory
    if (stagingDir.exists) stagingDir.delete()
    stagingDir.createDirectory()
    stagingDir.toJava should exist

    File(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyTo(stagingDir("ruimtereis01"))
    stagingDir("ruimtereis01").toJava should exist

    if (outputDepositDir.exists) outputDepositDir.delete()
    outputDepositDir.createDirectory()
    outputDepositDir.toJava should exist
  }

  "checkSpringFieldDepositHasAVformat" should "fail if the deposit contains SF_* fields, but no AV DC_FORMAT is given" in {
    val deposit = testInstructions1.toDeposit().copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("text/plain")
      )
    )
    inside(validation.checkSpringFieldDepositHasAVformat(deposit)) {
      case Failure(ParseException(_, message, _)) =>
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
    validation.checkSpringFieldDepositHasAVformat(deposit) shouldBe a[Success[_]]
  }

  val avFileReferences = Seq(
    AVFileMetadata(
      filepath = testDir / "md" / "ruimtereis01" / "reisverslag" / "centaur.mpg",
      mimeType = "video/mpeg",
      vocabulary = Video,
      title = "flyby of centaur",
      accessibleTo = FileAccessRights.ANONYMOUS,
      visibleTo = FileAccessRights.ANONYMOUS
    ))

  "checkSFColumnsIfDepositContainsAVFiles" should "succeed if the deposit contains the SF_* fields in case an A/V file is found" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option(Springfield("domain", "user", "collection", PlayMode.Continuous))
    )
    validation.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  it should "fail if the deposit contains A/V files but the SF_* fields are not present" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option.empty
    )
    inside(validation.checkSFColumnsIfDepositContainsAVFiles(deposit)) {
      case Failure(ParseException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION, SF_PLAY_MODE]") and
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
    validation.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  it should "fail if the deposit contains no A/V files and any of the SF_* fields are present" in {
    val depositId = "ruimtereis02"
    val deposit = testInstructions2.toDeposit().copy(
      row = 1,
      depositId = depositId,
      springfield = Option(Springfield(user = "user", collection = "collection", playMode = PlayMode.Continuous))
    )
    inside(validation.checkSFColumnsIfDepositContainsAVFiles(deposit)) {
      case Failure(ParseException(_, message, _)) =>
        message should include("Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]; these columns should be empty because there are no audio/video files found in this deposit")
    }
  }

  it should "create an empty list of file metadata if the deposit directory corresponding with the depositId does not exist and therefore succeed" in {
    val depositId = "ruimtereis03"
    val deposit = testInstructions2.copy(depositId = depositId).toDeposit()
    depositDir(depositId).toJava should not(exist)
    validation.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  "checkEitherVideoOrAudio" should "fail if a dataset has both audio and video material in it" in {
    val depositId = "ruimtereis01"
    val deposit = testInstructions1.copy(depositId = depositId)
      .toDeposit(avFileReferences :+ AVFileMetadata(
        filepath = currentWorkingDirectory,
        mimeType = "audio/mpeg",
        vocabulary = Audio,
        title = "mytitle",
        accessibleTo = FileAccessRights.ANONYMOUS,
        visibleTo = FileAccessRights.ANONYMOUS
      ))

    inside(validation.checkEitherVideoOrAudio(deposit)) {
      case Failure(ParseException(_, message, _)) =>
        message shouldBe "Found both audio and video in this dataset. Only one of them is allowed."
    }
  }
}
