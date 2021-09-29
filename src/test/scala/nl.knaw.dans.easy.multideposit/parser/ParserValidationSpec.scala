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
package nl.knaw.dans.easy.multideposit.parser

import better.files.File
import better.files.File.currentWorkingDirectory
import cats.data.NonEmptyList
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

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

  private val avFileReferences = Seq(
    AVFileMetadata(
      filepath = testDir / "md" / "ruimtereis01" / "reisverslag" / "centaur.mpg",
      mimeType = "video/mpeg",
      vocabulary = Video,
      title = "flyby of centaur",
      accessibleTo = FileAccessRights.ANONYMOUS,
      visibleTo = FileAccessRights.ANONYMOUS
    )
  )

  private val avFileReferences2 = Seq(
    AVFileMetadata(
      filepath = testDir / "md" / "ruimtereis01" / "reisverslag" / "big centaur.mpg",
      mimeType = "video/mpeg",
      vocabulary = Video,
      title = "flyby of centaur",
      accessibleTo = FileAccessRights.ANONYMOUS,
      visibleTo = FileAccessRights.ANONYMOUS
    )
  )

  "checkUserLicenseOnlyWithOpenAccess" should "succeed when accessright=OPEN_ACCESS and user license is given" in {
    val baseDeposit = testInstructions1.toDeposit()
    val deposit = baseDeposit.copy(
      depositId = depositId,
      profile = baseDeposit.profile.copy(
        accessright = AccessCategory.OPEN_ACCESS,
      ),
      metadata = baseDeposit.metadata.copy(
        userLicense = Option(UserLicense("http://creativecommons.org/licenses/by-nc-sa/4.0/")),
      ),
    )

    validation.checkUserLicenseOnlyWithOpenAccess(deposit).isValid shouldBe true
  }

  it should "fail when accessright=OPEN_ACCESS and no user license is given" in {
    val baseDeposit = testInstructions1.toDeposit()
    val deposit = baseDeposit.copy(
      depositId = depositId,
      profile = baseDeposit.profile.copy(
        accessright = AccessCategory.OPEN_ACCESS,
      ),
      metadata = baseDeposit.metadata.copy(
        userLicense = Option.empty,
      ),
    )
    val message = s"When access right '${ AccessCategory.OPEN_ACCESS }' is used, a user license must be specified as well."

    validation.checkUserLicenseOnlyWithOpenAccess(deposit) shouldBe ParseError(2, message).toInvalid
  }

  it should "fail when accessright=/=OPEN_ACCESS and user license is given" in {
    val baseDeposit = testInstructions1.toDeposit()
    val deposit = baseDeposit.copy(
      depositId = depositId,
      profile = baseDeposit.profile.copy(
        accessright = AccessCategory.REQUEST_PERMISSION,
      ),
      metadata = baseDeposit.metadata.copy(
        userLicense = Option(UserLicense("http://creativecommons.org/licenses/by-nc-sa/4.0/")),
      ),
    )
    val message = s"When access right '${ AccessCategory.OPEN_ACCESS }' is used, a user license must be specified as well."

    validation.checkUserLicenseOnlyWithOpenAccess(deposit) shouldBe ParseError(2, message).toInvalid
  }

  it should "succeed when accessright=/=OPEN_ACCESS and no user license is given" in {
    val baseDeposit = testInstructions1.toDeposit()
    val deposit = baseDeposit.copy(
      depositId = depositId,
      profile = baseDeposit.profile.copy(
        accessright = AccessCategory.REQUEST_PERMISSION,
      ),
      metadata = baseDeposit.metadata.copy(
        userLicense = Option.empty,
      ),
    )

    validation.checkUserLicenseOnlyWithOpenAccess(deposit).isValid shouldBe true
  }

  "checkSpringFieldDepositHasAVformat" should "fail if the deposit contains SF_* fields, but no AV DC_FORMAT is given" in {
    val deposit = testInstructions1.toDeposit().copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("text/plain"),
        rightsholder = NonEmptyList.of("morpheus"),
      )
    )

    inside(validation.checkSpringFieldDepositHasAVformat(deposit).invalidValue.toNonEmptyList.toList) {
      case List(ParseError(_, message)) =>
        message should include("No audio/video format found for this column: [DC_FORMAT]")
    }
  }

  it should "succeed if the deposit contains SF_* fields, and the DC_FORMAT contains audio/" in {
    val deposit = testInstructions1.toDeposit().copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("audio/mpeg3"),
        rightsholder = NonEmptyList.of("Smith"),
      )
    )
    validation.checkSpringFieldDepositHasAVformat(deposit).isValid shouldBe true
  }

  "checkSFColumnsIfDepositContainsAVFiles" should "succeed if the deposit contains the SF_* fields in case an A/V file is found" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option(Springfield("domain", "user", "collection", PlayMode.Continuous))
    )
    validation.checkSFColumnsIfDepositContainsAVFiles(deposit).isValid shouldBe true
  }

  it should "fail if the deposit contains A/V files but the SF_* fields are not present" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option.empty
    )

    inside(validation.checkSFColumnsIfDepositContainsAVFiles(deposit).invalidValue.toNonEmptyList.toList) {
      case List(ParseError(_, message)) =>
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

    validation.checkSFColumnsIfDepositContainsAVFiles(deposit).isValid shouldBe true
  }

  it should "fail if the deposit contains no A/V files and any of the SF_* fields are present" in {
    val depositId = "ruimtereis02"
    val deposit = testInstructions2.toDeposit().copy(
      row = 1,
      depositId = depositId,
      springfield = Option(Springfield(user = "user", collection = "collection", playMode = PlayMode.Continuous))
    )

    inside(validation.checkSFColumnsIfDepositContainsAVFiles(deposit).invalidValue.toNonEmptyList.toList) {
      case List(ParseError(_, message)) =>
        message should include("Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION, SF_PLAY_MODE]; these columns should be empty because there are no audio/video files found in this deposit")
    }
  }

  it should "create an empty list of file metadata if the deposit directory corresponding with the depositId does not exist and therefore succeed" in {
    val depositId = "ruimtereis03"
    val deposit = testInstructions2.copy(depositId = depositId).toDeposit()
    depositDir(depositId).toJava should not(exist)

    validation.checkSFColumnsIfDepositContainsAVFiles(deposit).isValid shouldBe true
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

    validation.checkEitherVideoOrAudio(deposit) shouldBe
      ParseError(2, "Found both audio and video in this dataset. Only one of them is allowed.").toInvalid
  }

  "checkAllAVFilesHaveSameAccessibility" should "fail if a dataset has multiple accessibleTo levels for A/V files" in {
    val depositId = "ruimtereis01"
    val deposit = testInstructions1.copy(depositId = depositId)
      .toDeposit(avFileReferences :+ AVFileMetadata(
        filepath = testDir / "md" / "ruimtereis01" / "reisverslag" / "centaur.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "flyby of centaur",
        accessibleTo = FileAccessRights.RESTRICTED_REQUEST,
        visibleTo = FileAccessRights.ANONYMOUS
      ))

    validation.checkAllAVFilesHaveSameAccessibility(deposit) shouldBe
      ParseError(2, "Multiple accessibility levels found for A/V files: {ANONYMOUS, RESTRICTED_REQUEST}").toInvalid
  }

  "checkAllAVFileNamesWithoutSpaces" should "succeed when there are no spaces in A/V filenames" in {
    val deposit = testInstructions1.toDeposit(avFileReferences).copy(
      depositId = depositId,
      springfield = Option(Springfield("domain", "user", "collection", PlayMode.Continuous))
    )
    validation.checkAllAVFileNamesWithoutSpaces(deposit).isValid shouldBe true
  }

  it should "fail when there are spaces in A/V filenames" in {
    val deposit = testInstructions1.toDeposit(avFileReferences2).copy(
      depositId = depositId,
      springfield = Option(Springfield("domain", "user", "collection", PlayMode.Continuous))
    )
    validation.checkAllAVFileNamesWithoutSpaces(deposit) shouldBe
      ParseError(2, "A/V filename 'big centaur.mpg' contains spaces").toInvalid
  }
}
