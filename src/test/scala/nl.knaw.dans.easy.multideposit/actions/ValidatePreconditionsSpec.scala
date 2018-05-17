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
import javax.naming.directory.Attributes

import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Audio, FileAccessRights, Metadata, Springfield, Video }
import nl.knaw.dans.easy.multideposit.{ FileExtensions, Ldap, TestSupportFixture }
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success, Try }

class ValidatePreconditionsSpec extends TestSupportFixture with BeforeAndAfterEach with MockFactory {

  private val depositId = "dsId1"
  private val ldapMock: Ldap = mock[Ldap]
  private val action = new ValidatePreconditions(ldapMock)

  override def beforeEach(): Unit = {
    super.beforeEach()

    stagingDir.deleteDirectory()
    Files.createDirectory(stagingDir)
    stagingDir.toFile should exist

    Paths.get(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyDir(stagingDir("ruimtereis01"))
    stagingDir("ruimtereis01").toFile should exist

    outputDepositDir.deleteDirectory()
    Files.createDirectory(outputDepositDir)
    outputDepositDir.toFile should exist
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

  "checkPreconditions" should "verify that the deposit does not yet exist in the outputDepositDir" in {
    action.checkOutputDirectoryExists("ruimtereis01") shouldBe a[Success[_]]
  }

  it should "fail if the deposit already exists in the outputDepositDir" in {
    val depositId = "ruimtereis01"
    stagingDir(depositId).copyDir(outputDepositDir(depositId))
    outputDepositDir(depositId).toFile should exist

    inside(action.checkOutputDirectoryExists(depositId)) {
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
      case Failure(InvalidInputException(_, message)) =>
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
      case Failure(InvalidInputException(_, message)) =>
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
      case Failure(InvalidInputException(_, message)) =>
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
      case Failure(InvalidInputException(_, message)) =>
        message shouldBe "Found both audio and video in this dataset. Only one of them is allowed."
    }
  }

  def mockLdapForDepositor(expectedResult: Try[Seq[Boolean]]): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects("dp1", *) returning expectedResult
  }

  "checkPreconditions" should "succeed if ldap identifies the depositorUserId as active" in {
    mockLdapForDepositor(Success(Seq(true)))

    action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit()) shouldBe a[Success[_]]
  }

  it should "fail if ldap identifies the depositorUserId as not active" in {
    mockLdapForDepositor(Success(Seq(false)))

    inside(action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit())) {
      case Failure(InvalidInputException(_, message)) => message should include("depositor 'dp1' is not an active user")
    }
  }

  it should "fail if ldap does not return anything for the depositor" in {
    mockLdapForDepositor(Success(Seq.empty))

    inside(action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit())) {
      case Failure(InvalidInputException(_, message)) => message should include("depositorUserId 'dp1' is unknown")
    }
  }

  it should "fail if ldap returns multiple values" in {
    mockLdapForDepositor(Success(Seq(true, true)))

    inside(action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit())) {
      case Failure(ActionException(message, _)) => message should include("multiple users with id 'dp1'")
    }
  }
}
