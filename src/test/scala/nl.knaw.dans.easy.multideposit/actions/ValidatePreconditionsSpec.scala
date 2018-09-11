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
import better.files.File.currentWorkingDirectory
import javax.naming.directory.Attributes
import nl.knaw.dans.easy.multideposit.model.{ Audio, AVFileMetadata, FileAccessRights, Metadata, Springfield, Video }
import nl.knaw.dans.easy.multideposit.{ FfprobeRunner, Ldap, TestSupportFixture }
import nl.knaw.dans.lib.error.CompositeException
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success, Try }

class ValidatePreconditionsSpec extends TestSupportFixture with BeforeAndAfterEach with MockFactory {

  private val depositId = "dsId1"
  private val ldapMock: Ldap = mock[Ldap]
  private val ffprobeMock: FfprobeRunner = mock[FfprobeRunner]
  private val action = new ValidatePreconditions(ldapMock, ffprobeMock)

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

  "checkDirectoriesDoNotExist" should "succeed if the directories do not yet exist" in {
    val dir = stagingBagDir(depositId)
    dir.toJava shouldNot exist

    action.checkDirectoriesDoNotExist(depositId)(dir) shouldBe a[Success[_]]
  }

  it should "fail if any of the directories already exist" in {
    val dir = stagingBagDir(depositId)
    dir.createDirectories()
    dir.toJava should exist

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
    depositDir(depositId).toJava should not(exist)
    action.checkSFColumnsIfDepositContainsAVFiles(deposit) shouldBe a[Success[_]]
  }

  it should "fail if a dataset has both audio and video material in it" in {
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

    inside(action.checkEitherVideoOrAudio(deposit)) {
      case Failure(InvalidInputException(_, message)) =>
        message shouldBe "Found both audio and video in this dataset. Only one of them is allowed."
    }
  }

  def mockLdapForDepositor(expectedResult: Try[Seq[Boolean]]): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects("dp1", *) returning expectedResult
  }

  "checkDepositorUserId" should "succeed if ldap identifies the depositorUserId as active" in {
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

  def mockFfprobeRunnerForAllSuccess(): Unit = {
    (ffprobeMock.run(_: File)) expects * anyNumberOfTimes() returning Success(())
  }

  def mockFfprobeRunnerForOneFailure(): Unit = {
    (ffprobeMock.run(_: File)) expects * once() returning Failure(CompositeException(Seq(FfprobeErrorException(File("dummy"), 0, "dummy"))))
  }

  "checkAudioVideoNotCorrupt" should "succeed if no A/V files are present" in {
    mockFfprobeRunnerForAllSuccess()
    action.checkAudioVideoNotCorrupt(testInstructions2.toDeposit()) shouldBe a[Success[_]]
  }

  it should "succeed if A/V files are present and ffprobe returns 0 for all of them" in {
    mockFfprobeRunnerForAllSuccess()
    val deposit = testInstructions1.toDeposit(avFileReferences)
    action.checkAudioVideoNotCorrupt(deposit) shouldBe a[Success[_]]
  }

  it should "fail if one A/V file makes ffprobe return nonzero" in {
    mockFfprobeRunnerForOneFailure()
    val deposit = testInstructions1.toDeposit(avFileReferences)

    inside(action.checkAudioVideoNotCorrupt(deposit)) {
      case Failure(InvalidInputException(row, msg)) =>
        row shouldBe testInstructions1.row
        msg should include("Possibly found corrupt A/V files.")
    }
  }
}
