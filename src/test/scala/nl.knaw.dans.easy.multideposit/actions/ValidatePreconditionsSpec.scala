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
import javax.naming.directory.Attributes
import nl.knaw.dans.easy.multideposit.FfprobeRunner.FfprobeError
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, FileAccessRights, Video }
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

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

    action.checkDirectoriesDoNotExist(depositId)(dir) shouldBe right[Unit]
  }

  it should "fail if any of the directories already exist" in {
    val dir = stagingBagDir(depositId)
    dir.createDirectories()
    dir.toJava should exist

    action.checkDirectoriesDoNotExist(depositId)(dir).leftValue.msg should
      include(s"The deposit for dataset $depositId already exists")
  }

  private val avFileReferences = Seq(
    AVFileMetadata(
      filepath = testDir / "md" / "ruimtereis01" / "reisverslag" / "centaur.mpg",
      mimeType = "video/mpeg",
      vocabulary = Video,
      title = "flyby of centaur",
      accessibleTo = FileAccessRights.ANONYMOUS,
      visibleTo = FileAccessRights.ANONYMOUS
    ))

  def mockLdapForDepositor(expectedResult: Seq[Boolean]): Unit = {
    (ldapMock.query(_: String)(_: Attributes => Boolean)) expects("dp1", *) returning Right(expectedResult)
  }

  "checkDepositorUserId" should "succeed if ldap identifies the depositorUserId as active" in {
    mockLdapForDepositor(Seq(true))

    action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit()) shouldBe right[Unit]
  }

  it should "fail if ldap identifies the depositorUserId as not active" in {
    mockLdapForDepositor(Seq(false))

    action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit())
      .leftValue.msg should include("depositor 'dp1' is not an active user")
  }

  it should "fail if ldap does not return anything for the depositor" in {
    mockLdapForDepositor(Seq.empty)

    action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit())
      .leftValue.msg should include("depositorUserId 'dp1' is unknown")
  }

  it should "fail if ldap returns multiple values" in {
    mockLdapForDepositor(Seq(true, true))

    action.checkDepositorUserId(testInstructions1.copy(depositorUserId = "dp1").toDeposit())
      .leftValue.msg should include("multiple users with id 'dp1'")
  }

  def mockFfprobeRunnerForAllSuccess(): Unit = {
    (ffprobeMock.run(_: File)) expects * anyNumberOfTimes() returning Right(())
  }

  // TODO remove CompositeException
  def mockFfprobeRunnerForOneFailure(): Unit = {
    (ffprobeMock.run(_: File)) expects * once() returning Left(FfprobeError(File("dummy"), 0, "dummy"))
  }

  "checkAudioVideoNotCorrupt" should "succeed if no A/V files are present" in {
    mockFfprobeRunnerForAllSuccess()
    action.checkAudioVideoNotCorrupt(testInstructions2.toDeposit()) shouldBe right[Unit]
  }

  it should "succeed if A/V files are present and ffprobe returns 0 for all of them" in {
    mockFfprobeRunnerForAllSuccess()
    val deposit = testInstructions1.toDeposit(avFileReferences)
    action.checkAudioVideoNotCorrupt(deposit) shouldBe right[Unit]
  }

  it should "fail if one A/V file makes ffprobe return nonzero" in {
    mockFfprobeRunnerForOneFailure()
    val deposit = testInstructions1.toDeposit(avFileReferences)

    inside(action.checkAudioVideoNotCorrupt(deposit)) {
      case Left(InvalidInput(row, msg)) =>
        row shouldBe testInstructions1.row
        msg should include("Possibly found corrupt A/V files.")
    }
  }
}
