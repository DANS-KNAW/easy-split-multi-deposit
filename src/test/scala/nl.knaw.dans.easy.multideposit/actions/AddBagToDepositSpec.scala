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

import java.security.MessageDigest
import java.util.UUID

import better.files.File
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.model.DepositId
import nl.knaw.dans.easy.multideposit.{ TestSupportFixture, encoding }
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach

class AddBagToDepositSpec extends TestSupportFixture with BeforeAndAfterEach {

  private val depositId = "dsId1"
  private val date = new DateTime(1992, 7, 30, 0, 0)
  private val base = UUID.fromString("1de3f841-0f0d-048b-b3db-4b03ad4834d7")
  private val file1Text = "abcdef"
  private val file2Text = "defghi"
  private val file3Text = "ghijkl"
  private val file4Text = "jklmno"
  private val file5Text = "mnopqr"
  private val action = new AddBagToDeposit

  override def beforeEach(): Unit = {
    super.beforeEach()

    (depositDir(depositId) / "file1.txt")
      .createIfNotExists(createParents = true)
      .write(file1Text)
    (depositDir(depositId) / "folder1" / "file2.txt")
      .createIfNotExists(createParents = true)
      .write(file2Text)
    (depositDir(depositId) / "folder1" / "file3.txt")
      .createIfNotExists(createParents = true)
      .write(file3Text)
    (depositDir(depositId) / "folder2" / "file4.txt")
      .createIfNotExists(createParents = true)
      .write(file4Text)
    (depositDir("ruimtereis02") / "folder3" / "file5.txt")
      .createIfNotExists(createParents = true)
      .write(file5Text)

    if (stagingDir.exists) stagingDir.delete()
    stagingBagDir(depositId).createDirectories()
  }

  "addBagToDeposit" should "succeed given the current setup" in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]
  }

  it should "create a bag with the files from ruimtereis01 in it and some meta-files around" in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    val root = stagingBagDir(depositId)
    root.toJava should exist
    root.walk().map {
      case file if file.isDirectory => file.name + "/"
      case file => file.name
    }.toList should contain theSameElementsAs
      List("bag/",
        "bag-info.txt",
        "bagit.txt",
        "data/",
        "file1.txt",
        "folder1/",
        "file2.txt",
        "file3.txt",
        "folder2/",
        "file4.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")
    stagingBagDataDir(depositId).toJava should exist
  }

  it should "preserve the file content after making the bag" in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    val root = stagingBagDataDir(depositId)
    (root / "file1.txt").contentAsString shouldBe file1Text
    (root / "folder1/file2.txt").contentAsString shouldBe file2Text
    (root / "folder1/file3.txt").contentAsString shouldBe file3Text
    (root / "folder2/file4.txt").contentAsString shouldBe file4Text
  }

  it should "create a bag with no files in data when the input directory does not exist" in {
    implicit val inputPathExplorer: InputPathExplorer = new InputPathExplorer {
      val multiDepositDir: File = testDir / "md-empty"
    }

    val bagDir = stagingBagDir(depositId)
    bagDir.createDirectories()
    bagDir.toJava should exist

    inputPathExplorer.depositDir(depositId).toJava shouldNot exist

    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    stagingDir(depositId).toJava should exist
    stagingBagDataDir(depositId).toJava should exist
    stagingBagDataDir(depositId).listRecursively.filterNot(_.isDirectory) shouldBe empty
    bagDir.listRecursively.filterNot(_.isDirectory).map(_.name).toList should contain theSameElementsAs
      List("bag-info.txt",
        "bagit.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")

    (bagDir / "manifest-sha1.txt").contentAsString shouldBe empty
    (bagDir / "tagmanifest-sha1.txt").contentAsString should include("bag-info.txt")
    (bagDir / "tagmanifest-sha1.txt").contentAsString should include("bagit.txt")
    (bagDir / "tagmanifest-sha1.txt").contentAsString should include("manifest-sha1.txt")
  }

  it should "contain the date-created in the bag-info.txt" in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    val bagInfo = stagingBagDir(depositId) / "bag-info.txt"
    bagInfo.toJava should exist

    bagInfo.contentAsString should include("Created")
  }

  it should "contain the Is-Version-Of in the bag-info.txt if the Option[BaseUUID] is Some " in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    val bagInfo = stagingBagDir(depositId) / "bag-info.txt"
    bagInfo.toJava should exist

    bagInfo.contentAsString should include(s"Is-Version-Of: urn:uuid:$base")
  }

  it should "not contain the Is-Version-Of in the bag-info.txt if the Option[BaseUUID] is None " in {
    action.addBagToDeposit(depositId, date, None) shouldBe right[Unit]

    val bagInfo = stagingBagDir(depositId) / "bag-info.txt"
    bagInfo.toJava should exist

    bagInfo.contentAsString should not include "Is-Version-Of"
  }

  it should "contain the correct checksums in its manifest file" in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    verifyChecksums(depositId, "manifest-sha1.txt")
  }

  it should "contain the correct checksums in its tagmanifest file" in {
    action.addBagToDeposit(depositId, date, Some(base)) shouldBe right[Unit]

    verifyChecksums(depositId, "tagmanifest-sha1.txt")
  }

  def verifyChecksums(depositId: DepositId, manifestFile: String): Unit = {
    val root = stagingBagDir(depositId)
    (root / manifestFile).contentAsString
      .split('\n')
      .map(_.split("  "))
      .foreach {
        case Array(sha1, file) => calcSHA1((root / file).contentAsString) shouldBe sha1
        case line => fail(s"unexpected line detected: ${ line.mkString("  ") }")
      }
  }

  def calcSHA1(string: String): String = {
    MessageDigest.getInstance(StandardSupportedAlgorithms.SHA1.getMessageDigestName)
      .digest(string.getBytes(encoding))
      .map("%02x".format(_))
      .mkString
  }
}
