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

import java.nio.file.Files
import java.security.MessageDigest

import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms
import nl.knaw.dans.easy.multideposit.model.{ Deposit, DepositId }
import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

class AddBagToDepositSpec extends UnitSpec with BeforeAndAfterEach {

  implicit val settings = Settings(
    multidepositDir = testDir.resolve("md"),
    stagingDir = testDir.resolve("sd")
  )

  val depositId = "ruimtereis01"
  val file1Text = "abcdef"
  val file2Text = "defghi"
  val file3Text = "ghijkl"
  val file4Text = "jklmno"
  val file5Text = "mnopqr"
  val deposit: Deposit = testDeposit1

  override def beforeEach(): Unit = {
    super.beforeEach()

    multiDepositDir(depositId).resolve("file1.txt").write(file1Text)
    multiDepositDir(depositId).resolve("folder1/file2.txt").write(file2Text)
    multiDepositDir(depositId).resolve("folder1/file3.txt").write(file3Text)
    multiDepositDir(depositId).resolve("folder2/file4.txt").write(file4Text)
    multiDepositDir("ruimtereis02").resolve("folder3/file5.txt").write("file5Text")

    Files.createDirectories(stagingBagDir(depositId))
  }

  "execute" should "succeed given the current setup" in {
    AddBagToDeposit(deposit).execute shouldBe a[Success[_]]
  }

  it should "create a bag with the files from ruimtereis01 in it and some meta-files around" in {
//    AddBagToDeposit(deposit).execute shouldBe a[Success[_]]
    AddBagToDeposit(deposit).execute match {
      case Failure(e) => fail(e)
      case Success(_) =>
    }

    val root = stagingBagDir(depositId)
    root.toFile should exist
    root.listRecursively().map {
      case file if Files.isDirectory(file) => file.getFileName.toString + "/"
      case file => file.getFileName.toString
    } should contain theSameElementsAs
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
    stagingBagDataDir(depositId).toFile should exist
  }

  it should "preserve the file content after making the bag" in {
    AddBagToDeposit(deposit).execute shouldBe a[Success[_]]

    val root = stagingBagDataDir(depositId)
    root.resolve("file1.txt").read() shouldBe file1Text
    root.resolve("folder1/file2.txt").read() shouldBe file2Text
    root.resolve("folder1/file3.txt").read() shouldBe file3Text
    root.resolve("folder2/file4.txt").read() shouldBe file4Text
  }

  it should "create a bag with no files in data when the input directory does not exist" in {
    implicit val settings = Settings(
      multidepositDir = testDir.resolve("md-empty"),
      stagingDir = testDir.resolve("sd")
    )

    val outputDir = stagingBagDir(depositId)
    Files.createDirectories(outputDir)
    outputDir.toFile should exist

    multiDepositDir(depositId).toFile should not(exist)

    AddBagToDeposit(deposit)(settings).execute shouldBe a[Success[_]]

    stagingDir(depositId).toFile should exist
    stagingBagDataDir(depositId).toFile should exist
    stagingBagDataDir(depositId).listRecursively(!Files.isDirectory(_)) shouldBe empty
    stagingBagDir(depositId)
      .listRecursively(!Files.isDirectory(_))
      .map(_.getFileName.toString) should contain theSameElementsAs
      List("bag-info.txt",
        "bagit.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")

    val root = stagingBagDir(depositId)
    root.resolve("manifest-sha1.txt").read() shouldBe empty
    root.resolve("tagmanifest-sha1.txt").read() should include("bag-info.txt")
    root.resolve("tagmanifest-sha1.txt").read() should include("bagit.txt")
    root.resolve("tagmanifest-sha1.txt").read() should include("manifest-sha1.txt")
  }

  it should "contain the date-created in the bag-info.txt" in {
    AddBagToDeposit(deposit).execute() shouldBe a[Success[_]]

    val bagInfo = stagingBagDir(depositId).resolve("bag-info.txt")
    bagInfo.toFile should exist

    bagInfo.read() should include("Created")
  }

  it should "contain the correct checksums in its manifest file" in {
    AddBagToDeposit(deposit).execute() shouldBe a[Success[_]]

    verifyChecksums(depositId, "manifest-sha1.txt")
  }

  it should "contain the correct checksums in its tagmanifest file" in {
    AddBagToDeposit(deposit).execute() shouldBe a[Success[_]]

    verifyChecksums(depositId, "tagmanifest-sha1.txt")
  }

  def verifyChecksums(depositId: DepositId, manifestFile: String): Unit = {
    val root = stagingBagDir(depositId)
    root.resolve(manifestFile).read()
      .split('\n')
      .map(_.split("  "))
      .foreach {
        case Array(sha1, file) => calcSHA1(root.resolve(file).read()) shouldBe sha1
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
