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

import java.io.File
import java.security.MessageDigest

import nl.knaw.dans.easy.multideposit.model.{ Dataset, DatasetId }
import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.Success

class AddBagToDepositSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "sd")
  )

  val datasetId = "ruimtereis01"
  val file1Text = "abcdef"
  val file2Text = "defghi"
  val file3Text = "ghijkl"
  val file4Text = "jklmno"
  val file5Text = "mnopqr"
  val dataset: Dataset = testDataset1

  before {
    new File(multiDepositDir(datasetId), "file1.txt").write(file1Text)
    new File(multiDepositDir(datasetId), "folder1/file2.txt").write(file2Text)
    new File(multiDepositDir(datasetId), "folder1/file3.txt").write(file3Text)
    new File(multiDepositDir(datasetId), "folder2/file4.txt").write(file4Text)
    new File(multiDepositDir("ruimtereis02"), "folder3/file5.txt").write("file5Text")

    stagingBagDir(datasetId).mkdirs
  }

  after {
    stagingBagDir(datasetId).deleteDirectory()
  }

  "execute" should "succeed given the current setup" in {
    AddBagToDeposit(dataset).execute shouldBe a[Success[_]]
  }

  it should "create a bag with the files from ruimtereis01 in it and some meta-files around" in {
    AddBagToDeposit(dataset).execute shouldBe a[Success[_]]

    val root = stagingBagDir(datasetId)
    root should exist
    root.listRecursively.map(_.getName) should contain theSameElementsAs
      List("bag-info.txt",
        "bagit.txt",
        "file1.txt",
        "file2.txt",
        "file3.txt",
        "file4.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")
    stagingBagDataDir(datasetId) should exist
  }

  it should "preserve the file content after making the bag" in {
    AddBagToDeposit(dataset).execute shouldBe a[Success[_]]

    val root = stagingBagDataDir(datasetId)
    new File(root, "file1.txt").read() shouldBe file1Text
    new File(root, "folder1/file2.txt").read() shouldBe file2Text
    new File(root, "folder1/file3.txt").read() shouldBe file3Text
    new File(root, "folder2/file4.txt").read() shouldBe file4Text
  }

  it should "create a bag with no files in data when the input directory does not exist" in {
    implicit val settings = Settings(
      multidepositDir = new File(testDir, "md-empty"),
      stagingDir = new File(testDir, "sd")
    )

    val outputDir = stagingBagDir(datasetId)
    outputDir.mkdirs
    outputDir should exist

    multiDepositDir(datasetId) should not(exist)

    AddBagToDeposit(dataset)(settings).execute shouldBe a[Success[_]]

    stagingDir(datasetId) should exist
    stagingBagDataDir(datasetId) should exist
    stagingBagDataDir(datasetId).listRecursively shouldBe empty
    stagingBagDir(datasetId).listRecursively.map(_.getName) should contain theSameElementsAs
      List("bag-info.txt",
        "bagit.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")

    val root = stagingBagDir(datasetId)
    new File(root, "manifest-sha1.txt").read() shouldBe empty
    new File(root, "tagmanifest-sha1.txt").read() should include("bag-info.txt")
    new File(root, "tagmanifest-sha1.txt").read() should include("bagit.txt")
    new File(root, "tagmanifest-sha1.txt").read() should include("manifest-sha1.txt")
  }

  it should "contain the date-created in the bag-info.txt" in {
    AddBagToDeposit(dataset).execute() shouldBe a[Success[_]]

    val bagInfo = new File(stagingBagDir(datasetId), "bag-info.txt")
    bagInfo should exist

    bagInfo.read() should include("Created")
  }

  it should "contain the correct checksums in its manifest file" in {
    AddBagToDeposit(dataset).execute() shouldBe a[Success[_]]

    verifyChecksums(datasetId, "manifest-sha1.txt")
  }

  it should "contain the correct checksums in its tagmanifest file" in {
    AddBagToDeposit(dataset).execute() shouldBe a[Success[_]]

    verifyChecksums(datasetId, "tagmanifest-sha1.txt")
  }

  def verifyChecksums(datasetId: DatasetId, manifestFile: String): Unit = {
    val root = stagingBagDir(datasetId)
    new File(root, manifestFile).read()
      .split('\n')
      .map(_.split("  "))
      .foreach {
        case Array(sha1, file) => calcSHA1(new File(root, file).read()) shouldBe sha1
        case line => fail(s"unexpected line detected: ${ line.mkString("  ") }")
      }
  }

  def calcSHA1(string: String): String = {
    MessageDigest.getInstance("SHA-1")
      .digest(string.getBytes(encoding))
      .map("%02x".format(_))
      .mkString
  }
}
