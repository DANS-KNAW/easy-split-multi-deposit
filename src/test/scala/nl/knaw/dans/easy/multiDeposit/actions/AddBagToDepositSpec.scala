/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.actions

import java.io.File

import nl.knaw.dans.easy.multideposit.{Settings, UnitSpec, _}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.util.Success
import scala.collection.JavaConversions.collectionAsScalaIterable

class AddBagToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    depositDir = new File(testDir, "dd")
  )

  val datasetID = "ds1"
  val file1Text = "abcdef"
  val file2Text = "defghi"
  val file3Text = "ghijkl"
  val file4Text = "jklmno"
  val file5Text = "mnopqr"

  before {
    new File(settings.multidepositDir, s"$datasetID/file1.txt").write(file1Text)
    new File(settings.multidepositDir, s"$datasetID/folder1/file2.txt").write(file2Text)
    new File(settings.multidepositDir, s"$datasetID/folder1/file3.txt").write(file3Text)
    new File(settings.multidepositDir, s"$datasetID/folder2/file4.txt").write(file4Text)
    new File(settings.multidepositDir, s"ds2/folder3/file5.txt").write("file5Text")

    depositBagDir(settings, datasetID).mkdirs
  }

  after {
    depositBagDir(settings, datasetID).deleteDirectory()
  }

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "fail if the md folder does not exist" in {
    val inputDir = multiDepositDir(settings, datasetID)
    inputDir.deleteDirectory()

    val pre = AddBagToDeposit(1, datasetID).checkPreconditions

    (the [ActionException] thrownBy pre.get).row shouldBe 1
    (the [ActionException] thrownBy pre.get).message should include (s"does not exist")
  }

  it should "succeed if the md folder exists" in {
    AddBagToDeposit(1, datasetID).checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed given the current setup" in {
    AddBagToDeposit(1, datasetID).run() shouldBe a[Success[_]]
  }

  it should "create a bag with the files from ds1 in it and some meta-files around" in {
    AddBagToDeposit(1, datasetID).run()

    val root = new File(testDir, "dd/md-ds1")
    new File(root, "bag").exists shouldBe true
    FileUtils.listFiles(new File(root, "bag"), null, true).map(_.getName) should contain only (
      "bag-info.txt",
      "bagit.txt",
      "file1.txt",
      "file2.txt",
      "file3.txt",
      "file4.txt",
      "manifest-md5.txt",
      "tagmanifest-md5.txt")
    new File(root, "bag/data").exists shouldBe true
  }

  it should "preserve the file content after making the bag" in {
    AddBagToDeposit(1, datasetID).run()

    val root = new File(testDir, "dd/md-ds1")
    new File(root, "bag/data/file1.txt").read shouldBe file1Text
    new File(root, "bag/data/folder1/file2.txt").read shouldBe file2Text
    new File(root, "bag/data/folder1/file3.txt").read shouldBe file3Text
    new File(root, "bag/data/folder2/file4.txt").read shouldBe file4Text
  }

  "rollback" should "always succeed" in {
    AddBagToDeposit(1, datasetID).rollback() shouldBe a[Success[_]]
  }
}
