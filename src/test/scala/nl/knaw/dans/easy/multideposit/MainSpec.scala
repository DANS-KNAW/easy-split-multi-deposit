/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.multideposit

import java.io.File

import nl.knaw.dans.easy.multideposit.Main.extractFileParameters

import scala.language.reflectiveCalls

class MainSpec extends UnitSpec {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "dd"),
    springfieldInbox = new File(testDir, "sfi")
  )

  /*"extractFileParameters"*/ ignore should "only yield the FileParameters where not all fields are empty" in {
    extractFileParameters(testDataset1) should {
      have size 1 and contain theSameElementsInOrderAs testFileParameters1
    }
  }

  ignore should "only yield the FileParameters where not all fields are empty in testDataset2" in {
    extractFileParameters(testDataset2) should {
      have size 3 and contain theSameElementsInOrderAs testFileParameters2
    }
  }

  ignore should "return Nil when the dataset is empty" in {
    extractFileParameters(new Dataset) shouldBe empty
  }

  ignore should "return the fileParameters without row number when these are not supplied" in {
    val res = FileParameters(None, Option("videos/centaur.mpg"), Option("footage/centaur.mpg"), Option("http://zandbak11.dans.knaw.nl/webdav"), None, Option("Yes"))
    extractFileParameters(testDataset1 -= "ROW") should contain only res
  }

  ignore should "return Nil when ALL extracted fields are removed from the dataset" in {
    val ds = new Dataset ++=
      testDataset1 --=
      List("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")

    extractFileParameters(ds) shouldBe empty
  }
}
