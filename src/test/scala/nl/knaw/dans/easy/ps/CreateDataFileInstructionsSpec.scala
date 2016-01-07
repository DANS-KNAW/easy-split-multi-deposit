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
package nl.knaw.dans.easy.ps


import java.io.FileNotFoundException

import nl.knaw.dans.easy.ps.CustomMatchers._
import scala.util.{Failure, Success}
import language.postfixOps

class CreateDataFileInstructionsSpec extends UnitSpec {

  implicit val s = Settings(
    ebiuDir = file(testDir, "ebiu"),
    sipDir = file(testDir, "sip"),
    storageServices = Map()
  )

  "checkPreconditions" should "fail if no fileStorageDatasetPath is provided" in {

    val action = CreateDataFileInstructions(row = "",
      fileSip = None,
      datasetId = "",
      fileInDataset = "",
      fileStorageService = "",
      fileStorageDatasetPath = None,
      fileStorageFilePath = ""
    )
    inside(action.checkPreconditions) {
      case Failure(ActionExceptionList(aes: Seq[ActionException])) =>
        aes.length shouldBe 1
        aes.head.message shouldBe "path on storage could not be determined"
    }
  }

  it should "succeed if fileStorageDatasetPath is an empty string?" in {

    val action = CreateDataFileInstructions(row = "",
      fileSip = None,
      datasetId = "",
      fileInDataset = "",
      fileStorageService = "",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )
    action.checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "throw a FileNotFoundException without an existing filedata directory" in {

    val action = CreateDataFileInstructions(row = "1",
      fileSip = None,
      datasetId = "",
      fileInDataset = "",
      fileStorageService = "",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )
    the [FileNotFoundException] thrownBy action.run() should
      have message file(s.ebiuDir, "filedata/.properties")+" (No such file or directory)"
  }

  it should "create a properties file" in {

    val propertiesFile = file(s.ebiuDir, "dataset-1/filedata/some.mpeg.properties")
    propertiesFile.getParentFile.mkdirs()

    val action = CreateDataFileInstructions(row = "1",
      fileSip = None,
      datasetId = "dataset-1",
      fileInDataset = "some.mpeg",
      fileStorageService = "ss",
      fileStorageDatasetPath = Some("dsp"),
      fileStorageFilePath = "sfp"
    )
    action.run shouldBe a[Success[_]]
    propertiesFile should be a 'file
    // TODO assert content
  }

  "rollback" should "succeed and leave created properties file?" in {

    val propertiesFile = file(s.ebiuDir, "dataset-1/filedata/some.mpeg.properties")
    propertiesFile.getParentFile.mkdirs()

    val action = CreateDataFileInstructions(row = "1",
      fileSip = None,
      datasetId = "dataset-1",
      fileInDataset = "some.mpeg",
      fileStorageService = "ss",
      fileStorageDatasetPath = Some("dsp"),
      fileStorageFilePath = "sfp"
    )
    action.run()

    action.rollback() shouldBe a[Success[_]]
    propertiesFile should exist
  }
}
