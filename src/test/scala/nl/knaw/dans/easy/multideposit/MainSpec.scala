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

import nl.knaw.dans.easy.multideposit.Main._
import nl.knaw.dans.easy.multideposit.actions.{ CreateSpringfieldActions, _ }

import scala.language.reflectiveCalls

class MainSpec extends UnitSpec {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "dd"),
    springfieldInbox = new File(testDir, "sfi")
  )

  val generalActions = Seq(CreateSpringfieldActions(-1, testDatasets))

  val dataset1Actions = new {
    val entry @ (datasetID, dataset) = testDatasets.head
    val datasetActions = Seq(
      CreateOutputDepositDir(2, datasetID),
      AddBagToDeposit(2, entry),
      AddDatasetMetadataToDeposit(2, entry),
      AddFileMetadataToDeposit(2, entry),
      AddPropertiesToDeposit(2, entry),
      SetDepositPermissions(2, datasetID))
    val fileActions = Seq(CopyToSpringfieldInbox(2, "videos/centaur.mpg"))
  }

  val dataset2Actions = new {
    val entry @ (datasetID, dataset) = testDatasets.tail.head
    val datasetActions = Seq(
      CreateOutputDepositDir(2, datasetID),
      AddBagToDeposit(2, entry),
      AddDatasetMetadataToDeposit(2, entry),
      AddFileMetadataToDeposit(2, entry),
      AddPropertiesToDeposit(2, entry),
      SetDepositPermissions(2, datasetID))
    val fileActions = Seq(CopyToSpringfieldInbox(4, "videos/centaur.mpg"))
  }

  "getActions" should "return all actions to be performed given the collection of datasets" in {
    getActions(testDatasets) should {
      have size 15 and
      contain theSameElementsInOrderAs(
        dataset1Actions.datasetActions ++ dataset1Actions.fileActions ++
        dataset2Actions.datasetActions ++ dataset2Actions.fileActions ++
        generalActions
      )
    }
  }

  "getGeneralActions" should "return a collection of actions that are supposed to run only once for all datasets" in {
    getGeneralActions(testDatasets) should {
      have size 1 and contain theSameElementsInOrderAs generalActions
    }
  }

  "getDatasetActions" should "return a collection of actions for the given dataset" in {
    import dataset1Actions._
    getDatasetActions(entry) should {
      have size 7 and contain theSameElementsInOrderAs (datasetActions ++ fileActions)
    }
  }

  it should "do the same for testDataset2" in {
    import dataset2Actions._
    getDatasetActions(entry) should {
      have size 7 and contain theSameElementsInOrderAs (datasetActions ++ fileActions)
    }
  }

  "getFileActions" should "return an action for each FileParameters object that is an A/V file" in {
    import dataset1Actions._
    getFileActions(dataset) should {
      have size 1 and contain theSameElementsInOrderAs fileActions
    }
  }

  it should "do the same for testDataset2" in {
    import dataset2Actions._
    getFileActions(dataset) should {
      have size 1 and contain theSameElementsInOrderAs fileActions
    }
  }

  "extractFileParameters" should "only yield the FileParameters where not all fields are empty" in {
    extractFileParameters(testDataset1) should {
      have size 1 and contain theSameElementsInOrderAs testFileParameters1
    }
  }

  it should "do the same for testDataset2" in {
    extractFileParameters(testDataset2) should {
      have size 3 and contain theSameElementsInOrderAs testFileParameters2
    }
  }

  it should "return Nil when the dataset is empty" in {
    extractFileParameters(new Dataset) shouldBe empty
  }

  it should "return the fileParameters without row number when these are not supplied" in {
    val res = FileParameters(None, Option("videos/centaur.mpg"), Option("footage/centaur.mpg"), Option("http://zandbak11.dans.knaw.nl/webdav"), None, Option("Yes"))
    extractFileParameters(testDataset1 -= "ROW") should contain only res
  }

  it should "return Nil when ALL extracted fields are removed from the dataset" in {
    val ds = new Dataset ++=
      testDataset1 --=
      List("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")

    extractFileParameters(ds) shouldBe empty
  }
}
