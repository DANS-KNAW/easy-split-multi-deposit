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

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }

import scala.collection.mutable
import scala.util.{ Failure, Success }
import scala.xml.Utility

class AddFileMetadataToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "dd"),
    outputDepositDir = new File(testDir, "dd")
  )
  val datasetID = "ruimtereis01"
  val dataset = mutable.HashMap(
    "DATASET" -> List(datasetID, datasetID),
    "FILE_SIP" -> List("ruimtereis01/reisverslag/deel01.txt", "")
  )
  before {
    new File(getClass.getResource("/spacetravel").toURI)
      .copyDir(settings.outputDepositDir)
  }

  after {
    settings.outputDepositDir.deleteDirectory()
  }

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  "preconditions check with existing SIP files" should "succeed" in {
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  "preconditions check with non-existing SIP files" should "fail" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID, datasetID),
      "FILE_SIP" -> List("ruimtereis01/reisverslag/deel01.txt", "", "non-existing-file-path")
    )

    inside(new AddFileMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include(s"""for dataset "$datasetID": [non-existing-file-path]""")
    }
  }

  "execute" should "write the file metadata to an xml file" in {
    val action = new AddFileMetadataToDeposit(1, (datasetID, dataset))
    val metadataDir = outputDepositBagMetadataDir(datasetID)

    action.execute() shouldBe a[Success[_]]

    metadataDir should exist
    outputFileMetadataFile(datasetID) should exist
  }

  "datasetToFileXml" should "produce the xml for all the files" in {
    AddFileMetadataToDeposit.datasetToFileXml("ruimtereis01").child.map(Utility.trim) should (
      have length 5 and
        contain allOf(
      <file filepath="data/ruimtereis01_verklaring.txt"><dcterms:format>text/plain</dcterms:format></file>,
      <file filepath="data/reisverslag/deel01.docx"><dcterms:format>application/vnd.openxmlformats-officedocument.wordprocessingml.document</dcterms:format></file>,
      <file filepath="data/reisverslag/deel01.txt"><dcterms:format>text/plain</dcterms:format></file>,
      <file filepath="data/reisverslag/deel02.txt"><dcterms:format>text/plain</dcterms:format></file>,
      <file filepath="data/reisverslag/deel03.txt"><dcterms:format>text/plain</dcterms:format></file>
    ))
  }

  //file does not need to exist for the mimetype to be established. it is based solely on filename.
  "xmlPerPath" should "create the correct filepath " in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(datasetID))(new File(multiDepositDir(datasetID), "ruimtereis01_verklaring.txt"))
    val res = <file filepath="data/ruimtereis01_verklaring.txt">
      <dcterms:format>text/plain</dcterms:format>
    </file>

    Utility.trim(xml) shouldBe Utility.trim(res)
  }

  //file does not need to exist for the mimetype to be established. it is based solely on filename.
  it should "produce the correct doc mimetype " in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(datasetID))(new File(multiDepositDir(datasetID), "reisverslag/deel01.doc"))
    val res = <file filepath="data/reisverslag/deel01.doc">
      <dcterms:format>application/msword</dcterms:format>
    </file>

    Utility.trim(xml) shouldBe Utility.trim(res)
  }

  //file does not need to exist for the mimetype to be established. it is based solely on filename.
  it should "produce the correct docx mimetype " in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(datasetID))(new File(multiDepositDir(datasetID), "reisverslag/deel01.docx"))
    val res = <file filepath="data/reisverslag/deel01.docx">
      <dcterms:format>application/vnd.openxmlformats-officedocument.wordprocessingml.document</dcterms:format>
    </file>

    Utility.trim(xml) shouldBe Utility.trim(res)
  }

  it should "produce the xml for one file" in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(datasetID))(new File(multiDepositDir(datasetID), "reisverslag/deel01.txt"))
    val res = <file filepath="data/reisverslag/deel01.txt">
      <dcterms:format>text/plain</dcterms:format>
    </file>

    Utility.trim(xml) shouldBe Utility.trim(res)
  }
}
