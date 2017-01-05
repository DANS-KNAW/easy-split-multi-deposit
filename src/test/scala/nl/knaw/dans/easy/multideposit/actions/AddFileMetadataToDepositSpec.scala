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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable
import scala.util.Success
import scala.xml.PrettyPrinter

class AddFileMetadataToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "dd"),
    outputDepositDir = new File(testDir, "dd")
  )
  val datasetID = "ruimtereis01"
  val dataset = mutable.HashMap(
    "DATASET" -> List(datasetID, datasetID),
    "FILE_SIP" -> List("reisverslag/deel01.txt", "")
  )
  before {
    new File(getClass.getResource("/spacetravel").toURI)
      .copyDir(settings.outputDepositDir)
  }

  after {
    settings.outputDepositDir.deleteDirectory()
  }

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "run" should "write the file metadata to an xml file" in {
    val action = new AddFileMetadataToDeposit(1, (datasetID, dataset))
    val metadataDir = outputDepositBagMetadataDir(settings, datasetID)

    action.run() shouldBe a[Success[_]]

    metadataDir should exist
    outputFileMetadataFile(settings, datasetID) should exist
  }

  "datasetToFileXml" should "produce the xml for all the files" in {
    val xml = AddFileMetadataToDeposit.datasetToFileXml("ruimtereis01")
    xml.child.length shouldBe 5

    xml.child should contain(<file filepath="data/ruimtereis01_verklaring.txt"><dcterms:format>text/plain</dcterms:format></file>)
    xml.child should contain(<file filepath="data/reisverslag/deel01.docx"><dcterms:format>application/vnd.openxmlformats-officedocument.wordprocessingml.document</dcterms:format></file>)
    xml.child should contain(<file filepath="data/reisverslag/deel01.txt"><dcterms:format>text/plain</dcterms:format></file>)
    xml.child should contain(<file filepath="data/reisverslag/deel02.txt"><dcterms:format>text/plain</dcterms:format></file>)
    xml.child should contain(<file filepath="data/reisverslag/deel03.txt"><dcterms:format>text/plain</dcterms:format></file>)
  }

  //file does not need to exist for the mimetype to be established. it is based solely on filename.
  "xmlPerPath" should "create the correct filepath " in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(settings, datasetID))(new File(multiDepositDir(settings, datasetID), "ruimtereis01_verklaring.txt"))
    val res = <file filepath="data/ruimtereis01_verklaring.txt">
      <dcterms:format>text/plain</dcterms:format>
    </file>

    new PrettyPrinter(160, 2).format(xml) shouldBe new PrettyPrinter(160, 2).format(res)
  }

  //file does not need to exist for the mimetype to be established. it is based solely on filename.
  it should "produce the correct doc mimetype " in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(settings, datasetID))(new File(multiDepositDir(settings, datasetID), "reisverslag/deel01.doc"))
    val res = <file filepath="data/reisverslag/deel01.doc">
      <dcterms:format>application/msword</dcterms:format>
    </file>

    new PrettyPrinter(160, 2).format(xml) shouldBe new PrettyPrinter(160, 2).format(res)
  }

  //file does not need to exist for the mimetype to be established. it is based solely on filename.
  it should "produce the correct docx mimetype " in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(settings, datasetID))(new File(multiDepositDir(settings, datasetID), "reisverslag/deel01.docx"))
    val res = <file filepath="data/reisverslag/deel01.docx">
      <dcterms:format>application/vnd.openxmlformats-officedocument.wordprocessingml.document</dcterms:format>
    </file>

    new PrettyPrinter(160, 2).format(xml) shouldBe new PrettyPrinter(160, 2).format(res)
  }

  it should "produce the xml for one file" in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(multiDepositDir(settings, datasetID))(new File(multiDepositDir(settings, datasetID), "reisverslag/deel01.txt"))
    val res = <file filepath="data/reisverslag/deel01.txt">
      <dcterms:format>text/plain</dcterms:format>
    </file>

    new PrettyPrinter(160, 2).format(xml) shouldBe new PrettyPrinter(160, 2).format(res)
  }
}
