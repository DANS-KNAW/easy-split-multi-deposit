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

import scala.util.Success
import scala.xml.PrettyPrinter

class AddFileMetadataToDepositSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "dd"),
    outputDepositDir = new File(testDir, "dd")
  )
  val datasetID = "ruimtereis01"

  before {
    new File(getClass.getResource("/Roundtrip_MD/sip-demo-2015-02-24").toURI)
      .copyDir(settings.outputDepositDir)
  }

  after {
    settings.outputDepositDir.deleteDirectory()
  }

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "always succeed" in {
    val action = new AddFileMetadataToDeposit(1, ("ruimtereis01", testDataset1))
    action.checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "write the file metadata to an xml file" in {
    val action = new AddFileMetadataToDeposit(1, (datasetID, testDataset1))
    val metadataDir = outputDepositBagMetadataDir(settings, datasetID)

    action.run() shouldBe a[Success[_]]

    metadataDir should exist
    outputFileMetadataFile(settings, datasetID) should exist
  }

  "rollback" should "always succeed" in {
    val action = new AddFileMetadataToDeposit(1, ("ruimtereis01", testDataset1))
    action.rollback() shouldBe a[Success[_]]
  }

  "datasetToFileXml" should "produce the xml for all the files" in {
    val xml = AddFileMetadataToDeposit.datasetToFileXml(("ruimtereis01", testDataset1))
    val res = <files>
      <file filepath="data/reisverslag/deel01.txt">
        <dcterms:subject>astronomie</dcterms:subject>
        <dcterms:subject>ruimtevaart</dcterms:subject>
        <dcterms:subject>planetoïden</dcterms:subject>
        <dcterms:format>video/mpeg</dcterms:format>
        <dcterms:format>text/plain</dcterms:format>
        <dcterms:language>NL</dcterms:language>
        <dcterms:language>encoding=UTF-8</dcterms:language>
        <dcterms:title>Reis naar Centaur-planetoïde</dcterms:title>
        <dcterms:title>Trip to Centaur asteroid</dcterms:title>
        <dcterms:description>Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.</dcterms:description>
        <dcterms:description>A two day mission to boldly go where no man has gone before</dcterms:description>
      </file>
      <file filepath="data/reisverslag/deel02.txt">
        <dcterms:subject>astronomie</dcterms:subject>
        <dcterms:subject>ruimtevaart</dcterms:subject>
        <dcterms:subject>planetoïden</dcterms:subject>
        <dcterms:format>video/mpeg</dcterms:format>
        <dcterms:format>text/plain</dcterms:format>
        <dcterms:language>NL</dcterms:language>
        <dcterms:language>encoding=UTF-8</dcterms:language>
        <dcterms:title>Reis naar Centaur-planetoïde</dcterms:title>
        <dcterms:title>Trip to Centaur asteroid</dcterms:title>
        <dcterms:description>Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.</dcterms:description>
        <dcterms:description>A two day mission to boldly go where no man has gone before</dcterms:description>
      </file>
      <file filepath="data/reisverslag/deel03.txt">
        <dcterms:subject>astronomie</dcterms:subject>
        <dcterms:subject>ruimtevaart</dcterms:subject>
        <dcterms:subject>planetoïden</dcterms:subject>
        <dcterms:format>video/mpeg</dcterms:format>
        <dcterms:format>text/plain</dcterms:format>
        <dcterms:language>NL</dcterms:language>
        <dcterms:language>encoding=UTF-8</dcterms:language>
        <dcterms:title>Reis naar Centaur-planetoïde</dcterms:title>
        <dcterms:title>Trip to Centaur asteroid</dcterms:title>
        <dcterms:description>Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.</dcterms:description>
        <dcterms:description>A two day mission to boldly go where no man has gone before</dcterms:description>
      </file>
    </files>

    new PrettyPrinter(160, 2).format(xml) shouldBe new PrettyPrinter(160, 2).format(res)
  }

  "xmlPerPath" should "produce the xml for one file" in {
    val xml = AddFileMetadataToDeposit.xmlPerPath(testDataset1)("data/reisverslag/deel01.txt")
    val res = <file filepath="data/reisverslag/deel01.txt">
      <dcterms:subject>astronomie</dcterms:subject>
      <dcterms:subject>ruimtevaart</dcterms:subject>
      <dcterms:subject>planetoïden</dcterms:subject>
      <dcterms:format>video/mpeg</dcterms:format>
      <dcterms:format>text/plain</dcterms:format>
      <dcterms:language>NL</dcterms:language>
      <dcterms:language>encoding=UTF-8</dcterms:language>
      <dcterms:title>Reis naar Centaur-planetoïde</dcterms:title>
      <dcterms:title>Trip to Centaur asteroid</dcterms:title>
      <dcterms:description>Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.</dcterms:description>
      <dcterms:description>A two day mission to boldly go where no man has gone before</dcterms:description>
    </file>

    new PrettyPrinter(160, 2).format(xml) shouldBe new PrettyPrinter(160, 2).format(res)
  }
}
