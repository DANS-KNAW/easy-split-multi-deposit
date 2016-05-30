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

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddDatasetMetadataToDeposit.datasetToXml
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.commons.io.FileUtils.readFileToString
import org.scalatest.BeforeAndAfterAll
import rx.lang.scala.observers.TestSubscriber
import rx.lang.scala.{Observable, ObservableExtensions}

import scala.util.Success
import scala.xml.{Elem, PrettyPrinter}

class AddDatasetMetadataToDepositSpec extends UnitSpec with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    outputDepositDir = new File(testDir, "dd")
  )

  val datasetID = "ds1"
  val dataset = new Dataset() +=
    "DATASET" -> List(datasetID, datasetID, datasetID, datasetID) +=
    "DC_TITLE" -> List("dataset title", "", "", "") +=
    "DCT_ALTERNATIVE" -> List("foobar", "", "", "") +=
    "DCX_CREATOR_TITLES" -> List("", "", "", "") +=
    "DCX_CREATOR_INITIALS" -> List("", "", "", "") +=
    "DCX_CREATOR_INSERTIONS" -> List("", "", "", "") +=
    "DCX_CREATOR_SURNAME" -> List("", "", "", "") +=
    "DCX_CREATOR_DAI" -> List("", "", "", "") +=
    "DCX_CREATOR_ORGANIZATION" -> List("wie dit leest is een aap", "let me know if you read this", "then you did well on your code review!", "") +=
    "DDM_CREATED" -> List("1992-07-30", "", "", "") +=
    "DC_DESCRIPTION" -> List("omschr1", "", "", "") +=
    "DCX_RELATION_QUALIFIER" -> List("", "", "", "") +=
    "DCX_RELATION_TITLE" -> List("", "", "", "") +=
    "DCX_RELATION_LINK" -> List("", "", "", "") +=
    "DC_TYPE" -> List("random test data", "", "", "") +=
    "DC_SOURCE" -> List("", "", "", "") +=
    "DDM_ACCESSRIGHTS" -> List("NONE", "", "", "") +=
    "DDM_AVAILABLE" -> List("nope", "", "", "") +=
    "DDM_AUDIENCE" -> List("everyone", "nobody", "some people", "people with yellow hear")

  val expectedXml = <ddm:DDM
  xmlns:ddm="http://easy.dans.knaw.nl/schemas/md/ddm/"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:dc="http://purl.org/dc/elements/1.1/"
  xmlns:dct="http://purl.org/dc/terms/"
  xmlns:dcterms="http://purl.org/dc/terms/"
  xmlns:dcmitype="http://purl.org/dc/dcmitype/"
  xmlns:dcx="http://easy.dans.knaw.nl/schemas/dcx/"
  xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
  xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
  xmlns:narcis="http://easy.dans.knaw.nl/schemas/vocab/narcis-type/"
  xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/"
  xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2012/11/ddm.xsd">
    <ddm:profile>
      <dc:title>dataset title</dc:title>
      <dcterms:description>omschr1</dcterms:description>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">wie dit leest is een aap</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">let me know if you read this</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">then you did well on your code review!</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <ddm:created>1992-07-30</ddm:created>
      <ddm:audience>everyone</ddm:audience>
      <ddm:audience>nobody</ddm:audience>
      <ddm:audience>some people</ddm:audience>
      <ddm:audience>people with yellow hear</ddm:audience>
      <ddm:accessRights>NONE</ddm:accessRights>
    </ddm:profile>
    <ddm:dcmiMetadata>
      <ddm:available>nope</ddm:available>
      <dcterms:alternative>foobar</dcterms:alternative>
      <dcterms:type>random test data</dcterms:type>
    </ddm:dcmiMetadata>
  </ddm:DDM>

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "run" should "write the metadata to a file at the correct place" in {
    val file = outputDatasetMetadataFile(settings, datasetID)

    file should not (exist)

    AddDatasetMetadataToDeposit(1, ("ds1", dataset)).run() shouldBe a[Success[_]]

    file should exist
  }

  "datasetToXml" should "return the expected xml" in {
    new PrettyPrinter(160, 2).format(datasetToXml(dataset)) shouldBe
      new PrettyPrinter(160, 2).format(expectedXml)
  }

  it should "return xml on reading from the sip-demo csv" in {
    val subscriber = toXmlSubscriber("/Roundtrip_MD/sip-demo-2015-02-24/instructions.csv")

    subscriber.assertNoErrors()
    subscriber.assertValueCount(2)
    subscriber.assertCompleted()
    subscriber.assertUnsubscribed()
  }

  it should "return xml on reading from the sip001 csv" in {
    val subscriber = toXmlSubscriber("/Roundtrip_MD/sip001/instructions.csv")

    subscriber.assertNoErrors()
    subscriber.assertValueCount(2)
    subscriber.assertCompleted()
    subscriber.assertUnsubscribed()
  }

  def toXmlSubscriber(file: String): TestSubscriber[Elem] = {
    val csv = new File(getClass.getResource(file).toURI)
    val subscriber = TestSubscriber[Elem]
    MultiDepositParser.parse(csv)
      .flatMap(_.toObservable)
      .map(tuple => AddDatasetMetadataToDeposit.datasetToXml(tuple._2))
      .subscribe(subscriber)
    subscriber
  }

  "createDcmiMetadata" should "return the expected spatial elements" in {
    val dataset = new Dataset() +=
      "DCT_SPATIAL" -> List("here", "there", "", "") +=
      "DCX_SPATIAL_SCHEME" -> List("degrees", "degrees", "", "") +=
      "DCX_SPATIAL_X" -> List("83575.4", "", "", "") +=
      "DCX_SPATIAL_Y" -> List("455271.2", "", "", "") +=
      "DCX_SPATIAL_NORTH" -> List("", "1", "", "") +=
      "DCX_SPATIAL_SOUTH" -> List("", "2", "", "") +=
      "DCX_SPATIAL_EAST" -> List("", "3", "", "") +=
      "DCX_SPATIAL_WEST" -> List("", "4", "", "")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcterms:spatial>here</dcterms:spatial>
        <dcterms:spatial>there</dcterms:spatial>
        <dcx-gml:spatial srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
          <Point xmlns="http://www.opengis.net/gml">
            <pos>455271.2 83575.4</pos>
          </Point>
        </dcx-gml:spatial>
        <dcx-gml:spatial>
          <boundedBy xmlns="http://www.opengis.net/gml">
            <Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
              <lowerCorner>1 3</lowerCorner>
              <upperCorner>2 4</upperCorner>
            </Envelope>
          </boundedBy>
        </dcx-gml:spatial>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>,expectedXml)
  }

  it should "return the expected contributor" in {
    val dataset = new Dataset() +=
      "DCX_CONTRIBUTOR_TITLES" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_INITIALS" -> List("B.O.", "", "", "") +=
      "DCX_CONTRIBUTOR_INSERTIONS" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_SURNAME" -> List("Kito", "", "", "") +=
      "DCX_CONTRIBUTOR_DAI" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_ORGANIZATION" -> List("", "", "", "")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcx-dai:contributorDetails>
          <dcx-dai:author>
            <dcx-dai:initials>B.O.</dcx-dai:initials>
            <dcx-dai:surname>Kito</dcx-dai:surname>
          </dcx-dai:author>
        </dcx-dai:contributorDetails>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>,expectedXml)
  }

  it should "return the expected relations" in {
    val dataset = new Dataset() +=
      "DCX_RELATION_QUALIFIER" -> List("replaces", "isRequiredBy", "", "") +=
      "DCX_RELATION_LINK" -> List("http://x", "", "http://y", "") +=
      "DCX_RELATION_TITLE" -> List("dummy", "blabla", "", "barbapappa")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcterms:replaces>http://x</dcterms:replaces>
        <dcterms:isRequiredBy>blabla</dcterms:isRequiredBy>
        <dc:relation>http://y</dc:relation>
        <dc:relation>barbapappa</dc:relation>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>,expectedXml)
  }

  it should "return the expected dcmidata" in {
    val dataset = new Dataset() +=
      "DCT_RIGHTSHOLDER" -> List("rh1", "", "", "") +=
      "DCT_TEMPORAL" -> List("1992-2016", "2005", "", "") +=
      "DC_PUBLISHER" -> List("pub1", "", "", "") +=
      "DC_FORMAT" -> List("text", "", "", "") +=
      "DC_SUBJECT" -> List("me", "you", "him", "her") +=
      "DC_IDENTIFIER" -> List("ds1", "", "", "") +=
      "DC_FORMAT" -> List("text", "", "", "") +=
      "DC_LANGUAGE" -> List("Scala", "", "", "")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcterms:publisher>pub1</dcterms:publisher>
        <dc:identifier>ds1</dc:identifier>
        <dcterms:rightsHolder>rh1</dcterms:rightsHolder>
        <dc:language>Scala</dc:language>
        <dc:format>text</dc:format>
        <dc:subject>me</dc:subject>
        <dc:subject>you</dc:subject>
        <dc:subject>him</dc:subject>
        <dc:subject>her</dc:subject>
        <dcterms:temporal>1992-2016</dcterms:temporal>
        <dcterms:temporal>2005</dcterms:temporal>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>,expectedXml)
  }

  def verify(actualXml: Elem, expectedXml: Elem): Unit = {
    new PrettyPrinter(160, 2).format(actualXml) shouldBe
      new PrettyPrinter(160, 2).format(expectedXml)
  }
}
