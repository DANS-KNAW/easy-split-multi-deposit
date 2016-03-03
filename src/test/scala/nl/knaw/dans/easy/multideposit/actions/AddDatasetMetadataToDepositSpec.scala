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
import org.scalatest.BeforeAndAfterAll

import scala.util.Success
import scala.xml.PrettyPrinter

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
    "DCX_CONTRIBUTOR_TITLES" -> List("", "", "", "") +=
    "DCX_CONTRIBUTOR_INITIALS" -> List("B.O.", "", "", "") +=
    "DCX_CONTRIBUTOR_INSERTIONS" -> List("", "", "", "") +=
    "DCX_CONTRIBUTOR_SURNAME" -> List("Kito", "", "", "") +=
    "DCX_CONTRIBUTOR_DAI" -> List("", "", "", "") +=
    "DCX_CONTRIBUTOR_ORGANIZATION" -> List("", "", "", "") +=
    "DDM_CREATED" -> List("1992-07-30", "", "", "") +=
    "DCT_RIGHTSHOLDER" -> List("rh1", "", "", "") +=
    "DC_PUBLISHER" -> List("pub1", "", "", "") +=
    "DC_DESCRIPTION" -> List("omschr1", "", "", "") +=
    "DC_SUBJECT" -> List("me", "you", "him", "her") +=
    "DCT_TEMPORAL" -> List("1992-2016", "2005", "", "") +=
    "DCT_SPATIAL" -> List("here", "there", "", "") +=
    "DCX_SPATIAL_SCHEME" -> List("", "", "", "") +=
    "DCX_SPATIAL_X" -> List("", "", "", "") +=
    "DCX_SPATIAL_Y" -> List("", "", "", "") +=
    "DCX_SPATIAL_NORTH" -> List("", "", "", "") +=
    "DCX_SPATIAL_SOUTH" -> List("", "", "", "") +=
    "DCX_SPATIAL_EAST" -> List("", "", "", "") +=
    "DCX_SPATIAL_WEST" -> List("", "", "", "") +=
    "DC_IDENTIFIER" -> List("ds1", "", "", "") +=
    "DCX_RELATION_QUALIFIER" -> List("", "", "", "") +=
    "DCX_RELATION_TITLE" -> List("", "", "", "") +=
    "DCX_RELATION_LINK" -> List("", "", "", "") +=
    "DC_TYPE" -> List("random test data", "", "", "") +=
    "DC_FORMAT" -> List("text", "", "", "") +=
    "DC_LANGUAGE" -> List("Scala", "", "", "") +=
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
          <dcx-dai:name xml:lang="en"> wie dit leest is een aap </dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en"> let me know if you read this </dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en"> then you did well on your code review! </dcx-dai:name>
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
      <dcterms:publisher>pub1</dcterms:publisher>
      <dc:identifier>ds1</dc:identifier>
      <dc:language>Scala</dc:language>
      <dcterms:rightsHolder>rh1</dcterms:rightsHolder>
      <ddm:available>nope</ddm:available>
      <dc:format>text</dc:format>
      <dc:subject>me</dc:subject>
      <dc:subject>you</dc:subject>
      <dc:subject>him</dc:subject>
      <dc:subject>her</dc:subject>
      <dcterms:spatial>here</dcterms:spatial>
      <dcterms:spatial>there</dcterms:spatial>
      <dcterms:alternative>foobar</dcterms:alternative>
      <dcterms:temporal>1992-2016</dcterms:temporal>
      <dcterms:temporal>2005</dcterms:temporal>
      <dcterms:type>random test data</dcterms:type>
      <dcx-dai:contributorDetails>
        <dcx-dai:author>
          <dcx-dai:initials>B.O.</dcx-dai:initials>
          <dcx-dai:surname>Kito</dcx-dai:surname>
        </dcx-dai:author>
      </dcx-dai:contributorDetails>
    </ddm:dcmiMetadata>
  </ddm:DDM>

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "always succeed" in {
    AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "write the metadata to a file at the correct place" in {
    val file = new File(outputDepositBagMetadataDir(settings, datasetID), "dataset.xml")

    file should not (exist)

    AddDatasetMetadataToDeposit(1, ("ds1", dataset)).run() shouldBe a[Success[_]]

    file should exist
  }

  "rollback" should "always succeed" in {
    AddDatasetMetadataToDeposit(1, ("ds1", dataset)).rollback() shouldBe a[Success[_]]
  }

  "datasetToXml" should "return the expected xml" in {
    AddDatasetMetadataToDeposit.datasetToXml(dataset) shouldBe new PrettyPrinter(160, 2).format(expectedXml)
  }
}
