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
package nl.knaw.dans.easy.multideposit.actions

import java.io.File

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddDatasetMetadataToDeposit.datasetToXml
import nl.knaw.dans.lib.error.CompositeException
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, Utility }

class AddDatasetMetadataToDepositSpec extends UnitSpec with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    outputDepositDir = new File(testDir, "dd")
  )

  val datasetID = "ds1"
  val dataset: Dataset = new Dataset() +=
    "DATASET" -> List(datasetID, datasetID, datasetID, datasetID) +=
    "DC_TITLE" -> List("dataset title", "", "", "") +=
    "DCT_ALTERNATIVE" -> List("foobar", "", "", "") +=
    "DCX_CREATOR_TITLES" -> List("", "", "", "") +=
    "DCX_CREATOR_INITIALS" -> List("", "", "", "") +=
    "DCX_CREATOR_INSERTIONS" -> List("", "", "", "") +=
    "DCX_CREATOR_SURNAME" -> List("", "", "", "") +=
    "DCX_CREATOR_DAI" -> List("", "", "", "") +=
    "DCX_CREATOR_ORGANIZATION" -> List("Lorem ipsum dolor sit amet", "consectetur adipiscing elit", "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", "") +=
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

  val expectedXml: Elem = <ddm:DDM
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
          <dcx-dai:name xml:lang="en">Lorem ipsum dolor sit amet</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">consectetur adipiscing elit</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">sed do eiusmod tempor incididunt ut labore et dolore magna aliqua</dcx-dai:name>
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

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

 "checkPreconditions" should "succeed with correctly corresponding access rights and audience" in {
   val validDataset = mutable.HashMap(
     "DATASET" -> List(datasetID, datasetID),
     "DDM_CREATED" -> List("2017-07-30", ""),
     "DDM_ACCESSRIGHTS" -> List("GROUP_ACCESS", "OPEN_ACCESS"),
     "DDM_AUDIENCE" -> List("D37000", "") // Archaeology
   )

   new AddDatasetMetadataToDeposit(1, (datasetID, validDataset)).checkPreconditions shouldBe a[Success[_]]
 }

  it should "fail with incorrectly corresponding access rights and audience" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "DDM_ACCESSRIGHTS" -> List("GROUP_ACCESS", ""),
      "DDM_AUDIENCE" -> List("D30000", "") // Humanities
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeologie), but it is: D30000"
    }
  }

  it should "fail with undefined audience" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "DDM_ACCESSRIGHTS" -> List("GROUP_ACCESS", ""),
      "DDM_AUDIENCE" -> List("", "") // Humanities
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeologie), but it is not defined"
    }
  }

  it should "succeed with required person information" in {
    val validDataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "DCX_CREATOR_TITLES" -> List("", ""),
      "DCX_CREATOR_INITIALS" -> List("C.R.E.A.T.O.R.", ""),
      "DCX_CREATOR_INSERTIONS" -> List("", ""),
      "DCX_CREATOR_SURNAME" -> List("Creator", ""),
      "DCX_CREATOR_DAI" -> List("", ""),
      "DCX_CREATOR_ORGANIZATION" -> List("", "CreatorOrganisation"),
      "DCX_CONTRIBUTOR_TITLES" -> List("", ""),
      "DCX_CONTRIBUTOR_INITIALS" -> List("C.O.N.T.R.I.B.U.T.O.R.", ""),
      "DCX_CONTRIBUTOR_INSERTIONS" -> List("", ""),
      "DCX_CONTRIBUTOR_SURNAME" -> List("Contributor", ""),
      "DCX_CONTRIBUTOR_DAI" -> List("", ""),
      "DCX_CONTRIBUTOR_ORGANIZATION" -> List("", "ContributerOrganisation")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, validDataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail with missing required person information" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_CREATOR_TITLES" -> List("Prof"),
      "DCX_CREATOR_INITIALS" -> List("F.A.I.L"),
      "DCX_CREATOR_INSERTIONS" -> List(""),
      "DCX_CREATOR_SURNAME" -> List(""),
      "DCX_CREATOR_DAI" -> List("")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [DCX_CREATOR_SURNAME]"
    }
  }

  it should "fail with missing required point coordinates" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_SPATIAL_X" -> List("5"),
      "DCX_SPATIAL_Y" -> List("")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [DCX_SPATIAL_Y]"
    }
  }

  it should "fail with missing required box coordinates" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_SPATIAL_NORTH" -> List("5"),
      "DCX_SPATIAL_SOUTH" -> List(""),
      "DCX_SPATIAL_EAST" -> List("5"),
      "DCX_SPATIAL_WEST" -> List("5")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [DCX_SPATIAL_SOUTH]"
    }
  }

  it should "succeed with required coordinates" in {
    val validDataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "DCX_SPATIAL_X" -> List("5", ""),
      "DCX_SPATIAL_Y" -> List("5", ""),
      "DCX_SPATIAL_NORTH" -> List("5", ""),
      "DCX_SPATIAL_SOUTH" -> List("5", ""),
      "DCX_SPATIAL_EAST" -> List("5", ""),
      "DCX_SPATIAL_WEST" -> List("5", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, validDataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed with valid schemes" in {
    val validDataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "DCT_TEMPORAL_SCHEME" -> List("abr:ABRperiode", ""),
      "DC_SUBJECT_SCHEME" -> List("abr:ABRcomplex", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, validDataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail with invalid temporal scheme" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCT_TEMPORAL_SCHEME" -> List("invalidTemporalScheme")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Wrong value: invalidTemporalScheme should be empty or one of: [abr:ABRperiode]"
    }
  }

  it should "fail with invalid subject scheme" in {
    val invalidDataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DC_SUBJECT_SCHEME" -> List("invalidSubjectScheme")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Wrong value: invalidSubjectScheme should be empty or one of: [abr:ABRcomplex]"
    }
  }

  it should "fail with relation qualifier, link and title" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List("foo"),
      "DCX_RELATION_LINK" -> List("bar"),
      "DCX_RELATION_TITLE" -> List("baz")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Only one of the following columns must contain a value: [DCX_RELATION_LINK, DCX_RELATION_TITLE]"
    }
  }

  it should "succeed with relation qualifier and link" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List("foo"),
      "DCX_RELATION_LINK" -> List("bar"),
      "DCX_RELATION_TITLE" -> List("")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed with relation qualifier and title" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List("foo"),
      "DCX_RELATION_LINK" -> List(""),
      "DCX_RELATION_TITLE" -> List("baz")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail with only relation qualifier and neither a link or title" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List("foo"),
      "DCX_RELATION_LINK" -> List(""),
      "DCX_RELATION_TITLE" -> List("")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Only one of the following columns must contain a value: [DCX_RELATION_LINK, DCX_RELATION_TITLE]"
    }
  }

  it should "fail without a relation qualifier, but both link and title" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List(""),
      "DCX_RELATION_LINK" -> List("bar"),
      "DCX_RELATION_TITLE" -> List("baz")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The columns [DCX_RELATION_LINK, DCX_RELATION_TITLE] must not all contain a value at the same time"
    }
  }

  it should "succeed without a relation qualifier and only a link" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List(""),
      "DCX_RELATION_LINK" -> List("bar"),
      "DCX_RELATION_TITLE" -> List("")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed without a relation qualifier and only a title" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List(""),
      "DCX_RELATION_LINK" -> List(""),
      "DCX_RELATION_TITLE" -> List("baz")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail without a relation qualifier, link or title" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30"),
      "DCX_RELATION_QUALIFIER" -> List(""),
      "DCX_RELATION_LINK" -> List(""),
      "DCX_RELATION_TITLE" -> List("")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the DDM_CREATED column is not defined" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID)
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column DDM_CREATED is not present in this instructions file"
    }
  }

  it should "fail when the DDM_CREATED column contains no non-blank values" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID, datasetID),
      "DDM_CREATED" -> List("  ", " ", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "No value defined for DDM_CREATED"
    }
  }

  it should "fail when the DDM_CREATED column contains multiple non-blank values" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", "2016-07-30")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "More than one value is defined for DDM_CREATED"
    }
  }

  it should "succeed when the DDM_CREATED column contains only one non-blank value but also other blank values" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("    ", "2017-07-30")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the DDM_CREATED column contains only one value that does not represent a date" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("foobar")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "'foobar' does not represent a date"
    }
  }

  it should "succeed when the DDM_CREATED column contains only one value with a different formatting" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID),
      "DDM_CREATED" -> List("2017-07-30T09:00:34.921+02:00")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  "execute" should "write the metadata to a file at the correct place" in {
    val file = outputDatasetMetadataFile(datasetID)

    file should not (exist)

    AddDatasetMetadataToDeposit(1, ("ds1", dataset)).execute shouldBe a[Success[_]]

    file should exist
  }

  "datasetToXml" should "return the expected xml" in {
    verify(datasetToXml(dataset), expectedXml)
  }

  it should "return xml on reading from the sip-demo csv" in {
    inside(toXml("/spacetravel/instructions.csv")) {
      case Success(xmls) => xmls should have size 2
    }
  }

  it should "return xml on reading from the sip001 csv" in {
    inside(toXml("/sip001/instructions.csv")) {
      case Success(xmls) => xmls should have size 2
    }
  }

  def toXml(file: String): Try[Seq[Elem]] = {
    val csv = new File(getClass.getResource(file).toURI)
    MultiDepositParser.parse(csv)
      .map(dss => dss.map { case (_, ds) => AddDatasetMetadataToDeposit.datasetToXml(ds) })
  }

  "createDcmiMetadata" should "return the expected spatial elements" in {
    val dataset = new Dataset() +=
      "DCT_SPATIAL" -> List("here", "there", "", "") +=
      "DCX_SPATIAL_SCHEME" -> List("degrees", "RD", "", "") +=
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
            <Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
              <lowerCorner>1 3</lowerCorner>
              <upperCorner>2 4</upperCorner>
            </Envelope>
          </boundedBy>
        </dcx-gml:spatial>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
  }

  it should "have a organization-contributor when no other author-details are given" in {
    val dataset = new Dataset() +=
      "DCX_CONTRIBUTOR_TITLES" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_INITIALS" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_INSERTIONS" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_SURNAME" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_DAI" -> List("", "", "", "") +=
      "DCX_CONTRIBUTOR_ORGANIZATION" -> List("some org", "", "", "")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcx-dai:contributorDetails>
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">some org</dcx-dai:name>
          </dcx-dai:organization>
        </dcx-dai:contributorDetails>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
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
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
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
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
  }

  it should "return the expected dcmidata" in {
    val dataset = new Dataset() +=
      "DCT_RIGHTSHOLDER" -> List("rh1", "", "", "") +=
      "DCT_TEMPORAL" -> List("1992-2016", "PALEOV", "2005", "some arbitrary text") +=
      "DCT_TEMPORAL_SCHEME" -> List("", "abr:ABRperiode", "", "") +=
      "DC_PUBLISHER" -> List("pub1", "", "", "") +=
      "DC_FORMAT" -> List("text", "", "", "") +=
      "DC_SUBJECT" -> List("me", "you", "him", "GX") +=
      "DC_SUBJECT_SCHEME" -> List("", "", "", "abr:ABRcomplex") +=
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
        <dc:subject xsi:type="abr:ABRcomplex">GX</dc:subject>
        <dcterms:temporal>1992-2016</dcterms:temporal>
        <dcterms:temporal xsi:type="abr:ABRperiode">PALEOV</dcterms:temporal>
        <dcterms:temporal>2005</dcterms:temporal>
        <dcterms:temporal>some arbitrary text</dcterms:temporal>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
  }

  it should "return the expected streaming surrogate relation" in {
    val dataset = new Dataset() +=
      "SF_DOMAIN" -> List("randomdomainname") +=
      "SF_USER" -> List("randomusername") +=
      "SF_COLLECTION" -> List("randomcollectionname") +=
      "SF_PRESENTATION" -> List("randompresentationname")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <ddm:relation scheme="STREAMING_SURROGATE_RELATION">/domain/randomdomainname/user/randomusername/collection/randomcollectionname/presentation/randompresentationname</ddm:relation>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
  }

  def verify(actualXml: Elem, expectedXml: Elem): Unit = {
    Utility.trim(actualXml).toString() shouldBe Utility.trim(expectedXml).toString()
  }
}
