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
import scala.xml.{ Elem, Node, Utility }

class AddDatasetMetadataToDepositSpec extends UnitSpec with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "sd")
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
  xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd">
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    new File(getClass.getResource("/allfields/input/ruimtereis01/reisverslag/centaur.mpg").toURI)
      .copyFile(new File(settings.multidepositDir, s"$datasetID/reisverslag/centaur.mpg"))
    new File(getClass.getResource("/allfields/input/ruimtereis01/reisverslag/centaur.srt").toURI)
      .copyFile(new File(settings.multidepositDir, s"$datasetID/reisverslag/centaur.srt"))
  }
  
  private def basicDataset: Dataset = mutable.HashMap(
    "DATASET" -> List(datasetID, datasetID),
    "DDM_CREATED" -> List("2017-07-30", ""),
    "DDM_ACCESSRIGHTS" -> List("OPEN_ACCESS", ""),
    "DDM_AVAILABLE" -> List("2017-07-31", ""))

 "checkPreconditions" should "succeed with correctly corresponding access rights and audience" in {
   val validDataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
     "DDM_ACCESSRIGHTS" -> List("GROUP_ACCESS", ""),
     "DDM_AUDIENCE" -> List("D37000", "") // Archaeology
   )

   new AddDatasetMetadataToDeposit(1, (datasetID, validDataset)).checkPreconditions shouldBe a[Success[_]]
 }

  it should "fail with incorrectly corresponding access rights and audience" in {
    val invalidDataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
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
    val invalidDataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
      "DDM_ACCESSRIGHTS" -> List("GROUP_ACCESS", ""),
      "DDM_AUDIENCE" -> List("", "") // Humanities
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeologie), but it is not defined"
    }
  }

  it should "fail with an incorrect access right" in {
    val dataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
      "DDM_ACCESSRIGHTS" -> List("INCORRECT_ACCESS", "")
    )
    inside(AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message should include ("Wrong value: INCORRECT_ACCESS")
    }
  }

  it should "fail with a correct access right that is not 'all-caps'" in {
    val dataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
      "DDM_ACCESSRIGHTS" -> List("open_access", "")
    )
    inside(AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message should include ("Wrong value: open_access")
    }
  }

  it should "fail when the DDM_ACCESSRIGHTS column is not defined" in {
    val dataset = basicDataset -= "DDM_ACCESSRIGHTS"

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column DDM_ACCESSRIGHTS is not present in this instructions file"
    }
  }

  it should "fail when the DDM_ACCESSRIGHTS column contains no non-blank values" in {
    val dataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
      "DDM_ACCESSRIGHTS" -> List("  ", " ")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "No value defined for DDM_ACCESSRIGHTS"
    }
  }

  it should "fail when the DDM_ACCESSRIGHTS column contains multiple non-blank values" in {
    val dataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
      "DDM_ACCESSRIGHTS" -> List("OPEN_ACCESS", "NO_ACCESS")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "More than one value is defined for DDM_ACCESSRIGHTS"
    }
  }

  it should "succeed when the DDM_ACCESSRIGHTS column contains only one non-blank value but also other blank values" in {
    val dataset = basicDataset -= "DDM_ACCESSRIGHTS" ++= List(
      "DDM_ACCESSRIGHTS" -> List("    ", "OPEN_ACCESS")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed with required person information" in {
    val validDataset = basicDataset ++= List(
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
    val invalidDataset = basicDataset ++= List(
      "DCX_CREATOR_TITLES" -> List("Prof", ""),
      "DCX_CREATOR_INITIALS" -> List("F.A.I.L", ""),
      "DCX_CREATOR_INSERTIONS" -> List("", ""),
      "DCX_CREATOR_SURNAME" -> List("", ""),
      "DCX_CREATOR_DAI" -> List("", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [DCX_CREATOR_SURNAME]"
    }
  }

  it should "fail with missing required point coordinates" in {
    val invalidDataset = basicDataset ++= List(
      "DCX_SPATIAL_X" -> List("5", ""),
      "DCX_SPATIAL_Y" -> List("", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [DCX_SPATIAL_Y]"
    }
  }

  it should "fail with missing required box coordinates" in {
    val invalidDataset = basicDataset ++= List(
      "DCX_SPATIAL_NORTH" -> List("5", ""),
      "DCX_SPATIAL_SOUTH" -> List("", ""),
      "DCX_SPATIAL_EAST" -> List("5", ""),
      "DCX_SPATIAL_WEST" -> List("5", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [DCX_SPATIAL_SOUTH]"
    }
  }

  it should "succeed with required coordinates" in {
    val validDataset = basicDataset ++= List(
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
    val validDataset = basicDataset ++= List(
      "DCT_TEMPORAL_SCHEME" -> List("abr:ABRperiode", ""),
      "DC_SUBJECT_SCHEME" -> List("abr:ABRcomplex", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, validDataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail with invalid temporal scheme" in {
    val invalidDataset = basicDataset ++= List(
      "DCT_TEMPORAL_SCHEME" -> List("invalidTemporalScheme", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Wrong value: invalidTemporalScheme should be empty or one of: [abr:ABRperiode]"
    }
  }

  it should "fail with invalid subject scheme" in {
    val invalidDataset = basicDataset ++= List(
      "DC_SUBJECT_SCHEME" -> List("invalidSubjectScheme", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, invalidDataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Wrong value: invalidSubjectScheme should be empty or one of: [abr:ABRcomplex]"
    }
  }

  it should "fail with relation qualifier, link and title" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("foo", ""),
      "DCX_RELATION_LINK" -> List("bar", ""),
      "DCX_RELATION_TITLE" -> List("baz", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Only one of the following columns must contain a value: [DCX_RELATION_LINK, DCX_RELATION_TITLE]"
    }
  }

  it should "succeed with relation qualifier and link" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("foo", ""),
      "DCX_RELATION_LINK" -> List("bar", ""),
      "DCX_RELATION_TITLE" -> List("", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed with relation qualifier and title" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("foo", ""),
      "DCX_RELATION_LINK" -> List("", ""),
      "DCX_RELATION_TITLE" -> List("baz", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail with only relation qualifier and neither a link or title" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("foo", ""),
      "DCX_RELATION_LINK" -> List("", ""),
      "DCX_RELATION_TITLE" -> List("", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Only one of the following columns must contain a value: [DCX_RELATION_LINK, DCX_RELATION_TITLE]"
    }
  }

  it should "fail without a relation qualifier, but both link and title" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("", ""),
      "DCX_RELATION_LINK" -> List("bar", ""),
      "DCX_RELATION_TITLE" -> List("baz", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The columns [DCX_RELATION_LINK, DCX_RELATION_TITLE] must not all contain a value at the same time"
    }
  }

  it should "succeed without a relation qualifier and only a link" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("", ""),
      "DCX_RELATION_LINK" -> List("bar", ""),
      "DCX_RELATION_TITLE" -> List("", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed without a relation qualifier and only a title" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("", ""),
      "DCX_RELATION_LINK" -> List("", ""),
      "DCX_RELATION_TITLE" -> List("baz", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail without a relation qualifier, link or title" in {
    val dataset = basicDataset ++= List(
      "DCX_RELATION_QUALIFIER" -> List("", ""),
      "DCX_RELATION_LINK" -> List("", ""),
      "DCX_RELATION_TITLE" -> List("", "")
    )

    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the DDM_CREATED column is not defined" in {
    val dataset = basicDataset -= "DDM_CREATED"

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column DDM_CREATED is not present in this instructions file"
    }
  }

  it should "fail when the DDM_CREATED column contains no non-blank values" in {
    val dataset = basicDataset -= "DDM_CREATED" ++= List(
      "DDM_CREATED" -> List("  ", " ")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "No value defined for DDM_CREATED"
    }
  }

  it should "fail when the DDM_CREATED column contains multiple non-blank values" in {
    val dataset = basicDataset -= "DDM_CREATED" ++= List(
      "DDM_CREATED" -> List("2017-07-30", "2016-07-30")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "More than one value is defined for DDM_CREATED"
    }
  }

  it should "succeed when the DDM_CREATED column contains only one non-blank value but also other blank values" in {
    val dataset = basicDataset -= "DDM_CREATED" ++= List(
      "DDM_CREATED" -> List("    ", "2017-07-30")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the DDM_CREATED column contains only one value that does not represent a date" in {
    val dataset = basicDataset -= "DDM_CREATED" ++= List(
      "DDM_CREATED" -> List("foobar", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "DDM_CREATED 'foobar' does not represent a date"
    }
  }

  it should "succeed when the DDM_CREATED column contains only one value with a different formatting" in {
    val dataset = basicDataset -= "DDM_CREATED" ++= List(
      "DDM_CREATED" -> List("2017-07-30T09:00:34.921+02:00", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the DDM_AVAILABLE column contains a wrong format" in {
    val dataset = basicDataset -= "DDM_AVAILABLE" ++= List(
      "DDM_AVAILABLE" -> List("01-01-2017", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
        case Failure(CompositeException(es)) =>
          val ActionException(_, message, _) :: Nil = es.toList
          message shouldBe "DDM_AVAILABLE '01-01-2017' does not represent a date"
      }
  }

  it should "fail when the DDM_AVAILABLE column is not defined" in {
    val dataset = basicDataset -= "DDM_AVAILABLE"

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column DDM_AVAILABLE is not present in this instructions file"
    }
  }

  it should "fail when the DDM_AVAILABLE column contains no non-blank values" in {
    val dataset = basicDataset -= "DDM_AVAILABLE" ++= List(
      "DDM_AVAILABLE" -> List("  ", " ")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "No value defined for DDM_AVAILABLE"
    }
  }

  it should "fail when the DDM_AVAILABLE column contains multiple non-blank values" in {
    val dataset = basicDataset -= "DDM_AVAILABLE" ++= List(
      "DDM_AVAILABLE" -> List("2017-07-30", "2016-07-30")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "More than one value is defined for DDM_AVAILABLE"
    }
  }

  it should "succeed when the DDM_AVAILABLE column contains only one non-blank value but also other blank values" in {
    val dataset = basicDataset -= "DDM_AVAILABLE" ++= List(
      "DDM_AVAILABLE" -> List("    ", "2017-07-30")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the DDM_AVAILABLE column contains only one value that does not represent a date" in {
    val dataset = basicDataset -= "DDM_AVAILABLE" ++= List(
      "DDM_AVAILABLE" -> List("foobar", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "DDM_AVAILABLE 'foobar' does not represent a date"
    }
  }

  it should "succeed when the DDM_AVAILABLE column contains only one value with a different formatting" in {
    val dataset = basicDataset -= "DDM_AVAILABLE" ++= List(
      "DDM_AVAILABLE" -> List("2017-07-30T09:00:34.921+02:00", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed when the SF columns only contain one row with values" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail when the SF columns contain multiple rows with values" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", "domain2"),
      "SF_USER" -> List("user", "user2"),
      "SF_COLLECTION" -> List("collection", "collection2")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Only one row can contain values for all these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]"
    }
  }

  it should "fail when the SF columns have values spread over multiple rows" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("", "user"),
      "SF_COLLECTION" -> List("collection", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Only one row can contain values for all these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]"
    }
  }

  it should "fail if the SF_COLLECTION contains forbidden characters" in {
    val dataset = basicDataset ++= List(
      "SF_COLLECTION" -> List("do main", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, ("no_weird_char", dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column 'SF_COLLECTION' contains the following invalid characters: { ' ' }"
    }
  }

  it should "fail if the SF_USER contains forbidden characters" in {
    val dataset = basicDataset ++= List(
      "SF_USER" -> List("do%main", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, ("no_weird_char", dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column 'SF_USER' contains the following invalid characters: { '%' }"
    }
  }

  it should "fail if the SF_DOMAIN contains forbidden characters" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("do/main", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, ("no_weird_char", dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column 'SF_DOMAIN' contains the following invalid characters: { '/' }"
    }
  }

  it should "fail if the DATASET contains forbidden characters" in {
    val dataset = basicDataset -= "DATASET" ++= List(
      "DATASET" -> List("weird#char", "weird#char")
    )
    inside(new AddDatasetMetadataToDeposit(1, ("weird#char", dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        es should have size 2
        es.head.getMessage shouldBe "The column 'DATASET' contains the following invalid characters: { '#' }"
    }
  }

  it should "succeed if the AV_FILEs exist" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the AV_FILE does not exist" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur-does-not-exist.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe s"AV_FILE '$datasetID/reisverslag/centaur-does-not-exist.mpg' does not exist"
    }
  }

  it should "succeed if the AV_SUBTITLESs exist" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the AV_SUBTITLES does not exist" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur-does-not-exist.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe s"AV_SUBTITLES '$datasetID/reisverslag/centaur-does-not-exist.srt' does not exist"
    }
  }

  it should "succeed if the AV_FILE is present when the AV_FILE_TITLE is present" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_FILE_TITLE" -> List("hello world", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the AV_FILE is present when the AV_SUBTITLES and AV_SUBTITLES_LANGUAGE are present" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the AV_FILE is present when the AV_FILE_TITLE and AV_SUBTITLES and AV_SUBTITLES_LANGUAGE are present" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_FILE_TITLE" -> List("hello world", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the AV_FILE is not present when the AV_FILE_TITLE is present" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE_TITLE" -> List("hello world", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [AV_FILE]"
    }
  }

  it should "fail if the AV_FILE is not present when the AV_SUBTITLES and AV_SUBTITLES_LANGUAGE are present" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )

    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [AV_FILE]"
    }
  }

  it should "fail if the AV_FILE is not present when the AV_FILE_TITLE and AV_SUBTITLES and AV_SUBTITLES_LANGUAGE are present" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE_TITLE" -> List("hello world", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [AV_FILE]"
    }
  }

  it should "succeed if AV_SUBTITLES is given, but AV_SUBTITLES_LANGUAGE is not" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_FILE_TITLE" -> List("hello world", ""),
      "AV_SUBTITLES" -> List(s"$datasetID/reisverslag/centaur.srt", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if AV_SUBTITLES_LANGUAGE is given, but AV_SUBTITLES is not" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", ""),
      "AV_FILE_TITLE" -> List("hello world", ""),
      "AV_SUBTITLES" -> List("", ""),
      "AV_SUBTITLES_LANGUAGE" -> List("en", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "Missing value(s) for: [AV_SUBTITLES]"
    }
  }

  it should "fail if AV_FILE is given, SF_USER is given, but SF_COLLECTION is not" in {
    val dataset = basicDataset ++= List(
      "SF_USER" -> List("user", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_USER is given, but SF_COLLECTION has empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_DOMAIN and SF_USER have values too, but SF_COLLECTION is missing" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_DOMAIN and SF_USER have values too, but SF_COLLECTION has empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_COLLECTION is given, but SF_USER is not" in {
    val dataset = basicDataset ++= List(
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_COLLECTION is given, but SF_USER has empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_DOMAIN and SF_COLLECTION have values too, but SF_USER is missing" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_DOMAIN and SF_COLLECTION have values too, but SF_USER has empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("collection", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, but SF_COLLECTION and SF_USER are not" in {
    val dataset = basicDataset ++= List(
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION, SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, but SF_COLLECTION and SF_USER have empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION, SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_DOMAIN has values too, but SF_COLLECTION and SF_USER are missing" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION, SF_USER] do not"
    }
  }

  it should "fail if AV_FILE is given, SF_DOMAIN has values too, but SF_COLLECTION and SF_USER have empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", ""),
      "AV_FILE" -> List(s"$datasetID/reisverslag/centaur.mpg", "")
    )
    inside(new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message shouldBe "The column AV_FILE contains values, but the column(s) [SF_COLLECTION, SF_USER] do not"
    }
  }

  it should "succeed if AV_FILE is not given, and SF_COLLECTION and SF_USER have empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if AV_FILE and SF_COLLECTION both have empty values only" in {
    val dataset = basicDataset ++= List(
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", ""),
      "AV_FILE" -> List("", "")
    )
    new AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed with correct file access right" in {
    val dataset = basicDataset ++= List(
      "SF_ACCESSIBILITY" -> List("ANONYMOUS", "")
    )
    AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail with an incorrect file access right" in {
    val dataset = basicDataset ++= List(
      "SF_ACCESSIBILITY" -> List("INCORRECT_ACCESS", "")
    )
    inside(AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message should include ("Wrong value: INCORRECT_ACCESS")
    }
  }

  it should "fail with a correct file access right that is not 'all-caps'" in {
    val dataset = basicDataset ++= List(
      "SF_ACCESSIBILITY" -> List("anonymous", "")
    )
    inside(AddDatasetMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(CompositeException(es)) =>
        val ActionException(_, message, _) :: Nil = es.toList
        message should include ("Wrong value: anonymous")
    }
  }

  "execute" should "write the metadata to a file at the correct place" in {
    val file = stagingDatasetMetadataFile(datasetID)

    file should not (exist)

    AddDatasetMetadataToDeposit(1, ("ds1", dataset)).execute shouldBe a[Success[_]]

    file should exist
  }

  "datasetToXml" should "return the expected xml" in {
    verify(datasetToXml(dataset), expectedXml)
  }

  it should "return xml on reading from the allfields input instructions csv" in {
    val csv = new File(getClass.getResource("/allfields/input/instructions.csv").toURI)
    inside(MultiDepositParser.parse(csv).map(_.map { case (_, ds) => datasetToXml(ds) })) {
      case Success(xmls) => xmls should have size 3
    }
  }

  def toXml(file: String): Try[Seq[Elem]] = {
    val csv = new File(getClass.getResource(file).toURI)
    MultiDepositParser.parse(csv)
      .map(dss => dss.map { case (_, ds) => AddDatasetMetadataToDeposit.datasetToXml(ds) })
  }

  "createDcmiMetadata" should "return the expected spatial Box elements" in {
    val dataset = new Dataset() +=
      "DCT_SPATIAL" -> List("here", "there", "", "") +=
      "DCX_SPATIAL_SCHEME" -> List("degrees", "RD", "", "") +=
      "DCX_SPATIAL_NORTH" -> List("4", "40", "", "") +=
      "DCX_SPATIAL_SOUTH" -> List("3", "30", "", "") +=
      "DCX_SPATIAL_EAST" -> List("2", "20", "", "") +=
      "DCX_SPATIAL_WEST" -> List("1", "10", "", "")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcterms:spatial>here</dcterms:spatial>
        <dcterms:spatial>there</dcterms:spatial>
        <dcx-gml:spatial>
          <boundedBy xmlns="http://www.opengis.net/gml">
            <Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
              <lowerCorner>3 1</lowerCorner>
              <upperCorner>4 2</upperCorner>
            </Envelope>
          </boundedBy>
        </dcx-gml:spatial>
        <dcx-gml:spatial>
          <boundedBy xmlns="http://www.opengis.net/gml">
            <Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
              <lowerCorner>10 30</lowerCorner>
              <upperCorner>20 40</upperCorner>
            </Envelope>
          </boundedBy>
        </dcx-gml:spatial>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
  }

  it should "return the expected spatial Point elements" in {
    val dataset = new Dataset() +=
      "DCT_SPATIAL" -> List("here", "there", "", "") +=
      "DCX_SPATIAL_SCHEME" -> List("degrees", "RD", "", "") +=
      "DCX_SPATIAL_X" -> List("83575.4", "210902", "", "") +=
      "DCX_SPATIAL_Y" -> List("455271.2", "442193", "", "")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <dcterms:spatial>here</dcterms:spatial>
        <dcterms:spatial>there</dcterms:spatial>
        <dcx-gml:spatial srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
          <Point xmlns="http://www.opengis.net/gml">
            <pos>455271.2 83575.4</pos>
          </Point>
        </dcx-gml:spatial>
        <dcx-gml:spatial srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
          <Point xmlns="http://www.opengis.net/gml">
            <pos>210902 442193</pos>
          </Point>
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
      "SF_COLLECTION" -> List("randomcollectionname")
    val expectedXml = <ddm>
      <ddm:dcmiMetadata>
        <ddm:relation scheme="STREAMING_SURROGATE_RELATION">/domain/randomdomainname/user/randomusername/collection/randomcollectionname/presentation/$presentation-placeholder</ddm:relation>
      </ddm:dcmiMetadata>
    </ddm>
    verify(<ddm>{AddDatasetMetadataToDeposit.createMetadata(dataset)}</ddm>, expectedXml)
  }

  def verify(actualXml: Node, expectedXml: Node): Unit = {
    Utility.trim(actualXml).toString() shouldBe Utility.trim(expectedXml).toString()
  }
}
