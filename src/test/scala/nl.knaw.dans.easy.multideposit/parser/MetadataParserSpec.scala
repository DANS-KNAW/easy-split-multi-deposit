/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit.parser

import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, UnitSpec }
import org.joda.time.DateTime

import scala.util.{ Failure, Success }

trait MetadataTestObjects {

  lazy val metadataCSV @ metadataCSVRow1 :: metadataCSVRow2 :: Nil = List(
    Map(
      "DCT_ALTERNATIVE" -> "alt1",
      "DC_PUBLISHER" -> "pub1",
      "DC_TYPE" -> "Collection",
      "DC_FORMAT" -> "format1",
      // identifier
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "ARCHIS-ZAAK-IDENTIFICATIE",
      "DC_SOURCE" -> "src1",
      "DC_LANGUAGE" -> "dut",
      "DCT_SPATIAL" -> "spat1",
      "DCT_RIGHTSHOLDER" -> "right1",
      // relation
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar",
      // date
      "DCT_DATE" -> "2016-02-01",
      "DCT_DATE_QUALIFIER" -> "Date Submitted",
      // contributor
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ROLE" -> "RelatedPerson",
      // subject
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex",
      // spatialPoint
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> "degrees",
      // temporal
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    ),
    Map(
      "DCT_ALTERNATIVE" -> "alt2",
      "DC_PUBLISHER" -> "pub2",
      "DC_TYPE" -> "MovingImage",
      "DC_FORMAT" -> "format2",
      "DC_IDENTIFIER" -> "id",
      "DC_SOURCE" -> "src2",
      "DC_LANGUAGE" -> "nld",
      "DCT_SPATIAL" -> "spat2",
      "DCT_RIGHTSHOLDER" -> "right2",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar",
      "DCT_DATE" -> "some random text",
      // spatialBox
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> "RD"
    )
  )

  lazy val metadata = Metadata(
    alternatives = List("alt1", "alt2"),
    publishers = List("pub1", "pub2"),
    types = List(DcType.COLLECTION, DcType.MOVINGIMAGE),
    formats = List("format1", "format2"),
    identifiers = List(Identifier("123456", Some(IdentifierType.ARCHIS_ZAAK_IDENTIFICATIE)), Identifier("id")),
    sources = List("src1", "src2"),
    languages = List("dut", "nld"),
    spatials = List("spat1", "spat2"),
    rightsholder = List("right1", "right2"),
    relations = List(QualifiedRelation(RelationQualifier.Replaces, link = Some("foo"), title = Some("bar")), UnqualifiedRelation(link = Some("foo"), title = Some("bar"))),
    dates = List(QualifiedDate(new DateTime(2016, 2, 1, 0, 0), DateQualifier.DATE_SUBMITTED), TextualDate("some random text")),
    contributors = List(ContributorPerson(initials = "A.", surname = "Jones", role = Some(ContributorRole.RELATED_PERSON))),
    subjects = List(Subject("IX", Option("abr:ABRcomplex"))),
    spatialPoints = List(SpatialPoint("12", "34", Option("degrees"))),
    spatialBoxes = List(SpatialBox("45", "34", "23", "12", Option("RD"))),
    temporal = List(Temporal("PALEOLB", Option("abr:ABRperiode")))
  )
}

class MetadataParserSpec extends UnitSpec with MetadataTestObjects {

  private val parser = new MetadataParser with ParserUtils {}

  import parser._

  "extractMetadata" should "convert the csv input to the corresponding output" in {
    extractMetadata(metadataCSV) should matchPattern { case Success(`metadata`) => }
  }

  it should "use the default type value if no value for DC_TYPE is specified" in {
    inside(extractMetadata(metadataCSV.map(row => row - "DC_TYPE"))) {
      case Success(md) => md.types should contain only DcType.DATASET
    }
  }

  "contributor" should "return None if the none of the fields are defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    )

    contributor(2)(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION is defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    )

    contributor(2)(row).value should matchPattern { case Success(ContributorOrganization("org", None)) => }
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION and DCX_CONTRIBUTOR_ROLE are defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> "RelatedPERSON"
    )

    contributor(2)(row).value should matchPattern { case Success(ContributorOrganization("org", Some(ContributorRole.RELATED_PERSON))) => }
  }

  it should "succeed with a person when only DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Success(ContributorPerson(None, "A.", None, "Jones", None, None, None)) =>
    }
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "X",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "dai123",
      "DCX_CONTRIBUTOR_ROLE" -> "related person"
    )

    contributor(2)(row).value should matchPattern {
      case Success(ContributorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some(ContributorRole.RELATED_PERSON), Some("dai123"))) =>
    }
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS is not defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: DCX_CONTRIBUTOR_INITIALS", _)) =>
    }
  }

  it should "fail if DCX_CONTRIBUTOR_SURNAME is not defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: DCX_CONTRIBUTOR_SURNAME", _)) =>
    }
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are both not defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CONTRIBUTOR_SURNAME, DCX_CONTRIBUTOR_INITIALS]", _)) =>
    }
  }

  it should "fail if DCX_CREATOR_ROLE has an invalid value" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> "invalid!"
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'invalid!' is not a valid contributor role", _)) =>
    }
  }

  "identifier" should "return None if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are not defined" in {
    val row = Map(
      "DC_IDENTIFIER" -> "",
      "DC_IDENTIFIER_TYPE" -> ""
    )
    identifier(2)(row) shouldBe empty
  }

  it should "fail if DC_IDENTIFIER_TYPE is defined, but DC_IDENTIFIER is not" in {
    val row = Map(
      "DC_IDENTIFIER" -> "",
      "DC_IDENTIFIER_TYPE" -> "ISSN"
    )
    identifier(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: DC_IDENTIFIER", _)) =>
    }
  }

  it should "succeed if DC_IDENTIFIER is defined and DC_IDENTIFIER_TYPE is not" in {
    val row = Map(
      "DC_IDENTIFIER" -> "id",
      "DC_IDENTIFIER_TYPE" -> ""
    )
    identifier(2)(row).value should matchPattern { case Success(Identifier("id", None)) => }
  }

  it should "succeed if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined and DC_IDENTIFIER_TYPE is valid" in {
    val row = Map(
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "ISSN"
    )
    identifier(2)(row).value should matchPattern { case Success(Identifier("123456", Some(IdentifierType.ISSN))) => }
  }

  it should "fail if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined, but DC_IDENTIFIER_TYPE is invalid" in {
    val row = Map(
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "INVALID_IDENTIFIER_TYPE"
    )
    identifier(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'INVALID_IDENTIFIER_TYPE' is not a valid identifier type", _)) =>
    }
  }

  "dcType" should "convert the value for DC_TYPE into the corresponding enum object" in {
    val row = Map("DC_TYPE" -> "Collection")
    dcType(2)(row).value should matchPattern { case Success(DcType.COLLECTION) => }
  }

  it should "return None if DC_TYPE is not defined" in {
    val row = Map("DC_TYPE" -> "")
    dcType(2)(row) shouldBe empty
  }

  it should "fail if the DC_TYPE value does not correspond to an object in the enum" in {
    val row = Map("DC_TYPE" -> "unknown value")
    dcType(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid type", _)) =>
    }
  }

  "relation" should "succeed if both the link and title are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Success(UnqualifiedRelation(Some("foo"), Some("bar"))) =>
    }
  }

  it should "succeed if the qualifier and both the link and title are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Success(QualifiedRelation(RelationQualifier.Replaces, Some("foo"), Some("bar"))) =>
    }
  }

  it should "fail when only the qualifier and link are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context", _)) =>
    }
  }

  it should "succeed when only the qualifier and title are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Success(QualifiedRelation(RelationQualifier.Replaces, None, Some("bar"))) =>
    }
  }

  it should "fail if only the qualifier is defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well", _)) =>
    }
  }

  it should "fail if an invalid qualifier is given" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "invalid",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'invalid' is not a valid relation qualifier", _)) =>
    }
  }

  it should "succeed if the qualifier is formatted differently" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "rEplAcEs",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Success(QualifiedRelation(RelationQualifier.Replaces, Some("foo"), Some("bar"))) =>
    }
  }

  it should "fail if only the link is defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context", _)) =>
    }
  }

  it should "succeed if only the title is defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Success(UnqualifiedRelation(None, Some("bar"))) =>
    }
  }

  it should "return None if none of these fields are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row) shouldBe empty
  }

  "date" should "convert a textual date into the corresponding object" in {
    val row = Map(
      "DCT_DATE" -> "random text",
      "DCT_DATE_QUALIFIER" -> ""
    )

    dateColumn(2)(row).value should matchPattern { case Success(TextualDate("random text")) => }
  }

  it should "fail if it has a correct qualifier but no well formatted date" in {
    val row = Map(
      "DCT_DATE" -> "random text",
      "DCT_DATE_QUALIFIER" -> "Valid"
    )

    dateColumn(2)(row).value should matchPattern { case Failure(ParseException(2, "DCT_DATE value 'random text' does not represent a date", _)) => }
  }

  it should "convert a date with qualifier into the corresponding object" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "Issued"
    )

    inside(dateColumn(2)(row).value) {
      case Success(QualifiedDate(date, DateQualifier.ISSUED)) =>
        date shouldBe new DateTime(2016, 7, 30, 0, 0)
    }
  }

  it should "succeed when the qualifier is formatted differently (capitals)" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "dateAccepted"
    )

    inside(dateColumn(2)(row).value) {
      case Success(QualifiedDate(date, DateQualifier.DATE_ACCEPTED)) =>
        date shouldBe new DateTime(2016, 7, 30, 0, 0)
    }
  }

  it should "succeed when the qualifier contains spaces instead of CamelCase" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "date accepted"
    )

    inside(dateColumn(2)(row).value) {
      case Success(QualifiedDate(date, DateQualifier.DATE_ACCEPTED)) =>
        date shouldBe new DateTime(2016, 7, 30, 0, 0)
    }
  }

  it should "succeed if the qualifier isn't given, but the date is (properly formatted)" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> ""
    )

    dateColumn(2)(row).value should matchPattern { case Success(TextualDate("2016-07-30")) => }
  }

  it should "fail if the qualifier is unknown" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "unknown"
    )

    dateColumn(2)(row).value should matchPattern { case Failure(ParseException(2, "Value 'unknown' is not a valid date qualifier", _)) => }
  }

  it should "fail if the date isn't properly formatted" in {
    val row = Map(
      "DCT_DATE" -> "30-07-2016",
      "DCT_DATE_QUALIFIER" -> "Issued"
    )

    dateColumn(2)(row).value should matchPattern { case Failure(ParseException(2, "DCT_DATE value '30-07-2016' does not represent a date", _)) => }
  }

  it should "fail if no date is given, but a valid qualifier is given" in {
    val row = Map(
      "DCT_DATE" -> "",
      "DCT_DATE_QUALIFIER" -> "Issued"
    )

    dateColumn(2)(row).value should matchPattern { case Failure(ParseException(2, "DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it", _)) => }
  }

  it should "fail if the qualifier is equal to 'available' since we use a different keyword for that" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "Available"
    )

    dateColumn(2)(row).value should matchPattern { case Failure(ParseException(2, "DCT_DATE_QUALIFIER value 'Available' is not allowed here. Use column DDM_AVAILABLE instead.", _)) => }
  }

  it should "fail if the qualifier is equal to 'created' since we use a different keyword for that" in {
    val row = Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "Created"
    )

    dateColumn(2)(row).value should matchPattern { case Failure(ParseException(2, "DCT_DATE_QUALIFIER value 'Created' is not allowed here. Use column DDM_CREATED instead.", _)) => }
  }

  it should "return an empty value if both values are empty" in {
    val row = Map(
      "DCT_DATE" -> "",
      "DCT_DATE_QUALIFIER" -> ""
    )

    dateColumn(2)(row) shouldBe empty
  }

  "subject" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex"
    )

    subject(2)(row).value should matchPattern { case Success(Subject("IX", Some("abr:ABRcomplex"))) => }
  }

  it should "succeed when the scheme is not defined" in {
    val row = Map(
      "DC_SUBJECT" -> "test",
      "DC_SUBJECT_SCHEME" -> ""
    )

    subject(2)(row).value should matchPattern { case Success(Subject("test", None)) => }
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = Map(
      "DC_SUBJECT" -> "",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex"
    )

    subject(2)(row).value should matchPattern { case Success(Subject("", Some("abr:ABRcomplex"))) => }
  }

  it should "fail if the scheme is not recognized" in {
    val row = Map(
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "random-incorrect-scheme"
    )

    subject(2)(row).value should matchPattern {
      case Failure(ParseException(2, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'", _)) =>
    }
  }

  it should "return None when both fields are empty or blank" in {
    val row = Map(
      "DC_SUBJECT" -> "",
      "DC_SUBJECT_SCHEME" -> ""
    )

    subject(2)(row) shouldBe empty
  }

  "temporal" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    )

    temporal(2)(row).value should matchPattern {
      case Success(Temporal("PALEOLB", Some("abr:ABRperiode"))) =>
    }
  }

  it should "succeed when the scheme is not defined" in {
    val row = Map(
      "DCT_TEMPORAL" -> "test",
      "DCT_TEMPORAL_SCHEME" -> ""
    )

    temporal(2)(row).value should matchPattern { case Success(Temporal("test", None)) => }
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = Map(
      "DCT_TEMPORAL" -> "",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    )

    temporal(2)(row).value should matchPattern { case Success(Temporal("", Some("abr:ABRperiode"))) => }
  }

  it should "fail if the scheme is not recognized" in {
    val row = Map(
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "random-incorrect-scheme"
    )

    temporal(2)(row).value should matchPattern {
      case Failure(ParseException(2, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'", _)) =>
    }
  }

  it should "return None when both fields are empty or blank" in {
    val row = Map(
      "DCT_TEMPORAL" -> "",
      "DCT_TEMPORAL_SCHEME" -> ""
    )

    temporal(2)(row) shouldBe empty
  }

  "spatialPoint" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> "degrees"
    )

    spatialPoint(2)(row).value should matchPattern {
      case Success(SpatialPoint("12", "34", Some("degrees"))) =>
    }
  }

  it should "succeed when no scheme is defined" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialPoint(2)(row).value should matchPattern { case Success(SpatialPoint("12", "34", None)) => }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialPoint(2)(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_SCHEME" -> "degrees"
    )

    spatialPoint(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: DCX_SPATIAL_Y", _)) =>
    }
  }

  "spatialBox" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> "RD"
    )

    spatialBox(2)(row).value should matchPattern {
      case Success(SpatialBox("45", "34", "23", "12", Some("RD"))) =>
    }
  }

  it should "succeed when no scheme is defined" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialBox(2)(row).value should matchPattern {
      case Success(SpatialBox("45", "34", "23", "12", None)) =>
    }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_SOUTH" -> "",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialBox(2)(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SCHEME" -> "RD"
    )

    spatialBox(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_SPATIAL_NORTH, DCX_SPATIAL_EAST]", _)) =>
    }
  }
}
