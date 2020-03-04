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

import java.net.URI

import better.files.File
import cats.data.NonEmptyList
import cats.data.Validated.Valid
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model._
import org.joda.time.DateTime

trait LanguageBehavior {
  this: TestSupportFixture =>
  def validLanguage3Tag(parser: MetadataParser, lang: String): Unit = {
    it should "succeed when the language tag is valid" in {
      val row = DepositRow(2, Map("taal" -> lang))
      parser.iso639_2Language("taal")(row).value.value shouldBe lang
    }
  }

  def invalidLanguage3Tag(parser: MetadataParser, lang: String): Unit = {
    it should "fail when the language tag is invalid" in {
      val row = DepositRow(2, Map("taal" -> lang))
      parser.iso639_2Language("taal")(row).value.invalidValue shouldBe
        ParseError(2, s"Value '$lang' is not a valid value for taal").chained
    }
  }
}

trait MetadataTestObjects {

  val testUriString = "http://does.not.exist.dans.knaw.nl/"
  val testUri = new URI(testUriString)

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
      "DCX_RELATION_LINK" -> testUriString,
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
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode",
      // user license
      "DCT_LICENSE" -> "http://creativecommons.org/licenses/by/4.0",
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
      "DCX_RELATION_LINK" -> testUriString,
      "DCX_RELATION_TITLE" -> "bar",
      "DCT_DATE" -> "some random text",
      // spatialBox
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> "RD",
      // user license (repetition from the one above, but the same value, to test that only one is picked)
      "DCT_LICENSE" -> "http://creativecommons.org/licenses/by/4.0",
    )
  )

  lazy val metadataCSVRow = List(
    DepositRow(2, metadataCSVRow1),
    DepositRow(3, metadataCSVRow2),
  )

  lazy val metadata = Metadata(
    alternatives = List("alt1", "alt2"),
    publishers = List("pub1", "pub2"),
    types = NonEmptyList.of(DcType.COLLECTION, DcType.MOVINGIMAGE),
    formats = List("format1", "format2"),
    identifiers = List(Identifier("123456", Some(IdentifierType.ARCHIS_ZAAK_IDENTIFICATIE)), Identifier("id")),
    sources = List("src1", "src2"),
    languages = List("dut", "nld"),
    spatials = List("spat1", "spat2"),
    rightsholder = NonEmptyList.of("right1", "right2"),
    relations = List(QualifiedRelation(RelationQualifier.Replaces, link = Some(testUri), title = Some("bar")), UnqualifiedRelation(link = Some(testUri), title = Some("bar"))),
    dates = List(QualifiedDate(new DateTime(2016, 2, 1, 0, 0), DateQualifier.DATE_SUBMITTED), TextualDate("some random text")),
    contributors = List(ContributorPerson(initials = "A.", surname = "Jones", role = Some(ContributorRole.RELATED_PERSON))),
    subjects = List(Subject("IX", Option("abr:ABRcomplex"))),
    spatialPoints = List(SpatialPoint("12", "34", Option("degrees"))),
    spatialBoxes = List(SpatialBox("45", "34", "23", "12", Option("RD"))),
    temporal = List(Temporal("PALEOLB", Option("abr:ABRperiode"))),
    userLicense = Option(UserLicense("http://creativecommons.org/licenses/by/4.0"))
  )
}

class MetadataParserSpec extends TestSupportFixture with MetadataTestObjects with LanguageBehavior {
  self =>

  private val parser = new MetadataParser with ParserUtils with InputPathExplorer {
    override val multiDepositDir: File = self.multiDepositDir
    override val userLicenses: Set[String] = self.userLicenses
  }

  import parser._

  "extractMetadata" should "convert the csv input to the corresponding output" in {
    extractMetadata(2, metadataCSVRow).value shouldBe metadata
  }

  it should "use the default type value if no value for DC_TYPE is specified" in {
    val metadataCSVRow = List(
      DepositRow(2, metadataCSVRow1 - "DC_TYPE"),
      DepositRow(3, metadataCSVRow2 - "DC_TYPE"),
    )

    inside(extractMetadata(2, metadataCSVRow)) {
      case Valid(md) => md.types.toList should contain only DcType.DATASET
    }
  }

  "dcType" should "convert the value for DC_TYPE into the corresponding enum object" in {
    val row = DepositRow(2, Map("DC_TYPE" -> "Collection"))

    dcType(row).value.value shouldBe DcType.COLLECTION
  }

  it should "return None if DC_TYPE is not defined" in {
    val row = DepositRow(2, Map("DC_TYPE" -> ""))

    dcType(row) shouldBe empty
  }

  it should "fail if the DC_TYPE value does not correspond to an object in the enum" in {
    val row = DepositRow(2, Map("DC_TYPE" -> "unknown value"))

    dcType(row).value.invalidValue shouldBe
      ParseError(2, "Value 'unknown value' is not a valid type").chained
  }

  "identifier" should "return None if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are not defined" in {
    val row = DepositRow(2, Map(
      "DC_IDENTIFIER" -> "",
      "DC_IDENTIFIER_TYPE" -> ""
    ))

    identifier(row) shouldBe empty
  }

  it should "fail if DC_IDENTIFIER_TYPE is defined, but DC_IDENTIFIER is not" in {
    val row = DepositRow(2, Map(
      "DC_IDENTIFIER" -> "",
      "DC_IDENTIFIER_TYPE" -> "ISSN"
    ))

    identifier(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DC_IDENTIFIER").chained
  }

  it should "succeed if DC_IDENTIFIER is defined and DC_IDENTIFIER_TYPE is not" in {
    val row = DepositRow(2, Map(
      "DC_IDENTIFIER" -> "id",
      "DC_IDENTIFIER_TYPE" -> ""
    ))

    identifier(row).value.value shouldBe Identifier("id", None)
  }

  it should "succeed if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined and DC_IDENTIFIER_TYPE is valid" in {
    val row = DepositRow(2, Map(
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "ISSN"
    ))

    identifier(row).value.value shouldBe Identifier("123456", Some(IdentifierType.ISSN))
  }

  it should "fail if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined, but DC_IDENTIFIER_TYPE is invalid" in {
    val row = DepositRow(2, Map(
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "INVALID_IDENTIFIER_TYPE"
    ))

    identifier(row).value.invalidValue shouldBe
      ParseError(2, "Value 'INVALID_IDENTIFIER_TYPE' is not a valid identifier type").chained
  }

  "iso639_2Language (with normal 3-letter tag)" should behave like validLanguage3Tag(parser, "eng")

  "iso639_2Language (with terminology tag)" should behave like validLanguage3Tag(parser, "nld")

  "iso639_2Language (with bibliographic tag)" should behave like validLanguage3Tag(parser, "dut")

  "iso639_2Language (with a random tag)" should behave like invalidLanguage3Tag(parser, "abc")

  "iso639_2Language (with some obscure language tag no one has ever heard about)" should behave like validLanguage3Tag(parser, "day")

  "iso639_2Language (with a 2-letter tag)" should behave like invalidLanguage3Tag(parser, "nl")

  "iso639_2Language (with a too short tag)" should behave like invalidLanguage3Tag(parser, "a")

  "iso639_2Language (with a too long tag)" should behave like invalidLanguage3Tag(parser, "abcdef")

  "iso639_2Language (with encoding tag)" should behave like invalidLanguage3Tag(parser, "encoding=UTF-8")

  "relation" should "succeed if both the link and title are defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> testUriString,
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.value shouldBe UnqualifiedRelation(Some(testUri), Some("bar"))
  }

  it should "succeed if the qualifier and both the link and title are defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> testUriString,
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.value shouldBe QualifiedRelation(RelationQualifier.Replaces, Some(testUri), Some("bar"))
  }

  it should "fail when only the qualifier and link are defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> testUriString,
      "DCX_RELATION_TITLE" -> ""
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context").chained
  }

  it should "succeed when only the qualifier and title are defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.value shouldBe QualifiedRelation(RelationQualifier.Replaces, None, Some("bar"))
  }

  it should "fail if only the qualifier is defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> ""
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well").chained
  }

  it should "fail if an invalid qualifier is given" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "invalid",
      "DCX_RELATION_LINK" -> testUriString,
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "Value 'invalid' is not a valid relation qualifier").chained
  }

  it should "fail if an invalid link is given" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "invalid uri",
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "DCX_RELATION_LINK value 'invalid uri' is not a valid URI").chained
  }

  it should "succeed if the qualifier is formatted differently" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "rEplAcEs",
      "DCX_RELATION_LINK" -> testUriString,
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.value shouldBe QualifiedRelation(RelationQualifier.Replaces, Some(testUri), Some("bar"))
  }

  it should "fail if only the link is defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> ""
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context").chained
  }

  it should "fail if no qualifier is given and an invalid link is given" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "invalid uri",
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "DCX_RELATION_LINK value 'invalid uri' is not a valid URI").chained
  }

  it should "succeed if only the title is defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> "bar"
    ))

    relation(row).value.value shouldBe UnqualifiedRelation(None, Some("bar"))
  }

  it should "return None if none of these fields are defined" in {
    val row = DepositRow(2, Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> ""
    ))

    relation(row) shouldBe empty
  }

  "date" should "convert a textual date into the corresponding object" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "random text",
      "DCT_DATE_QUALIFIER" -> ""
    ))

    dateColumn(row).value.value shouldBe TextualDate("random text")
  }

  it should "fail if it has a correct qualifier but no well formatted date" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "random text",
      "DCT_DATE_QUALIFIER" -> "Valid"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE value 'random text' does not represent a date").chained
  }

  it should "convert a date with qualifier into the corresponding object" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "Issued"
    ))

    dateColumn(row).value.value shouldBe QualifiedDate(new DateTime(2016, 7, 30, 0, 0), DateQualifier.ISSUED)
  }

  it should "succeed when the qualifier is formatted differently (capitals)" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "dateAccepted"
    ))

    dateColumn(row).value.value shouldBe QualifiedDate(new DateTime(2016, 7, 30, 0, 0), DateQualifier.DATE_ACCEPTED)
  }

  it should "succeed when the qualifier contains spaces instead of CamelCase" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "date accepted"
    ))

    dateColumn(row).value.value shouldBe QualifiedDate(new DateTime(2016, 7, 30, 0, 0), DateQualifier.DATE_ACCEPTED)
  }

  it should "succeed if the qualifier isn't given, but the date is (properly formatted)" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> ""
    ))

    dateColumn(row).value.value shouldBe TextualDate("2016-07-30")
  }

  it should "fail if the qualifier is unknown" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "unknown"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "Value 'unknown' is not a valid date qualifier").chained
  }

  it should "fail if the date isn't properly formatted" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "30-07-2016",
      "DCT_DATE_QUALIFIER" -> "Issued"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE value '30-07-2016' does not represent a date").chained
  }

  it should "fail if no date is given, but a valid qualifier is given" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "",
      "DCT_DATE_QUALIFIER" -> "Issued"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it").chained
  }

  it should "fail if the qualifier is equal to 'available' since we use a different keyword for that" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "Available"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE_QUALIFIER value 'Available' is not allowed here. Use column DDM_AVAILABLE instead.").chained
  }

  it should "fail if the qualifier is equal to 'created' since we use a different keyword for that" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "2016-07-30",
      "DCT_DATE_QUALIFIER" -> "Created"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE_QUALIFIER value 'Created' is not allowed here. Use column DDM_CREATED instead.").chained
  }

  it should "return an empty value if both values are empty" in {
    val row = DepositRow(2, Map(
      "DCT_DATE" -> "",
      "DCT_DATE_QUALIFIER" -> ""
    ))

    dateColumn(row) shouldBe empty
  }

  "contributor" should "return None if the none of the fields are defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    ))

    contributor(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION is defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    ))

    contributor(row).value.value shouldBe ContributorOrganization("org", None)
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION and DCX_CONTRIBUTOR_ROLE are defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> "RelatedPERSON"
    ))

    contributor(row).value.value shouldBe ContributorOrganization("org", Some(ContributorRole.RELATED_PERSON))
  }

  it should "succeed with a person when only DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    ))

    contributor(row).value.value shouldBe ContributorPerson(None, "A.", None, "Jones", None, None, None)
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "X",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "dai123",
      "DCX_CONTRIBUTOR_ROLE" -> "related person"
    ))

    contributor(row).value.value shouldBe ContributorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some(ContributorRole.RELATED_PERSON), Some("dai123"))
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS is not defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DCX_CONTRIBUTOR_INITIALS").chained
  }

  it should "fail if DCX_CONTRIBUTOR_SURNAME is not defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DCX_CONTRIBUTOR_SURNAME").chained
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are both not defined" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> ""
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Missing value(s) for: [DCX_CONTRIBUTOR_SURNAME, DCX_CONTRIBUTOR_INITIALS]").chained
  }

  it should "fail if DCX_CREATOR_ROLE has an invalid value" in {
    val row = DepositRow(2, Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> "",
      "DCX_CONTRIBUTOR_ROLE" -> "invalid!"
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Value 'invalid!' is not a valid contributor role").chained
  }

  "subject" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex"
    ))

    subject(row).value.value shouldBe Subject("IX", Some("abr:ABRcomplex"))
  }

  it should "succeed when the scheme is not defined" in {
    val row = DepositRow(2, Map(
      "DC_SUBJECT" -> "test",
      "DC_SUBJECT_SCHEME" -> ""
    ))

    subject(row).value.value shouldBe Subject("test", None)
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = DepositRow(2, Map(
      "DC_SUBJECT" -> "",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex"
    ))

    subject(row).value.value shouldBe Subject("", Some("abr:ABRcomplex"))
  }

  it should "fail if the scheme is not recognized" in {
    val row = DepositRow(2, Map(
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "random-incorrect-scheme"
    ))

    subject(row).value.invalidValue shouldBe
      ParseError(2, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'").chained
  }

  it should "return None when both fields are empty or blank" in {
    val row = DepositRow(2, Map(
      "DC_SUBJECT" -> "",
      "DC_SUBJECT_SCHEME" -> ""
    ))

    subject(row) shouldBe empty
  }

  "spatialPoint" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> "degrees"
    ))

    spatialPoint(row).value.value shouldBe SpatialPoint("12", "34", Some("degrees"))
  }

  it should "succeed when no scheme is defined" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> ""
    ))

    spatialPoint(row).value.value shouldBe SpatialPoint("12", "34", None)
  }

  it should "return None if there is no value for any of these keys" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_X" -> "",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_SCHEME" -> ""
    ))

    spatialPoint(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_SCHEME" -> "degrees"
    ))

    spatialPoint(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DCX_SPATIAL_Y").chained
  }

  "spatialBox" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> "RD"
    ))

    spatialBox(row).value.value shouldBe SpatialBox("45", "34", "23", "12", Some("RD"))
  }

  it should "succeed when no scheme is defined" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> ""
    ))

    spatialBox(row).value.value shouldBe SpatialBox("45", "34", "23", "12", None)
  }

  it should "return None if there is no value for any of these keys" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_WEST" -> "",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_SOUTH" -> "",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SCHEME" -> ""
    ))

    spatialBox(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = DepositRow(2, Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SCHEME" -> "RD"
    ))

    spatialBox(row).value.invalidValue shouldBe
      ParseError(2, "Missing value(s) for: [DCX_SPATIAL_NORTH, DCX_SPATIAL_EAST]").chained
  }

  "temporal" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    ))

    temporal(row).value.value shouldBe Temporal("PALEOLB", Some("abr:ABRperiode"))
  }

  it should "succeed when the scheme is not defined" in {
    val row = DepositRow(2, Map(
      "DCT_TEMPORAL" -> "test",
      "DCT_TEMPORAL_SCHEME" -> ""
    ))

    temporal(row).value.value shouldBe Temporal("test", None)
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = DepositRow(2, Map(
      "DCT_TEMPORAL" -> "",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    ))

    temporal(row).value.value shouldBe Temporal("", Some("abr:ABRperiode"))
  }

  it should "fail if the scheme is not recognized" in {
    val row = DepositRow(2, Map(
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "random-incorrect-scheme"
    ))

    temporal(row).value.invalidValue shouldBe
      ParseError(2, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'").chained
  }

  it should "return None when both fields are empty or blank" in {
    val row = DepositRow(2, Map(
      "DCT_TEMPORAL" -> "",
      "DCT_TEMPORAL_SCHEME" -> ""
    ))

    temporal(row) shouldBe empty
  }

  "userLicense" should "return a user license object when the given license is allowed" in {
    val license = "http://www.mozilla.org/en-US/MPL/2.0/FAQ/"

    userLicense(2, "DCT_LICENSE")(license).value shouldBe UserLicense(license)
  }

  it should "fail when the given license is unknown" in {
    val license = "unknown"
    val expectedErrorMsg = s"User license '$license' is not allowed."

    userLicense(2, "DCT_LICENSE")(license).invalidValue shouldBe ParseError(2, expectedErrorMsg).chained
  }
}
