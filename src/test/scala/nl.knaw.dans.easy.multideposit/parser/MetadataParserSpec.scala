/*
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
      val row = DepositRow(2, Map(Headers.Language -> lang))
      parser.iso639_2Language(Headers.Language)(row).value.value shouldBe lang
    }
  }

  def invalidLanguage3Tag(parser: MetadataParser, lang: String): Unit = {
    it should "fail when the language tag is invalid" in {
      val row = DepositRow(2, Map(Headers.Language -> lang))
      parser.iso639_2Language(Headers.Language)(row).value.invalidValue shouldBe
        ParseError(2, s"Value '$lang' is not a valid value for DC_LANGUAGE").chained
    }
  }
}

trait MetadataTestObjects {

  val testUriString = "http://does.not.exist.dans.knaw.nl/"
  val testUri = new URI(testUriString)

  lazy val metadataCSV @ metadataCSVRow1 :: metadataCSVRow2 :: Nil = List(
    Map(
      Headers.AlternativeTitle -> "alt1",
      Headers.Publisher -> "pub1",
      Headers.Type -> "Collection",
      Headers.Format -> "format1",
      // identifier
      Headers.Identifier -> "123456",
      Headers.IdentifierType -> "ARCHIS-ZAAK-IDENTIFICATIE",
      Headers.Source -> "src1",
      Headers.Language -> "dut",
      Headers.SchemeSpatial -> "",
      Headers.Spatial -> "spat1",
      Headers.Rightsholder -> "right1",
      // relation
      Headers.RelationQualifier -> "replaces",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> "bar",
      // date
      Headers.Date -> "2016-02-01",
      Headers.DateQualifier -> "Date Submitted",
      // contributor
      Headers.ContributorInitials -> "A.",
      Headers.ContributorSurname -> "Jones",
      Headers.ContributorRole -> "RelatedPerson",
      // subject
      Headers.Subject -> "IX",
      Headers.SubjectScheme -> "abr:ABRcomplex",
      // spatialPoint
      Headers.SpatialX -> "12",
      Headers.SpatialY -> "34",
      Headers.SpatialScheme -> "degrees",
      // temporal
      Headers.Temporal -> "PALEOLB",
      Headers.TemporalScheme -> "abr:ABRperiode",
      // user license
      Headers.License -> "http://creativecommons.org/licenses/by/4.0",
    ),
    Map(
      Headers.AlternativeTitle -> "alt2",
      Headers.Publisher -> "pub2",
      Headers.Type -> "MovingImage",
      Headers.Format -> "format2",
      Headers.Identifier -> "id",
      Headers.Source -> "src2",
      Headers.Language -> "nld",
      Headers.SchemeSpatial -> "dcterms:ISO3166",
      Headers.Spatial -> "GBR",
      Headers.Rightsholder -> "right2",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> "bar",
      Headers.Date -> "some random text",
      // spatialBox
      Headers.SpatialWest -> "12",
      Headers.SpatialEast -> "23",
      Headers.SpatialSouth -> "34",
      Headers.SpatialNorth -> "45",
      Headers.SpatialScheme -> "RD",
      // user license (repetition from the one above, but the same value, to test that only one is picked)
      Headers.License -> "http://creativecommons.org/licenses/by/4.0",
    )
  )

  lazy val metadataCSVRow = List(
    DepositRow(2, metadataCSVRow1),
    DepositRow(3, metadataCSVRow2),
  )

  lazy val metadata: Metadata = Metadata(
    alternatives = List("alt1", "alt2"),
    publishers = List("pub1", "pub2"),
    types = NonEmptyList.of(DcType.COLLECTION, DcType.MOVINGIMAGE),
    formats = List("format1", "format2"),
    identifiers = List(Identifier("123456", Some(IdentifierType.ARCHIS_ZAAK_IDENTIFICATIE)), Identifier("id")),
    sources = List("src1", "src2"),
    languages = List("dut", "nld"),
    spatials = List(Spatial("spat1"), Spatial("GBR", Some(SpatialScheme.ISO3166))),
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
      DepositRow(2, metadataCSVRow1 - Headers.Type),
      DepositRow(3, metadataCSVRow2 - Headers.Type),
    )

    inside(extractMetadata(2, metadataCSVRow)) {
      case Valid(md) => md.types.toList should contain only DcType.DATASET
    }
  }

  "dcType" should "convert the value for DC_TYPE into the corresponding enum object" in {
    val row = DepositRow(2, Map(Headers.Type -> "Collection"))

    dcType(row).value.value shouldBe DcType.COLLECTION
  }

  it should "return None if DC_TYPE is not defined" in {
    val row = DepositRow(2, Map(Headers.Type -> ""))

    dcType(row) shouldBe empty
  }

  it should "fail if the DC_TYPE value does not correspond to an object in the enum" in {
    val row = DepositRow(2, Map(Headers.Type -> "unknown value"))

    dcType(row).value.invalidValue shouldBe
      ParseError(2, "Value 'unknown value' is not a valid type").chained
  }

  "identifier" should "return None if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are not defined" in {
    val row = DepositRow(2, Map(
      Headers.Identifier -> "",
      Headers.IdentifierType -> ""
    ))

    identifier(row) shouldBe empty
  }

  it should "fail if DC_IDENTIFIER_TYPE is defined, but DC_IDENTIFIER is not" in {
    val row = DepositRow(2, Map(
      Headers.Identifier -> "",
      Headers.IdentifierType -> "ISSN"
    ))

    identifier(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DC_IDENTIFIER").chained
  }

  it should "succeed if DC_IDENTIFIER is defined and DC_IDENTIFIER_TYPE is not" in {
    val row = DepositRow(2, Map(
      Headers.Identifier -> "id",
      Headers.IdentifierType -> ""
    ))

    identifier(row).value.value shouldBe Identifier("id", None)
  }

  it should "succeed if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined and DC_IDENTIFIER_TYPE is valid" in {
    val row = DepositRow(2, Map(
      Headers.Identifier -> "123456",
      Headers.IdentifierType -> "ISSN"
    ))

    identifier(row).value.value shouldBe Identifier("123456", Some(IdentifierType.ISSN))
  }

  it should "fail if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined, but DC_IDENTIFIER_TYPE is invalid" in {
    val row = DepositRow(2, Map(
      Headers.Identifier -> "123456",
      Headers.IdentifierType -> "INVALID_IDENTIFIER_TYPE"
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

  "spatial" should "succeed if spatial scheme is empty and spatial does not follow any standard" in {
    val row = DepositRow(2, Map(
      Headers.SchemeSpatial -> "",
      Headers.Spatial -> "bar",
    ))

    spatial(row).value.value shouldBe Spatial("bar", None)
  }

  it should "succeed if spatial scheme is ISO3166 and spatial is country code following iso3166 scheme" in {
    val row = DepositRow(2, Map(
      Headers.SchemeSpatial -> "dcterms:ISO3166",
      Headers.Spatial -> "GBR",
    ))

    spatial(row).value.value shouldBe Spatial("GBR", Some(SpatialScheme.ISO3166))
  }

  it should "fail when scheme is given and it is not ISO3166" in {
    val row = DepositRow(2, Map(
      Headers.SchemeSpatial -> "dcterms:ISO3177",
      Headers.Spatial -> "GBR",
    ))

    spatial(row).value.invalidValue shouldBe
      ParseError(2, "Value 'dcterms:ISO3177' is not a valid scheme").chained
  }

  it should "fail when scheme is ISO3166 and spatial is not a country code following ISO3166 scheme" in {
    val row = DepositRow(2, Map(
      Headers.SchemeSpatial -> "dcterms:ISO3166",
      Headers.Spatial -> "XYZ",
    ))

    spatial(row).value.invalidValue shouldBe
      ParseError(2, "Value 'XYZ' is not a valid value for country code").chained
  }

  "relation" should "succeed if both the link and title are defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.value shouldBe UnqualifiedRelation(Some(testUri), Some("bar"))
  }

  it should "succeed if the qualifier and both the link and title are defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "replaces",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.value shouldBe QualifiedRelation(RelationQualifier.Replaces, Some(testUri), Some("bar"))
  }

  it should "fail when only the qualifier and link are defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "replaces",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> ""
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context").chained
  }

  it should "succeed when only the qualifier and title are defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "replaces",
      Headers.RelationLink -> "",
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.value shouldBe QualifiedRelation(RelationQualifier.Replaces, None, Some("bar"))
  }

  it should "fail if only the qualifier is defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "replaces",
      Headers.RelationLink -> "",
      Headers.RelationTitle -> ""
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well").chained
  }

  it should "fail if an invalid qualifier is given" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "invalid",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "Value 'invalid' is not a valid relation qualifier").chained
  }

  it should "fail if an invalid link is given" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "replaces",
      Headers.RelationLink -> "invalid uri",
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "DCX_RELATION_LINK value 'invalid uri' is not a valid URI").chained
  }

  it should "succeed if the qualifier is formatted differently" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "rEplAcEs",
      Headers.RelationLink -> testUriString,
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.value shouldBe QualifiedRelation(RelationQualifier.Replaces, Some(testUri), Some("bar"))
  }

  it should "fail if only the link is defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "",
      Headers.RelationLink -> "foo",
      Headers.RelationTitle -> ""
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context").chained
  }

  it should "fail if no qualifier is given and an invalid link is given" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "",
      Headers.RelationLink -> "invalid uri",
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.invalidValue shouldBe
      ParseError(2, "DCX_RELATION_LINK value 'invalid uri' is not a valid URI").chained
  }

  it should "succeed if only the title is defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "",
      Headers.RelationLink -> "",
      Headers.RelationTitle -> "bar"
    ))

    relation(row).value.value shouldBe UnqualifiedRelation(None, Some("bar"))
  }

  it should "return None if none of these fields are defined" in {
    val row = DepositRow(2, Map(
      Headers.RelationQualifier -> "",
      Headers.RelationLink -> "",
      Headers.RelationTitle -> ""
    ))

    relation(row) shouldBe empty
  }

  "date" should "convert a textual date into the corresponding object" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "random text",
      Headers.DateQualifier -> ""
    ))

    dateColumn(row).value.value shouldBe TextualDate("random text")
  }

  it should "fail if it has a correct qualifier but no well formatted date" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "random text",
      Headers.DateQualifier -> "Valid"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE value 'random text' does not represent a date").chained
  }

  it should "convert a date with qualifier into the corresponding object" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> "Issued"
    ))

    dateColumn(row).value.value shouldBe QualifiedDate(new DateTime(2016, 7, 30, 0, 0), DateQualifier.ISSUED)
  }

  it should "succeed when the qualifier is formatted differently (capitals)" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> "dateAccepted"
    ))

    dateColumn(row).value.value shouldBe QualifiedDate(new DateTime(2016, 7, 30, 0, 0), DateQualifier.DATE_ACCEPTED)
  }

  it should "succeed when the qualifier contains spaces instead of CamelCase" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> "date accepted"
    ))

    dateColumn(row).value.value shouldBe QualifiedDate(new DateTime(2016, 7, 30, 0, 0), DateQualifier.DATE_ACCEPTED)
  }

  it should "succeed if the qualifier isn't given, but the date is (properly formatted)" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> ""
    ))

    dateColumn(row).value.value shouldBe TextualDate("2016-07-30")
  }

  it should "fail if the qualifier is unknown" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> "unknown"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "Value 'unknown' is not a valid date qualifier").chained
  }

  it should "fail if the date isn't properly formatted" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "30-07-2016",
      Headers.DateQualifier -> "Issued"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE value '30-07-2016' does not represent a date").chained
  }

  it should "fail if no date is given, but a valid qualifier is given" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "",
      Headers.DateQualifier -> "Issued"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it").chained
  }

  it should "fail if the qualifier is equal to 'available' since we use a different keyword for that" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> "Available"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE_QUALIFIER value 'Available' is not allowed here. Use column DDM_AVAILABLE instead.").chained
  }

  it should "fail if the qualifier is equal to 'created' since we use a different keyword for that" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "2016-07-30",
      Headers.DateQualifier -> "Created"
    ))

    dateColumn(row).value.invalidValue shouldBe
      ParseError(2, "DCT_DATE_QUALIFIER value 'Created' is not allowed here. Use column DDM_CREATED instead.").chained
  }

  it should "return an empty value if both values are empty" in {
    val row = DepositRow(2, Map(
      Headers.Date -> "",
      Headers.DateQualifier -> ""
    ))

    dateColumn(row) shouldBe empty
  }

  "contributor" should "return None if the none of the fields are defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "",
      Headers.ContributorInitials -> "",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "",
      Headers.ContributorOrganization -> "",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> ""
    ))

    contributor(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION is defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "",
      Headers.ContributorInitials -> "",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "",
      Headers.ContributorOrganization -> "org",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> ""
    ))

    contributor(row).value.value shouldBe ContributorOrganization("org", None)
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION and DCX_CONTRIBUTOR_ROLE are defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "",
      Headers.ContributorInitials -> "",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "",
      Headers.ContributorOrganization -> "org",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> "RelatedPERSON"
    ))

    contributor(row).value.value shouldBe ContributorOrganization("org", Some(ContributorRole.RELATED_PERSON))
  }

  it should "succeed with a person when only DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "",
      Headers.ContributorInitials -> "A.",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "Jones",
      Headers.ContributorOrganization -> "",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> ""
    ))

    contributor(row).value.value shouldBe ContributorPerson(None, "A.", None, "Jones", None, None, None)
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "Dr.",
      Headers.ContributorInitials -> "A.",
      Headers.ContributorInsertions -> "X",
      Headers.ContributorSurname -> "Jones",
      Headers.ContributorOrganization -> "org",
      Headers.ContributorDAI -> "dai123",
      Headers.ContributorRole -> "related person"
    ))

    contributor(row).value.value shouldBe ContributorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some(ContributorRole.RELATED_PERSON), Some("dai123"))
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS is not defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "Dr.",
      Headers.ContributorInitials -> "",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "Jones",
      Headers.ContributorOrganization -> "",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> ""
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DCX_CONTRIBUTOR_INITIALS").chained
  }

  it should "fail if DCX_CONTRIBUTOR_SURNAME is not defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "Dr.",
      Headers.ContributorInitials -> "A.",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "",
      Headers.ContributorOrganization -> "",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> ""
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DCX_CONTRIBUTOR_SURNAME").chained
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are both not defined" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "Dr.",
      Headers.ContributorInitials -> "",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "",
      Headers.ContributorOrganization -> "",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> ""
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Missing value(s) for: [DCX_CONTRIBUTOR_INITIALS, DCX_CONTRIBUTOR_SURNAME]").chained
  }

  it should "fail if DCX_CREATOR_ROLE has an invalid value" in {
    val row = DepositRow(2, Map(
      Headers.ContributorTitles -> "Dr.",
      Headers.ContributorInitials -> "A.",
      Headers.ContributorInsertions -> "",
      Headers.ContributorSurname -> "Jones",
      Headers.ContributorOrganization -> "",
      Headers.ContributorDAI -> "",
      Headers.ContributorRole -> "invalid!"
    ))

    contributor(row).value.invalidValue shouldBe
      ParseError(2, "Value 'invalid!' is not a valid contributor role").chained
  }

  "subject" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      Headers.Subject -> "IX",
      Headers.SubjectScheme -> "abr:ABRcomplex"
    ))

    subject(row).value.value shouldBe Subject("IX", Some("abr:ABRcomplex"))
  }

  it should "succeed when the scheme is not defined" in {
    val row = DepositRow(2, Map(
      Headers.Subject -> "test",
      Headers.SubjectScheme -> ""
    ))

    subject(row).value.value shouldBe Subject("test", None)
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = DepositRow(2, Map(
      Headers.Subject -> "",
      Headers.SubjectScheme -> "abr:ABRcomplex"
    ))

    subject(row).value.value shouldBe Subject("", Some("abr:ABRcomplex"))
  }

  it should "fail if the scheme is not recognized" in {
    val row = DepositRow(2, Map(
      Headers.Subject -> "IX",
      Headers.SubjectScheme -> "random-incorrect-scheme"
    ))

    subject(row).value.invalidValue shouldBe
      ParseError(2, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'").chained
  }

  it should "return None when both fields are empty or blank" in {
    val row = DepositRow(2, Map(
      Headers.Subject -> "",
      Headers.SubjectScheme -> ""
    ))

    subject(row) shouldBe empty
  }

  "spatialPoint" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      Headers.SpatialX -> "12",
      Headers.SpatialY -> "34",
      Headers.SpatialScheme -> "degrees"
    ))

    spatialPoint(row).value.value shouldBe SpatialPoint("12", "34", Some("degrees"))
  }

  it should "succeed when no scheme is defined" in {
    val row = DepositRow(2, Map(
      Headers.SpatialX -> "12",
      Headers.SpatialY -> "34",
      Headers.SpatialScheme -> ""
    ))

    spatialPoint(row).value.value shouldBe SpatialPoint("12", "34", None)
  }

  it should "return None if there is no value for any of these keys" in {
    val row = DepositRow(2, Map(
      Headers.SpatialX -> "",
      Headers.SpatialY -> "",
      Headers.SpatialScheme -> ""
    ))

    spatialPoint(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = DepositRow(2, Map(
      Headers.SpatialX -> "12",
      Headers.SpatialY -> "",
      Headers.SpatialScheme -> "degrees"
    ))

    spatialPoint(row).value.invalidValue shouldBe
      ParseError(2, "Missing value for: DCX_SPATIAL_Y").chained
  }

  "spatialBox" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      Headers.SpatialWest -> "12",
      Headers.SpatialEast -> "23",
      Headers.SpatialSouth -> "34",
      Headers.SpatialNorth -> "45",
      Headers.SpatialScheme -> "RD"
    ))

    spatialBox(row).value.value shouldBe SpatialBox("45", "34", "23", "12", Some("RD"))
  }

  it should "succeed when no scheme is defined" in {
    val row = DepositRow(2, Map(
      Headers.SpatialWest -> "12",
      Headers.SpatialEast -> "23",
      Headers.SpatialSouth -> "34",
      Headers.SpatialNorth -> "45",
      Headers.SpatialScheme -> ""
    ))

    spatialBox(row).value.value shouldBe SpatialBox("45", "34", "23", "12", None)
  }

  it should "return None if there is no value for any of these keys" in {
    val row = DepositRow(2, Map(
      Headers.SpatialWest -> "",
      Headers.SpatialEast -> "",
      Headers.SpatialSouth -> "",
      Headers.SpatialNorth -> "",
      Headers.SpatialScheme -> ""
    ))

    spatialBox(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = DepositRow(2, Map(
      Headers.SpatialWest -> "12",
      Headers.SpatialEast -> "",
      Headers.SpatialSouth -> "34",
      Headers.SpatialNorth -> "",
      Headers.SpatialScheme -> "RD"
    ))

    spatialBox(row).value.invalidValue shouldBe
      ParseError(2, "Missing value(s) for: [DCX_SPATIAL_NORTH, DCX_SPATIAL_EAST]").chained
  }

  "temporal" should "convert the csv input into the corresponding object" in {
    val row = DepositRow(2, Map(
      Headers.Temporal -> "PALEOLB",
      Headers.TemporalScheme -> "abr:ABRperiode"
    ))

    temporal(row).value.value shouldBe Temporal("PALEOLB", Some("abr:ABRperiode"))
  }

  it should "succeed when the scheme is not defined" in {
    val row = DepositRow(2, Map(
      Headers.Temporal -> "test",
      Headers.TemporalScheme -> ""
    ))

    temporal(row).value.value shouldBe Temporal("test", None)
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = DepositRow(2, Map(
      Headers.Temporal -> "",
      Headers.TemporalScheme -> "abr:ABRperiode"
    ))

    temporal(row).value.value shouldBe Temporal("", Some("abr:ABRperiode"))
  }

  it should "fail if the scheme is not recognized" in {
    val row = DepositRow(2, Map(
      Headers.Temporal -> "PALEOLB",
      Headers.TemporalScheme -> "random-incorrect-scheme"
    ))

    temporal(row).value.invalidValue shouldBe
      ParseError(2, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'").chained
  }

  it should "return None when both fields are empty or blank" in {
    val row = DepositRow(2, Map(
      Headers.Temporal -> "",
      Headers.TemporalScheme -> ""
    ))

    temporal(row) shouldBe empty
  }

  "userLicense" should "return a user license object when the given license is allowed" in {
    val license = "http://www.mozilla.org/en-US/MPL/2.0/FAQ/"

    userLicense(2, Headers.License)(license).value shouldBe UserLicense(license)
  }

  it should "fail when the given license is unknown" in {
    val license = "unknown"
    val expectedErrorMsg = s"User license '$license' is not allowed in column DCT_LICENSE."

    userLicense(2, Headers.License)(license).invalidValue shouldBe ParseError(2, expectedErrorMsg).chained
  }
}
