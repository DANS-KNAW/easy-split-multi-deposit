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

import better.files.File
import cats.data.Validated.{ Invalid, Valid }
import cats.data.{ Chain, NonEmptyList }
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model.{ ContributorRole, CreatorOrganization, CreatorPerson, MultiDepositKey, Profile, listToNEL }
import org.joda.time.DateTime

trait ProfileTestObjects {

  lazy val profileCSV @ profileCSVRow1 :: profileCSVRow2 :: Nil = List(
    Map(
      "DC_TITLE" -> "title1",
      "DC_DESCRIPTION" -> "descr1",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ROLE" -> "Supervisor",
      "DDM_CREATED" -> "2016-07-30",
      "DDM_AVAILABLE" -> "2016-07-31",
      "DDM_AUDIENCE" -> "D30000",
      "DDM_ACCESSRIGHTS" -> "GROUP_ACCESS"
    ),
    Map(
      "DC_TITLE" -> "title2",
      "DC_DESCRIPTION" -> "descr2",
      "DDM_AUDIENCE" -> "D37000"
    )
  )

  lazy val profileCSVRows = List(
    DepositRow(2, profileCSVRow1),
    DepositRow(3, profileCSVRow2),
  )

  lazy val profile = Profile(
    titles = List("title1", "title2"),
    descriptions = List("descr1", "descr2"),
    creators = List(CreatorPerson(initials = "A.", surname = "Jones", role = Option(ContributorRole.SUPERVISOR))),
    created = DateTime.parse("2016-07-30"),
    available = DateTime.parse("2016-07-31"),
    audiences = List("D30000", "D37000"),
    accessright = AccessCategory.GROUP_ACCESS
  )
}

class ProfileParserSpec extends TestSupportFixture with ProfileTestObjects {
  self =>

  private val parser = new ProfileParser with ParserUtils with InputPathExplorer {
    val multiDepositDir: File = self.multiDepositDir
  }

  import parser._

  "extractProfile" should "convert the csv input to the corresponding output" in {
    extractProfile(2, profileCSVRows) shouldBe Valid(profile)
  }

  it should "fail if there are no values for DC_TITLE, DC_DESCRIPTION, creator, DDM_CREATED, DDM_AUDIENCE and DDM_ACCESSRIGHTS" in {
    val rows = DepositRow(2, Map.empty[MultiDepositKey, String]) ::
      DepositRow(3, Map.empty[MultiDepositKey, String]) :: Nil

    inside(extractProfile(2, rows)) {
      case Invalid(chain) =>
        chain.toNonEmptyList shouldBe NonEmptyList.of(
          ParseError(2, "There should be at least one non-empty value for DC_TITLE"),
          ParseError(2, "There should be at least one non-empty value for DC_DESCRIPTION"),
          ParseError(2, "There should be at least one non-empty value for the creator fields"),
          ParseError(2, "There should be one non-empty value for DDM_CREATED"),
          ParseError(2, "There should be at least one non-empty value for DDM_AUDIENCE"),
          ParseError(2, "There should be one non-empty value for DDM_ACCESSRIGHTS"),
        )
    }
  }

  it should "fail if there are multiple values for DDM_CREATED, DDM_AVAILABLE and DDM_ACCESSRIGHTS" in {
    val rows = DepositRow(2, profileCSVRow1) ::
      DepositRow(3, profileCSVRow2.updated("DDM_CREATED", "2015-07-30")
        .updated("DDM_AVAILABLE", "2015-07-31")
        .updated("DDM_ACCESSRIGHTS", "NO_ACCESS")) :: Nil

    inside(extractProfile(2, rows)) {
      case Invalid(chain) =>
        chain.toNonEmptyList shouldBe NonEmptyList.of(
          ParseError(2, "Only one row is allowed to contain a value for the column 'DDM_CREATED'. Found: [2016-07-30, 2015-07-30]"),
          ParseError(2, "At most one row is allowed to contain a value for the column 'DDM_AVAILABLE'. Found: [2016-07-31, 2015-07-31]"),
          ParseError(2, "Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'. Found: [GROUP_ACCESS, NO_ACCESS]"),
        )
    }
  }

  it should "fail if DDM_ACCESSRIGHTS is GROUPACCESS and DDM_AUDIENCE does not contain D37000" in {
    val rows = DepositRow(2, profileCSVRow1) :: Nil

    extractProfile(2, rows) shouldBe Invalid(Chain(
      ParseError(2, "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeology)")
    ))
  }

  "accessCategory" should "convert the value for DDM_ACCESSRIGHTS into the corresponding enum object" in {
    accessCategory(2, "DDM_ACCESSRIGHTS")("ANONYMOUS_ACCESS").right.value shouldBe AccessCategory.ANONYMOUS_ACCESS
  }

  it should "fail if the DDM_ACCESSRIGHTS value does not correspond to an object in the enum" in {
    accessCategory(2, "DDM_ACCESSRIGHTS")("unknown value").left.value shouldBe
      ParseError(2, "Value 'unknown value' is not a valid accessright in column DDM_ACCESSRIGHTS")
  }

  "creator" should "return None if none of the fields are defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    ))

    creator(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION is defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    ))

    creator(row).value shouldBe Valid(CreatorOrganization("org", None))
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION and DCX_CREATOR_ROLE are defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> "ProjectManager"
    ))

    creator(row).value shouldBe
      Valid(CreatorOrganization("org", Some(ContributorRole.PROJECT_MANAGER)))
  }

  it should "succeed with a person when only DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    ))

    creator(row).value shouldBe
      Valid(CreatorPerson(None, "A.", None, "Jones", None, None, None))
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "X",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "dai123",
      "DCX_CREATOR_ROLE" -> "rElAtEdpErsOn"
    ))

    creator(row).value shouldBe
      Valid(CreatorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some(ContributorRole.RELATED_PERSON), Some("dai123")))
  }

  it should "fail if DCX_CREATOR_INITIALS is not defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    ))

    creator(row).value shouldBe
      Invalid(Chain(ParseError(2, "Missing value for: DCX_CREATOR_INITIALS")))
  }

  it should "fail if DCX_CREATOR_SURNAME is not defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    ))

    creator(row).value shouldBe
      Invalid(Chain(ParseError(2, "Missing value for: DCX_CREATOR_SURNAME")))
  }

  it should "fail if DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are both not defined" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    ))

    creator(row).value shouldBe
      Invalid(Chain(ParseError(2, "Missing value(s) for: [DCX_CREATOR_SURNAME, DCX_CREATOR_INITIALS]")))
  }

  it should "fail if DCX_CREATOR_ROLE has an invalid value" in {
    val row = DepositRow(2, Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> "invalid!"
    ))

    creator(row).value shouldBe
      Invalid(Chain(ParseError(2, "Value 'invalid!' is not a valid creator role")))
  }
}
