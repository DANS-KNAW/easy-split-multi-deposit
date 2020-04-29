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
import cats.data.NonEmptyList
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model.{ ContributorRole, CreatorOrganization, CreatorPerson, Profile }
import nl.knaw.dans.easy.multideposit.parser.Headers.Header
import org.joda.time.DateTime

trait ProfileTestObjects {

  lazy val profileCSV @ profileCSVRow1 :: profileCSVRow2 :: Nil = List(
    Map(
      Headers.Title -> "title1",
      Headers.Description -> "descr1",
      Headers.CreatorInitials -> "A.",
      Headers.CreatorSurname -> "Jones",
      Headers.CreatorRole -> "Supervisor",
      Headers.Created -> "2016-07-30",
      Headers.Available -> "2016-07-31",
      Headers.Audience -> "D30000",
      Headers.AccessRights -> "REQUEST_PERMISSION"
    ),
    Map(
      Headers.Title -> "title2",
      Headers.Description -> "descr2",
      Headers.Audience -> "D37000"
    )
  )

  lazy val profileCSVRows = List(
    DepositRow(2, profileCSVRow1),
    DepositRow(3, profileCSVRow2),
  )

  lazy val profile: Profile = Profile(
    titles = NonEmptyList.of("title1", "title2"),
    descriptions = NonEmptyList.of("descr1", "descr2"),
    creators = NonEmptyList.of(CreatorPerson(initials = "A.", surname = "Jones", role = Option(ContributorRole.SUPERVISOR))),
    created = DateTime.parse("2016-07-30"),
    available = DateTime.parse("2016-07-31"),
    audiences = NonEmptyList.of("D30000", "D37000"),
    accessright = AccessCategory.REQUEST_PERMISSION
  )
}

class ProfileParserSpec extends TestSupportFixture with ProfileTestObjects {
  self =>

  private val parser = new ProfileParser with ParserUtils with InputPathExplorer {
    val multiDepositDir: File = self.multiDepositDir
  }

  import parser._

  "extractProfile" should "convert the csv input to the corresponding output" in {
    extractProfile(2, profileCSVRows).value shouldBe profile
  }

  it should "fail if there are no values for DC_TITLE, DC_DESCRIPTION, creator, DDM_CREATED, DDM_AUDIENCE and DDM_ACCESSRIGHTS" in {
    val rows = DepositRow(2, Map.empty[Header, String]) ::
      DepositRow(3, Map.empty[Header, String]) :: Nil

    extractProfile(2, rows).invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "There should be at least one non-empty value for DC_TITLE"),
      ParseError(2, "There should be at least one non-empty value for DC_DESCRIPTION"),
      ParseError(2, "There should be at least one non-empty value for the creator fields"),
      ParseError(2, "There should be one non-empty value for DDM_CREATED"),
      ParseError(2, "There should be at least one non-empty value for DDM_AUDIENCE"),
      ParseError(2, "There should be one non-empty value for DDM_ACCESSRIGHTS"),
    )
  }

  it should "fail if there are multiple values for DDM_CREATED, DDM_AVAILABLE and DDM_ACCESSRIGHTS" in {
    val rows = DepositRow(2, profileCSVRow1) ::
      DepositRow(3, profileCSVRow2.updated(Headers.Created, "2015-07-30")
        .updated(Headers.Available, "2015-07-31")
        .updated(Headers.AccessRights, "NO_ACCESS")) :: Nil

    extractProfile(2, rows).invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "Only one row is allowed to contain a value for the column 'DDM_CREATED'. Found: [2016-07-30, 2015-07-30]"),
      ParseError(2, "At most one row is allowed to contain a value for the column 'DDM_AVAILABLE'. Found: [2016-07-31, 2015-07-31]"),
      ParseError(2, "Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'. Found: [REQUEST_PERMISSION, NO_ACCESS]"),
    )
  }

  "accessCategory" should "convert the value for DDM_ACCESSRIGHTS into the corresponding enum object" in {
    accessCategory(2, Headers.AccessRights)("ANONYMOUS_ACCESS").value shouldBe AccessCategory.ANONYMOUS_ACCESS
  }

  it should "fail if the DDM_ACCESSRIGHTS value does not correspond to an object in the enum" in {
    accessCategory(2, Headers.AccessRights)("unknown value").invalidValue shouldBe
      ParseError(2, "Value 'unknown value' is not a valid accessright in column DDM_ACCESSRIGHTS").chained
  }

  "creator" should "return None if none of the fields are defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "",
      Headers.CreatorInitials -> "",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "",
      Headers.CreatorOrganization -> "",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> ""
    ))

    creator(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION is defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "",
      Headers.CreatorInitials -> "",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "",
      Headers.CreatorOrganization -> "org",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> ""
    ))

    creator(row).value.value shouldBe CreatorOrganization("org", None)
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION and DCX_CREATOR_ROLE are defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "",
      Headers.CreatorInitials -> "",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "",
      Headers.CreatorOrganization -> "org",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> "ProjectManager"
    ))

    creator(row).value.value shouldBe CreatorOrganization("org", Some(ContributorRole.PROJECT_MANAGER))
  }

  it should "succeed with a person when only DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "",
      Headers.CreatorInitials -> "A.",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "Jones",
      Headers.CreatorOrganization -> "",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> ""
    ))

    creator(row).value.value shouldBe CreatorPerson(None, "A.", None, "Jones", None, None, None)
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "Dr.",
      Headers.CreatorInitials -> "A.",
      Headers.CreatorInsertions -> "X",
      Headers.CreatorSurname -> "Jones",
      Headers.CreatorOrganization -> "org",
      Headers.CreatorDAI -> "dai123",
      Headers.CreatorRole -> "rElAtEdpErsOn"
    ))

    creator(row).value.value shouldBe CreatorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some(ContributorRole.RELATED_PERSON), Some("dai123"))
  }

  it should "fail if DCX_CREATOR_INITIALS is not defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "Dr.",
      Headers.CreatorInitials -> "",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "Jones",
      Headers.CreatorOrganization -> "",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> ""
    ))

    creator(row).value.invalidValue shouldBe ParseError(2, "Missing value for: DCX_CREATOR_INITIALS").chained
  }

  it should "fail if DCX_CREATOR_SURNAME is not defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "Dr.",
      Headers.CreatorInitials -> "A.",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "",
      Headers.CreatorOrganization -> "",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> ""
    ))

    creator(row).value.invalidValue shouldBe ParseError(2, "Missing value for: DCX_CREATOR_SURNAME").chained
  }

  it should "fail if DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are both not defined" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "Dr.",
      Headers.CreatorInitials -> "",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "",
      Headers.CreatorOrganization -> "",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> ""
    ))

    creator(row).value.invalidValue shouldBe ParseError(2, "Missing value(s) for: [DCX_CREATOR_INITIALS, DCX_CREATOR_SURNAME]").chained
  }

  it should "fail if DCX_CREATOR_ROLE has an invalid value" in {
    val row = DepositRow(2, Map(
      Headers.CreatorTitles -> "Dr.",
      Headers.CreatorInitials -> "A.",
      Headers.CreatorInsertions -> "",
      Headers.CreatorSurname -> "Jones",
      Headers.CreatorOrganization -> "",
      Headers.CreatorDAI -> "",
      Headers.CreatorRole -> "invalid!"
    ))

    creator(row).value.invalidValue shouldBe ParseError(2, "Value 'invalid!' is not a valid creator role").chained
  }
}
