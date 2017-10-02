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

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, UnitSpec }
import nl.knaw.dans.lib.error.CompositeException
import org.joda.time.DateTime

import scala.util.{ Failure, Success }

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

class ProfileParserSpec extends UnitSpec with ProfileTestObjects {

  private val parser = new ProfileParser with ParserUtils {}

  import parser._

  "extractProfile" should "convert the csv input to the corresponding output" in {
    extractProfile(profileCSV, 2) should matchPattern { case Success(`profile`) => }
  }

  it should "fail if there are no values for DC_TITLE, DC_DESCRIPTION, creator, DDM_CREATED, DDM_AUDIENCE and DDM_ACCESSRIGHTS" in {
    val rows = Map.empty[MultiDepositKey, String] :: Map.empty[MultiDepositKey, String] :: Nil
    inside(extractProfile(rows, 2)) {
      case Failure(CompositeException(e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: Nil)) =>
        e1 should have message "There should be at least one non-empty value for DC_TITLE"
        e2 should have message "There should be at least one non-empty value for DC_DESCRIPTION"
        e3 should have message "There should be at least one non-empty value for the creator fields"
        e4 should have message "One row has to contain a value for the column: 'DDM_CREATED'"
        e5 should have message "There should be at least one non-empty value for DDM_AUDIENCE"
        e6 should have message "One row has to contain a value for the column: 'DDM_ACCESSRIGHTS'"
    }
  }

  it should "fail if there are multiple values for DDM_CREATED, DDM_AVAILABLE and DDM_ACCESSRIGHTS" in {
    val rows = profileCSVRow1 ::
      profileCSVRow2.updated("DDM_CREATED", "2015-07-30")
        .updated("DDM_AVAILABLE", "2015-07-31")
        .updated("DDM_ACCESSRIGHTS", "NO_ACCESS") :: Nil

    inside(extractProfile(rows, 2)) {
      case Failure(CompositeException(e1 :: e2 :: e3 :: Nil)) =>
        e1.getMessage should include("Only one row is allowed to contain a value for the column 'DDM_CREATED'")
        e2.getMessage should include("Only one row is allowed to contain a value for the column 'DDM_AVAILABLE'")
        e3.getMessage should include("Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'")
    }
  }

  it should "fail if DDM_ACCESSRIGHTS is GROUPACCESS and DDM_AUDIENCE does not contain D37000" in {
    val rows = profileCSVRow1 :: Nil

    inside(extractProfile(rows, 2)) {
      case Failure(ParseException(2, msg, _)) =>
        msg should {
          include("DDM_AUDIENCE should be D37000 (Archaeology)") and
            include("contains: [D30000]")
        }
    }
  }

  "date" should "convert the value of the date into the corresponding object" in {
    val row = Map("datum" -> "2016-07-30")

    inside(date("datum")(2)(row).value) {
      case Success(date) => date shouldBe DateTime.parse("2016-07-30")
    }
  }

  it should "return None if the date is not defined" in {
    val row = Map("datum" -> "")

    date("datum")(2)(row) shouldBe empty
  }

  it should "fail if the value does not represent a date" in {
    val row = Map("datum" -> "you can't parse me!")

    date("datum")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "datum value 'you can't parse me!' does not represent a date", _)) =>
    }
  }

  "accessCategory" should "convert the value for DDM_ACCESSRIGHTS into the corresponding enum object" in {
    val row = Map("DDM_ACCESSRIGHTS" -> "ANONYMOUS_ACCESS")
    accessCategory(2)(row).value should matchPattern {
      case Success(AccessCategory.ANONYMOUS_ACCESS) =>
    }
  }

  it should "return None if DDM_ACCESSRIGHTS is not defined" in {
    val row = Map("DDM_ACCESSRIGHTS" -> "")
    accessCategory(2)(row) shouldBe empty
  }

  it should "fail if the DDM_ACCESSRIGHTS value does not correspond to an object in the enum" in {
    val row = Map("DDM_ACCESSRIGHTS" -> "unknown value")
    accessCategory(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid accessright", _)) =>
    }
  }

  "creator" should "return None if none of the fields are defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    )

    creator(2)(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION is defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    )

    creator(2)(row).value should matchPattern { case Success(CreatorOrganization("org", None)) => }
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION and DCX_CREATOR_ROLE are defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> "ProjectManager"
    )

    creator(2)(row).value should matchPattern { case Success(CreatorOrganization("org", Some(ContributorRole.PROJECT_MANAGER))) => }
  }

  it should "succeed with a person when only DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Success(CreatorPerson(None, "A.", None, "Jones", None, None, None)) =>
    }
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "X",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "dai123",
      "DCX_CREATOR_ROLE" -> "rElAtEdpErsOn"
    )

    creator(2)(row).value should matchPattern {
      case Success(CreatorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some(ContributorRole.RELATED_PERSON), Some("dai123"))) =>
    }
  }

  it should "fail if DCX_CREATOR_INITIALS is not defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: DCX_CREATOR_INITIALS", _)) =>
    }
  }

  it should "fail if DCX_CREATOR_SURNAME is not defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: DCX_CREATOR_SURNAME", _)) =>
    }
  }

  it should "fail if DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are both not defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CREATOR_SURNAME, DCX_CREATOR_INITIALS]", _)) =>
    }
  }

  it should "fail if DCX_CREATOR_ROLE has an invalid value" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> "",
      "DCX_CREATOR_ROLE" -> "invalid!"
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'invalid!' is not a valid creator role", _)) =>
    }
  }
}
