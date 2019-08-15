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

import cats.data.NonEmptyList
import cats.data.Validated.catchOnly
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.ContributorRole.ContributorRole
import nl.knaw.dans.easy.multideposit.model.{ ContributorRole, Creator, CreatorOrganization, CreatorPerson, Profile }
import org.joda.time.DateTime

trait ProfileParser {
  this: ParserUtils =>

  def extractProfile(rowNum: Int, rows: DepositRows): Validated[Profile] = {
    (
      extractAtLeastOne(rowNum, "DC_TITLE", rows),
      extractAtLeastOne(rowNum, "DC_DESCRIPTION", rows),
      extractCreators(rowNum, rows),
      extractDdmCreated(rowNum, rows),
      extractDdmAvailable(rowNum, rows),
      extractAtLeastOne(rowNum, "DDM_AUDIENCE", rows),
      extractDdmAccessrights(rowNum, rows),
    ).mapN(Profile)
      .andThen {
        case Profile(_, _, _, _, _, audiences, AccessCategory.GROUP_ACCESS) if !audiences.exists(_ == "D37000") =>
          ParseError(rowNum, "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeology)").toInvalid
        case otherwise => otherwise.toValidated
      }
  }

  private def extractCreators(rowNum: Int, rows: DepositRows): Validated[NonEmptyList[Creator]] = {
    extractList(rows)(creator)
      .ensure(ParseError(rowNum, "There should be at least one non-empty value for the creator fields").chained)(_.nonEmpty)
      .map(NonEmptyList.fromListUnsafe)
  }

  private def extractDdmCreated(rowNum: Int, rows: DepositRows): Validated[DateTime] = {
    extractExactlyOne(rowNum, "DDM_CREATED", rows)
      .andThen(date(rowNum, "DDM_CREATED"))
  }

  private def extractDdmAvailable(rowNum: Int, rows: DepositRows): Validated[DateTime] = {
    extractAtMostOne(rowNum, "DDM_AVAILABLE", rows)
      .andThen {
        case Some(value) => date(rowNum, "DDM_AVAILABLE")(value)
        case None => DateTime.now().toValidated
      }
  }

  private def extractDdmAccessrights(rowNum: Int, rows: DepositRows): Validated[AccessCategory] = {
    extractExactlyOne(rowNum, "DDM_ACCESSRIGHTS", rows)
      .andThen(accessCategory(rowNum, "DDM_ACCESSRIGHTS")(_))
  }

  def accessCategory(rowNum: => Int, columnName: => String)(s: String): Validated[AccessCategory] = {
    catchOnly[IllegalArgumentException] { AccessCategory.valueOf(s) }
      .leftMap(_ => ParseError(rowNum, s"Value '$s' is not a valid accessright in column $columnName"))
      .toValidatedNec
  }

  def creator(row: DepositRow): Option[Validated[Creator]] = {
    val titles = row.find("DCX_CREATOR_TITLES")
    val initials = row.find("DCX_CREATOR_INITIALS")
    val insertions = row.find("DCX_CREATOR_INSERTIONS")
    val surname = row.find("DCX_CREATOR_SURNAME")
    val organization = row.find("DCX_CREATOR_ORGANIZATION")
    val dai = row.find("DCX_CREATOR_DAI")
    val cRole = row.find("DCX_CREATOR_ROLE")

    (titles, initials, insertions, surname, organization, dai, cRole) match {
      case (None, None, None, None, None, None, None) => none
      case (None, None, None, None, Some(org), None, _) =>
        (
          org.toValidated,
          cRole.map(creatorRole(row.rowNum)).sequence,
        ).mapN(CreatorOrganization).some
      case (_, Some(init), _, Some(sur), _, _, _) =>
        (
          titles.toValidated,
          init.toValidated,
          insertions.toValidated,
          sur.toValidated,
          organization.toValidated,
          cRole.map(creatorRole(row.rowNum)).sequence,
          dai.toValidated,
        ).mapN(CreatorPerson).some
      case (_, _, _, _, _, _, _) =>
        missingRequired(row, Set("DCX_CREATOR_INITIALS", "DCX_CREATOR_SURNAME")).toInvalid.some
    }
  }

  private def creatorRole(rowNum: => Int)(role: String): Validated[ContributorRole] = {
    ContributorRole.valueOf(role)
      .toValidNec(ParseError(rowNum, s"Value '$role' is not a valid creator role"))
  }
}
