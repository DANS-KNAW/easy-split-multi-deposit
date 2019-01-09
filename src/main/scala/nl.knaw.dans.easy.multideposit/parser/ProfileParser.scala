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

import cats.instances.either._
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.ContributorRole.ContributorRole
import nl.knaw.dans.easy.multideposit.model.{ ContributorRole, Creator, CreatorOrganization, CreatorPerson, NonEmptyList, Profile, listToNEL }
import org.joda.time.DateTime

trait ProfileParser {
  this: ParserUtils =>

  def extractProfile(rowNum: Int, rows: DepositRows): Validated[Profile] = {
    (
      extractAtLeastOne(rowNum, "DC_TITLE", rows).toValidated,
      extractAtLeastOne(rowNum, "DC_DESCRIPTION", rows).toValidated,
      extractCreators(rowNum, rows),
      extractExactlyOne(rowNum, "DDM_CREATED", rows)
        .flatMap(date(rowNum, "DDM_CREATED"))
        .toValidated,
      extractAtMostOne(rowNum, "DDM_AVAILABLE", rows)
        .flatMap(_.map(date(rowNum, "DDM_AVAILABLE")).getOrElse(DateTime.now().asRight))
        .toValidated,
      extractAtLeastOne(rowNum, "DDM_AUDIENCE", rows).toValidated,
      extractExactlyOne(rowNum, "DDM_ACCESSRIGHTS", rows)
        .flatMap(accessCategory(rowNum, "DDM_ACCESSRIGHTS"))
        .toValidated,
    ).mapN(Profile)
      .ensure(ParseError(rowNum, "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeology)").chained) {
        case Profile(_, _, _, _, _, audiences, AccessCategory.GROUP_ACCESS) => audiences.contains("D37000")
        case _ => true
      }
  }

  private def extractCreators(rowNum: Int, rows: DepositRows): Validated[NonEmptyList[Creator]] = {
    extractList(rows)(creator)
      .ensure(ParseError(rowNum, "There should be at least one non-empty value for the creator fields").chained)(_.nonEmpty)
      .map(listToNEL)
  }

  def accessCategory(rowNum: => Int, columnName: => String)(s: String): FailFast[AccessCategory] = {
    Either.catchOnly[IllegalArgumentException] { AccessCategory.valueOf(s) }
      .leftMap(_ => ParseError(rowNum, s"Value '$s' is not a valid accessright in column $columnName"))
  }

  def creator(rowNum: => Int)(row: DepositRow): Option[Validated[Creator]] = {
    val titles = row.find("DCX_CREATOR_TITLES")
    val initials = row.find("DCX_CREATOR_INITIALS")
    val insertions = row.find("DCX_CREATOR_INSERTIONS")
    val surname = row.find("DCX_CREATOR_SURNAME")
    val organization = row.find("DCX_CREATOR_ORGANIZATION")
    val dai = row.find("DCX_CREATOR_DAI")
    val cRole = row.find("DCX_CREATOR_ROLE")

    (titles, initials, insertions, surname, organization, dai, cRole) match {
      case (None, None, None, None, None, None, None) => none
      case (None, None, None, None, Some(org), None, _) => Some {
        (
          org.toValidated,
          cRole.map(creatorRole(rowNum)).sequence[FailFast, ContributorRole].toValidated,
        ).mapN(CreatorOrganization)
      }
      case (_, Some(init), _, Some(sur), _, _, _) => Some {
        (
          titles.toValidated,
          init.toValidated,
          insertions.toValidated,
          sur.toValidated,
          organization.toValidated,
          cRole.map(creatorRole(rowNum)).sequence[FailFast, ContributorRole].toValidated,
          dai.toValidated,
        ).mapN(CreatorPerson)
      }
      case (_, _, _, _, _, _, _) => Some {
        missingRequired(rowNum, row, Set("DCX_CREATOR_INITIALS", "DCX_CREATOR_SURNAME")).toInvalid
      }
    }
  }

  private def creatorRole(rowNum: => Int)(role: String): FailFast[ContributorRole] = {
    ContributorRole.valueOf(role)
      .toRight(ParseError(rowNum, s"Value '$role' is not a valid creator role"))
  }
}
