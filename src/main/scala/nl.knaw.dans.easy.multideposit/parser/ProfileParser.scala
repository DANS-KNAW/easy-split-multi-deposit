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
import nl.knaw.dans.easy.multideposit.parser.Headers.Header
import org.joda.time.DateTime

trait ProfileParser {
  this: ParserUtils =>

  def extractProfile(rowNum: Int, rows: DepositRows): Validated[Profile] = {
    (
      extractAtLeastOne(rowNum, Headers.Title, rows),
      extractAtLeastOne(rowNum, Headers.Description, rows),
      extractCreators(rowNum, rows),
      extractDdmCreated(rowNum, rows),
      extractDdmAvailable(rowNum, rows),
      extractAtLeastOne(rowNum, Headers.Audience, rows),
      extractDdmAccessrights(rowNum, rows),
    ).mapN(Profile)
  }

  private def extractCreators(rowNum: Int, rows: DepositRows): Validated[NonEmptyList[Creator]] = {
    extractList(rows)(creator)
      .ensure(ParseError(rowNum, "There should be at least one non-empty value for the creator fields").chained)(_.nonEmpty)
      .map(NonEmptyList.fromListUnsafe)
  }

  private def extractDdmCreated(rowNum: Int, rows: DepositRows): Validated[DateTime] = {
    extractExactlyOne(rowNum, Headers.Created, rows)
      .andThen(date(rowNum, Headers.Created))
  }

  private def extractDdmAvailable(rowNum: Int, rows: DepositRows): Validated[DateTime] = {
    extractAtMostOne(rowNum, Headers.Available, rows)
      .andThen {
        case Some(value) => date(rowNum, Headers.Available)(value)
        case None => DateTime.now().toValidated
      }
  }

  private def extractDdmAccessrights(rowNum: Int, rows: DepositRows): Validated[AccessCategory] = {
    extractExactlyOne(rowNum, Headers.AccessRights, rows)
      .andThen(accessCategory(rowNum, Headers.AccessRights)(_))
  }

  def accessCategory(rowNum: => Int, columnName: => Header)(s: String): Validated[AccessCategory] = {
    catchOnly[IllegalArgumentException] { AccessCategory.valueOf(s) }
      .leftMap(_ => ParseError(rowNum, s"Value '$s' is not a valid accessright in column $columnName"))
      .toValidatedNec
  }

  def creator(row: DepositRow): Option[Validated[Creator]] = {
    val titles = row.find(Headers.CreatorTitles)
    val initials = row.find(Headers.CreatorInitials)
    val insertions = row.find(Headers.CreatorInsertions)
    val surname = row.find(Headers.CreatorSurname)
    val organization = row.find(Headers.CreatorOrganization)
    val dai = row.find(Headers.CreatorDAI)
    val cRole = row.find(Headers.CreatorRole)

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
        missingRequired(row, Headers.CreatorSurname, Headers.CreatorInitials).toInvalid.some
    }
  }

  private def creatorRole(rowNum: => Int)(role: String): Validated[ContributorRole] = {
    ContributorRole.valueOf(role)
      .toValidNec(ParseError(rowNum, s"Value '$role' is not a valid creator role"))
  }
}
