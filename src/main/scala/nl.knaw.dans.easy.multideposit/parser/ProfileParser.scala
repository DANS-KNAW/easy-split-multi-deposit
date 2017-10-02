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
import nl.knaw.dans.easy.multideposit.ParseException
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error._
import org.joda.time.DateTime

import scala.util.{ Failure, Success, Try }

trait ProfileParser {
  this: ParserUtils =>

  def extractProfile(rows: DepositRows, rowNum: Int): Try[Profile] = {
    Try { Profile.curried }
      .combine(extractNEL(rows, rowNum, "DC_TITLE"))
      .combine(extractNEL(rows, rowNum, "DC_DESCRIPTION"))
      .combine(extractList(rows)(creator)
        .flatMap {
          case Seq() => Failure(ParseException(rowNum, "There should be at least one non-empty value for the creator fields"))
          case xs => Success(listToNEL(xs))
        })
      .combine(extractList(rows)(date("DDM_CREATED"))
        .flatMap(exactlyOne(rowNum, List("DDM_CREATED"))))
      .combine(extractList(rows)(date("DDM_AVAILABLE"))
        .flatMap(atMostOne(rowNum, List("DDM_AVAILABLE")))
        .map(_.getOrElse(DateTime.now())))
      .combine(extractNEL(rows, rowNum, "DDM_AUDIENCE"))
      .combine(extractList(rows)(accessCategory)
        .flatMap(exactlyOne(rowNum, List("DDM_ACCESSRIGHTS"))))
      .flatMap {
        case Profile(_, _, _, _, _, audiences, AccessCategory.GROUP_ACCESS) if !audiences.contains("D37000") =>
          Failure(ParseException(rowNum, "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE " +
            s"should be D37000 (Archaeology), but it contains: ${ audiences.mkString("[", ", ", "]") }"))
        case profile => Success(profile)
      }
  }

  def date(columnName: MultiDepositKey)(rowNum: => Int)(row: DepositRow): Option[Try[DateTime]] = {
    row.find(columnName)
      .map(date => Try { DateTime.parse(date) }.recoverWith {
        case e: IllegalArgumentException => Failure(ParseException(rowNum, s"$columnName value " +
          s"'$date' does not represent a date", e))
      })
  }

  def accessCategory(rowNum: => Int)(row: DepositRow): Option[Try[AccessCategory]] = {
    row.find("DDM_ACCESSRIGHTS")
      .map(acc => Try { AccessCategory.valueOf(acc) }
        .recoverWith {
          case e: IllegalArgumentException => Failure(ParseException(rowNum, s"Value '$acc' is " +
            s"not a valid accessright", e))
        })
  }

  def creator(rowNum: => Int)(row: DepositRow): Option[Try[Creator]] = {
    val titles = row.find("DCX_CREATOR_TITLES")
    val initials = row.find("DCX_CREATOR_INITIALS")
    val insertions = row.find("DCX_CREATOR_INSERTIONS")
    val surname = row.find("DCX_CREATOR_SURNAME")
    val organization = row.find("DCX_CREATOR_ORGANIZATION")
    val dai = row.find("DCX_CREATOR_DAI")
    val cRole = row.find("DCX_CREATOR_ROLE")

    (titles, initials, insertions, surname, organization, dai, cRole) match {
      case (None, None, None, None, None, None, None) => None
      case (None, None, None, None, Some(org), None, _) => Some {
        cRole.map(creatorRole(rowNum))
          .map(Try { CreatorOrganization.curried }.map(_ (org)).combine(_))
          .getOrElse(Try { CreatorOrganization(org) })
      }
      case (_, Some(init), _, Some(sur), _, _, _) => Some {
        cRole.map(creatorRole(rowNum))
          .map(Try { CreatorPerson.curried }
            .map(_ (titles))
            .map(_ (init))
            .map(_ (insertions))
            .map(_ (sur))
            .map(_ (organization))
            .combine(_)
            .map(_ (dai)))
          .getOrElse(Try { CreatorPerson(titles, init, insertions, sur, organization, dai = dai) })
      }
      case (_, _, _, _, _, _, _) => Some(missingRequired(rowNum, row, Set("DCX_CREATOR_INITIALS", "DCX_CREATOR_SURNAME")))
    }
  }

  def creatorRole(rowNum: => Int)(role: String): Try[Option[ContributorRole.Value]] = {
    ContributorRole.valueOf(role)
      .map(v => Success(Option(v)))
      .getOrElse(Failure(ParseException(rowNum, s"Value '$role' is not a valid creator role")))
  }
}
