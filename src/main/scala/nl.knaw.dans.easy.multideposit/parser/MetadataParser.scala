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

import nl.knaw.dans.easy.multideposit.ParseException
import nl.knaw.dans.easy.multideposit.model.{ ContributorRole, _ }
import nl.knaw.dans.lib.error._
import org.joda.time.DateTime

import scala.util.{ Failure, Success, Try }

trait MetadataParser {
  this: ParserUtils =>

  def extractMetadata(rows: DepositRows): Try[Metadata] = {
    Try { Metadata.curried }
      .map(_ (extractList(rows, "DCT_ALTERNATIVE")))
      .map(_ (extractList(rows, "DC_PUBLISHER")))
      .combine(extractList(rows)(dcType).map(_ defaultIfEmpty DcType.DATASET))
      .map(_ (extractList(rows, "DC_FORMAT")))
      .combine(extractList(rows)(identifier))
      .map(_ (extractList(rows, "DC_SOURCE")))
      .combine(extractList(rows)(iso639_2Language("DC_LANGUAGE")))
      .map(_ (extractList(rows, "DCT_SPATIAL")))
      .map(_ (extractList(rows, "DCT_RIGHTSHOLDER")))
      .combine(extractList(rows)(relation))
      .combine(extractList(rows)(dateColumn))
      .combine(extractList(rows)(contributor))
      .combine(extractList(rows)(subject))
      .combine(extractList(rows)(spatialPoint))
      .combine(extractList(rows)(spatialBox))
      .combine(extractList(rows)(temporal))
  }

  def contributor(rowNum: => Int)(row: DepositRow): Option[Try[Contributor]] = {
    val titles = row.find("DCX_CONTRIBUTOR_TITLES")
    val initials = row.find("DCX_CONTRIBUTOR_INITIALS")
    val insertions = row.find("DCX_CONTRIBUTOR_INSERTIONS")
    val surname = row.find("DCX_CONTRIBUTOR_SURNAME")
    val organization = row.find("DCX_CONTRIBUTOR_ORGANIZATION")
    val dai = row.find("DCX_CONTRIBUTOR_DAI")
    val cRole = row.find("DCX_CONTRIBUTOR_ROLE")

    (titles, initials, insertions, surname, organization, dai, cRole) match {
      case (None, None, None, None, None, None, None) => None
      case (None, None, None, None, Some(org), None, _) => Some {
        cRole.map(contributorRole(rowNum))
          .map(Try { ContributorOrganization.curried }.map(_ (org)).combine(_))
          .getOrElse(Try { ContributorOrganization(org) })
      }
      case (_, Some(init), _, Some(sur), _, _, _) => Some {
        cRole.map(contributorRole(rowNum))
          .map(Try { ContributorPerson.curried }
            .map(_ (titles))
            .map(_ (init))
            .map(_ (insertions))
            .map(_ (sur))
            .map(_ (organization))
            .combine(_)
            .map(_ (dai)))
          .getOrElse(Try { ContributorPerson(titles, init, insertions, sur, organization, dai = dai) })
      }
      case (_, _, _, _, _, _, _) => Some(missingRequired(rowNum, row, Set("DCX_CONTRIBUTOR_INITIALS", "DCX_CONTRIBUTOR_SURNAME")))
    }
  }

  def contributorRole(rowNum: => Int)(role: String): Try[Option[ContributorRole.Value]] = {
    ContributorRole.valueOf(role)
      .map(v => Success(Option(v)))
      .getOrElse(Failure(ParseException(rowNum, s"Value '$role' is not a valid contributor role")))
  }

  def identifier(rowNum: => Int)(row: DepositRow): Option[Try[Identifier]] = {
    val identifier = row.find("DC_IDENTIFIER")
    val identifierType = row.find("DC_IDENTIFIER_TYPE")

    (identifier, identifierType) match {
      case (Some(id), idt) => Some {
        idt.map(s => {
          val triedIdentifierType = IdentifierType.valueOf(s)
            .map(v => Success(Option(v)))
            .getOrElse(Failure(ParseException(rowNum, s"Value '$s' is not a valid identifier type")))

          Try { Identifier.curried }.map(_ (id)).combine(triedIdentifierType)
        }).getOrElse(Try { Identifier(id) })
      }
      case (None, Some(_)) => Some(missingRequired(rowNum, row, Set("DC_IDENTIFIER")))
      case (None, None) => None
    }
  }

  def dcType(rowNum: => Int)(row: DepositRow): Option[Try[DcType.Value]] = {
    row.find("DC_TYPE")
      .map(t => DcType.valueOf(t)
        .map(Success(_))
        .getOrElse(Failure(ParseException(rowNum, s"Value '$t' is not a valid type"))))
  }

  def relation(rowNum: => Int)(row: DepositRow): Option[Try[Relation]] = {
    val qualifier = row.find("DCX_RELATION_QUALIFIER")
    val link = row.find("DCX_RELATION_LINK")
    val title = row.find("DCX_RELATION_TITLE")

    (qualifier, link, title) match {
      case (Some(_), None, None) => Some(Failure(ParseException(rowNum, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well")))
      case (_, Some(_), None) => Some(Failure(ParseException(rowNum, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context")))
      case (Some(q), l, t) =>
        RelationQualifier.valueOf(q)
          .map(qf => Success(QualifiedRelation(qf, l, t)))
          .orElse(Some(Failure(ParseException(rowNum, s"Value '$q' is not a valid relation qualifier"))))
      case (None, None, None) => None
      case (None, l, t) => Some(Try { UnqualifiedRelation(l, t) })
    }
  }

  def dateColumn(rowNum: => Int)(row: DepositRow): Option[Try[Date]] = {
    val dateString = row.find("DCT_DATE")
    val qualifierString = row.find("DCT_DATE_QUALIFIER")

    (dateString, qualifierString) match {
      case (Some(d), Some(q)) =>
        DateQualifier.valueOf(q)
          .map(qualifier => {
            Try { DateTime.parse(d) }
              .map(date => Some(Success(QualifiedDate(date, qualifier))))
              .getOrRecover(e => Some(Failure(ParseException(rowNum, s"DCT_DATE value '$d' does not represent a date", e))))
          })
          .getOrElse {
            q.toLowerCase match {
              case "created" => Some(Failure(ParseException(rowNum, s"DCT_DATE_QUALIFIER value '$q' is not allowed here. Use column DDM_CREATED instead.")))
              case "available" => Some(Failure(ParseException(rowNum, s"DCT_DATE_QUALIFIER value '$q' is not allowed here. Use column DDM_AVAILABLE instead.")))
              case _ => Some(Failure(ParseException(rowNum, s"Value '$q' is not a valid date qualifier")))
            }
          }
      case (Some(d), None) => Some(Success(TextualDate(d)))
      case (None, Some(_)) => Some(Failure(ParseException(rowNum, "DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it")))
      case (None, None) => None
    }
  }

  def subject(rowNum: => Int)(row: DepositRow): Option[Try[Subject]] = {
    val subject = row.find("DC_SUBJECT")
    val scheme = row.find("DC_SUBJECT_SCHEME")

    (subject, scheme) match {
      case (Some(subj), Some(sch)) if sch == "abr:ABRcomplex" => Some(Try { Subject(subj, Some(sch)) })
      case (Some(_), Some(_)) => Some(Failure(ParseException(rowNum, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'")))
      case (Some(subj), None) => Some(Try { Subject(subj) })
      case (None, Some(_)) => Some(Try { Subject(scheme = scheme) })
      case (None, None) => None
    }
  }

  def temporal(rowNum: => Int)(row: DepositRow): Option[Try[Temporal]] = {
    val temporal = row.find("DCT_TEMPORAL")
    val scheme = row.find("DCT_TEMPORAL_SCHEME")

    (temporal, scheme) match {
      case (Some(temp), Some(sch)) if sch == "abr:ABRperiode" => Some(Try { Temporal(temp, Some(sch)) })
      case (Some(_), Some(_)) => Some(Failure(ParseException(rowNum, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'")))
      case (Some(temp), None) => Some(Try { Temporal(temp, None) })
      case (None, Some(_)) => Some(Try { Temporal(scheme = scheme) })
      case (None, None) => None
    }
  }

  def spatialPoint(rowNum: => Int)(row: DepositRow): Option[Try[SpatialPoint]] = {
    val maybeX = row.find("DCX_SPATIAL_X")
    val maybeY = row.find("DCX_SPATIAL_Y")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (maybeX, maybeY, maybeScheme) match {
      case (Some(x), Some(y), scheme) => Some(Try { SpatialPoint(x, y, scheme) })
      case (None, None, _) => None
      case _ => Some(missingRequired(rowNum, row, Set("DCX_SPATIAL_X", "DCX_SPATIAL_Y")))
    }
  }

  def spatialBox(rowNum: => Int)(row: DepositRow): Option[Try[SpatialBox]] = {
    val west = row.find("DCX_SPATIAL_WEST")
    val east = row.find("DCX_SPATIAL_EAST")
    val south = row.find("DCX_SPATIAL_SOUTH")
    val north = row.find("DCX_SPATIAL_NORTH")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (west, east, south, north, maybeScheme) match {
      case (Some(w), Some(e), Some(s), Some(n), scheme) => Some(Try { SpatialBox(n, s, e, w, scheme) })
      case (None, None, None, None, _) => None
      case _ => Some(missingRequired(rowNum, row, Set("DCX_SPATIAL_WEST", "DCX_SPATIAL_EAST", "DCX_SPATIAL_SOUTH", "DCX_SPATIAL_NORTH")))
    }
  }
}
