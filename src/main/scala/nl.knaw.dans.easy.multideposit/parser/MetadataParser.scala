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

import java.util.Locale

import cats.instances.either._
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.model.ContributorRole.ContributorRole
import nl.knaw.dans.easy.multideposit.model.DcType.DcType
import nl.knaw.dans.easy.multideposit.model.IdentifierType.IdentifierType
import nl.knaw.dans.easy.multideposit.model._

trait MetadataParser {
  this: ParserUtils =>

  val userLicenses: Set[String]

  def extractMetadata(rowNum: Int, rows: DepositRows): Validated[Metadata] = {
    (
      extractList(rows, "DCT_ALTERNATIVE").toValidated,
      extractList(rows, "DC_PUBLISHER").toValidated,
      extractDcType(rows),
      extractList(rows, "DC_FORMAT").toValidated,
      extractList(rows)(identifier),
      extractList(rows, "DC_SOURCE").toValidated,
      extractList(rows)(iso639_2Language("DC_LANGUAGE")),
      extractList(rows, "DCT_SPATIAL").toValidated,
      extractList(rows, "DCT_RIGHTSHOLDER").toValidated,
      extractList(rows)(relation),
      extractList(rows)(dateColumn),
      extractList(rows)(contributor),
      extractList(rows)(subject),
      extractList(rows)(spatialPoint),
      extractList(rows)(spatialBox),
      extractList(rows)(temporal),
      extractAtMostOne(rowNum, "DCT_LICENSE", rows)
        .flatMap {
          case Some(license) => userLicense(rowNum, "DCT_LICENSE")(license).map(_.some)
          case None => none.asRight
        }
        .toValidated,
    ).mapN(Metadata)
  }

  def extractDcType(rows: DepositRows): Validated[NonEmptyList[DcType]] = {
    extractList(rows)(dcType).map(_ defaultIfEmpty DcType.DATASET)
  }

  def dcType(row: DepositRow): Option[Validated[DcType]] = {
    row.find("DC_TYPE")
      .map(t => DcType.valueOf(t)
        .map(_.toValidated)
        .getOrElse(ParseError(row.rowNum, s"Value '$t' is not a valid type").toInvalid))
  }

  def identifier(row: DepositRow): Option[Validated[Identifier]] = {
    val identifier = row.find("DC_IDENTIFIER")
    val idType = row.find("DC_IDENTIFIER_TYPE")

    (identifier, idType) match {
      case (Some(id), idt) => Some {
        (
          id.toValidated,
          idt.map(identifierType(row.rowNum)).sequence[FailFast, IdentifierType].toValidated
        ).mapN(Identifier)
      }
      case (None, Some(_)) => Some {
        missingRequired(row, Set("DC_IDENTIFIER")).toInvalid
      }
      case (None, None) => none
    }
  }

  private def identifierType(rowNum: => Int)(role: String): FailFast[IdentifierType] = {
    IdentifierType.valueOf(role)
      .toRight(ParseError(rowNum, s"Value '$role' is not a valid identifier type"))
  }

  private lazy val iso639v2Languages = Locale.getISOLanguages.map(new Locale(_).getISO3Language).toSet

  def iso639_2Language(columnName: MultiDepositKey)(row: DepositRow): Option[Validated[String]] = {
    row.find(columnName)
      .map(lang => {
        // Most ISO 639-2/T languages are contained in the iso639v2Languages Set.
        // However, some of them are not and need to be checked using the second predicate.
        // The latter also allows to check ISO 639-2/B language codes.
        lazy val b0 = lang.length == 3
        lazy val b1 = iso639v2Languages.contains(lang)
        lazy val b2 = new Locale(lang).getDisplayLanguage.toLowerCase != lang.toLowerCase

        if (b0 && (b1 || b2)) lang.toValidated
        else ParseError(row.rowNum, s"Value '$lang' is not a valid value for $columnName").toInvalid
      })
  }

  def relation(row: DepositRow): Option[Validated[Relation]] = {
    val qualifier = row.find("DCX_RELATION_QUALIFIER")
    val link = row.find("DCX_RELATION_LINK")
    val title = row.find("DCX_RELATION_TITLE")

    (qualifier, link, title) match {
      case (Some(_), None, None) => Some {
        ParseError(row.rowNum, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well").toInvalid
      }
      case (_, Some(_), None) => Some {
        ParseError(row.rowNum, "When DCX_RELATION_LINK is defined, a DCX_RELATION_TITLE must be given as well to provide context").toInvalid
      }
      case (Some(q), l, t) => Some {
        (
          RelationQualifier.valueOf(q).map(_.toValidated).getOrElse(ParseError(row.rowNum, s"Value '$q' is not a valid relation qualifier").toInvalid),
          l.toValidated,
          t.toValidated,
        ).mapN(QualifiedRelation)
      }
      case (None, None, None) => None
      case (None, l, t) => Some {
        UnqualifiedRelation(l, t).toValidated
      }
    }
  }

  def dateColumn(row: DepositRow): Option[Validated[Date]] = {
    val dateString = row.find("DCT_DATE")
    val qualifierString = row.find("DCT_DATE_QUALIFIER")

    (dateString, qualifierString) match {
      case (Some(d), Some(q)) => Some {
        (
          date(row.rowNum, "DCT_DATE")(d).toValidated,
          DateQualifier.valueOf(q)
            .map(_.toValidated)
            .getOrElse {
              q.toLowerCase match {
                case "created" => ParseError(row.rowNum, s"DCT_DATE_QUALIFIER value '$q' is not allowed here. Use column DDM_CREATED instead.").toInvalid
                case "available" => ParseError(row.rowNum, s"DCT_DATE_QUALIFIER value '$q' is not allowed here. Use column DDM_AVAILABLE instead.").toInvalid
                case _ => ParseError(row.rowNum, s"Value '$q' is not a valid date qualifier").toInvalid
              }
            },
        ).mapN(QualifiedDate)
      }
      case (Some(d), None) => Some {
        TextualDate(d).toValidated
      }
      case (None, Some(_)) => Some {
        ParseError(row.rowNum, "DCT_DATE_QUALIFIER is only allowed to have a value if DCT_DATE has a well formatted date to go with it").toInvalid
      }
      case (None, None) => None
    }
  }

  def contributor(row: DepositRow): Option[Validated[Contributor]] = {
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
        (
          org.toValidated,
          cRole.map(contributorRole(row.rowNum)).sequence[FailFast, ContributorRole].toValidated,
        ).mapN(ContributorOrganization)
      }
      case (_, Some(init), _, Some(sur), _, _, _) => Some {
        (
          titles.toValidated,
          init.toValidated,
          insertions.toValidated,
          sur.toValidated,
          organization.toValidated,
          cRole.map(contributorRole(row.rowNum)).sequence[FailFast, ContributorRole].toValidated,
          dai.toValidated,
        ).mapN(ContributorPerson)
      }
      case (_, _, _, _, _, _, _) => Some(missingRequired(row, Set("DCX_CONTRIBUTOR_INITIALS", "DCX_CONTRIBUTOR_SURNAME")).toInvalid)
    }
  }

  private def contributorRole(rowNum: => Int)(role: String): FailFast[ContributorRole] = {
    ContributorRole.valueOf(role)
      .toRight(ParseError(rowNum, s"Value '$role' is not a valid contributor role"))
  }

  def subject(row: DepositRow): Option[Validated[Subject]] = {
    val subject = row.find("DC_SUBJECT")
    val scheme = row.find("DC_SUBJECT_SCHEME")

    (subject, scheme) match {
      case (Some(subj), sch) => Some {
        val subjectScheme: Validated[Option[String]] = sch.toValidated.ensure(ParseError(row.rowNum, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'").chained)(_.forall(_ == "abr:ABRcomplex"))

        (
          subj.toValidated,
          subjectScheme,
        ).mapN(Subject)
      }
      case (None, Some(_)) => Some {
        Subject(scheme = scheme).toValidated
      }
      case (None, None) => None
    }
  }

  def spatialPoint(row: DepositRow): Option[Validated[SpatialPoint]] = {
    val maybeX = row.find("DCX_SPATIAL_X")
    val maybeY = row.find("DCX_SPATIAL_Y")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (maybeX, maybeY, maybeScheme) match {
      case (Some(x), Some(y), scheme) => Some {
        SpatialPoint(x, y, scheme).toValidated
      }
      case (None, None, _) => None
      case _ => Some {
        missingRequired(row, Set("DCX_SPATIAL_X", "DCX_SPATIAL_Y")).toInvalid
      }
    }
  }

  def spatialBox(row: DepositRow): Option[Validated[SpatialBox]] = {
    val west = row.find("DCX_SPATIAL_WEST")
    val east = row.find("DCX_SPATIAL_EAST")
    val south = row.find("DCX_SPATIAL_SOUTH")
    val north = row.find("DCX_SPATIAL_NORTH")
    val maybeScheme = row.find("DCX_SPATIAL_SCHEME")

    (west, east, south, north, maybeScheme) match {
      case (Some(w), Some(e), Some(s), Some(n), scheme) => Some {
        SpatialBox(n, s, e, w, scheme).toValidated
      }
      case (None, None, None, None, _) => None
      case _ => Some {
        missingRequired(row, Set("DCX_SPATIAL_WEST", "DCX_SPATIAL_EAST", "DCX_SPATIAL_SOUTH", "DCX_SPATIAL_NORTH")).toInvalid
      }
    }
  }

  def temporal(row: DepositRow): Option[Validated[Temporal]] = {
    val temporal = row.find("DCT_TEMPORAL")
    val scheme = row.find("DCT_TEMPORAL_SCHEME")

    (temporal, scheme) match {
      case (Some(temp), sch) => Some {
        val temporalScheme: Validated[Option[String]] = sch.toValidated
          .ensure(ParseError(row.rowNum, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'").chained)(_.forall(_ == "abr:ABRperiode"))

        (
          temp.toValidated,
          temporalScheme,
        ).mapN(Temporal)
      }
      case (None, Some(_)) => Some {
        Temporal(scheme = scheme).toValidated
      }
      case (None, None) => None
    }
  }

  def userLicense(rowNum: => Int, columnName: => String)(licenseString: String): FailFast[UserLicense] = {
    if (userLicenses contains licenseString)
      UserLicense(licenseString).asRight
    else
      ParseError(rowNum, s"User license '$licenseString' is not allowed.").asLeft
  }
}
