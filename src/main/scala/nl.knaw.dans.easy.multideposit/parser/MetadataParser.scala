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

import cats.data.NonEmptyList
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.option._
import cats.syntax.traverse._
import nl.knaw.dans.easy.multideposit.model.ContributorRole.ContributorRole
import nl.knaw.dans.easy.multideposit.model.DcType.DcType
import nl.knaw.dans.easy.multideposit.model.IdentifierType.IdentifierType
import nl.knaw.dans.easy.multideposit.model.SpatialScheme.SpatialScheme
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.parser.Headers.Header

trait MetadataParser {
  this: ParserUtils =>

  val userLicenses: Set[String]
  val countries: Set[String] = Set("NLD", "GBR", "DEU", "BEL")

  def extractMetadata(rowNum: Int, rows: DepositRows): Validated[Metadata] = {
    (
      extractList(rows, Headers.AlternativeTitle).toValidated,
      extractList(rows, Headers.Publisher).toValidated,
      extractDcType(rows),
      extractList(rows, Headers.Format).toValidated,
      extractList(rows)(identifier),
      extractList(rows, Headers.Source).toValidated,
      extractList(rows)(iso639_2Language(Headers.Language)),
      extractList(rows)(spatial),
      extractAtLeastOne(rowNum, Headers.Rightsholder, rows),
      extractList(rows)(relation),
      extractList(rows)(dateColumn),
      extractList(rows)(contributor),
      extractList(rows)(subject),
      extractList(rows)(spatialPoint),
      extractList(rows)(spatialBox),
      extractList(rows)(temporal),
      extractUserLicenses(rowNum, rows),
    ).mapN(Metadata)
  }

  private def extractDcType(rows: DepositRows): Validated[NonEmptyList[DcType]] = {
    extractList(rows)(dcType).map {
      case Nil => NonEmptyList.one(DcType.DATASET)
      case x :: xs => NonEmptyList(x, xs)
    }
  }

  private def extractUserLicenses(rowNum: Int, rows: DepositRows): Validated[Option[UserLicense]] = {
    extractAtMostOne(rowNum, Headers.License, rows)
      .andThen {
        case Some(license) => userLicense(rowNum, Headers.License)(license).map(_.some)
        case None => none.toValidated
      }
  }

  def dcType(row: DepositRow): Option[Validated[DcType]] = {
    row.find(Headers.Type)
      .map(t => DcType.valueOf(t)
        .map(_.toValidated)
        .getOrElse(ParseError(row.rowNum, s"Value '$t' is not a valid type").toInvalid))
  }

  def identifier(row: DepositRow): Option[Validated[Identifier]] = {
    val identifier = row.find(Headers.Identifier)
    val idType = row.find(Headers.IdentifierType)

    (identifier, idType) match {
      case (Some(id), idt) =>
        (
          id.toValidated,
          idt.map(identifierType(row.rowNum)).sequence
        ).mapN(Identifier).some
      case (None, Some(_)) => missingRequired(row, Headers.Identifier).toInvalid.some
      case (None, None) => none
    }
  }

  private def identifierType(rowNum: => Int)(role: String): Validated[IdentifierType] = {
    IdentifierType.valueOf(role)
      .toValidNec(ParseError(rowNum, s"Value '$role' is not a valid identifier type"))
  }

  private lazy val iso639v2Languages = Locale.getISOLanguages.map(new Locale(_).getISO3Language).toSet

  def iso639_2Language(columnName: Header)(row: DepositRow): Option[Validated[String]] = {
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

  def spatial(row: DepositRow): Option[Validated[Spatial]] = {
    val spatial = row.find(Headers.Spatial)
    val schemeSpatial = row.find(Headers.SchemeSpatial)

    (spatial, schemeSpatial) match {
      case (Some(sp), Some(scheme)) =>
        spatialScheme(row.rowNum)(scheme)
          .andThen(scheme => spatialCheck(row.rowNum)(sp, scheme)
            .map(Spatial(_, scheme.some))
          )
          .some
      case (Some(sp), None) =>
        (
          sp.toValidated,
          none.toValidated
        ).mapN(Spatial)
        .some
      case (None, Some(_)) => missingRequired(row, Headers.Spatial).toInvalid.some
      case (None, None) => none
    }
  }

  private def spatialScheme(rowNum: => Int)(scheme: String): Validated[SpatialScheme] = {
    SpatialScheme.valueOf(scheme)
      .toValidNec(ParseError(rowNum, s"Value '$scheme' is not a valid scheme"))
  }

  private def spatialCheck(rowNum: => Int)(spatial: String, scheme: SpatialScheme): Validated[String] = {
    (spatial, scheme) match {
      case (spatial, SpatialScheme.ISO3166) => countryCodeIso3166(rowNum)(spatial)
      case (spatial, _) => spatial.toValidated
    }
  }

  private def countryCodeIso3166(rowNum: => Int)(countryCode: String): Validated[String] = {
    if (countries.contains(countryCode))
      countryCode.toValidated
    else
      ParseError(rowNum, s"Value '$countryCode' is not a valid value for country code").toInvalid
  }

  def relation(row: DepositRow): Option[Validated[Relation]] = {
    val qualifier = row.find(Headers.RelationQualifier)
    val link = row.find(Headers.RelationLink)
    val title = row.find(Headers.RelationTitle)

    (qualifier, link, title) match {
      case (Some(_), None, None) =>
        ParseError(row.rowNum, s"When ${ Headers.RelationQualifier } is defined, one of the values [${ Headers.RelationLink }, ${ Headers.RelationTitle }] must be defined as well").toInvalid.some
      case (_, Some(_), None) =>
        ParseError(row.rowNum, s"When ${ Headers.RelationLink } is defined, a ${ Headers.RelationTitle } must be given as well to provide context").toInvalid.some
      case (Some(q), l, t) =>
        (
          RelationQualifier.valueOf(q).map(_.toValidated).getOrElse(ParseError(row.rowNum, s"Value '$q' is not a valid relation qualifier").toInvalid),
          l.traverse(uri(row.rowNum, Headers.RelationLink)),
          t.toValidated,
        ).mapN(QualifiedRelation).some
      case (None, None, None) => none
      case (None, l, t) =>
        (
          l.traverse(uri(row.rowNum, Headers.RelationLink)),
          t.toValidated
        ).mapN(UnqualifiedRelation).some
    }
  }

  def dateColumn(row: DepositRow): Option[Validated[Date]] = {
    val dateString = row.find(Headers.Date)
    val qualifierString = row.find(Headers.DateQualifier)

    (dateString, qualifierString) match {
      case (Some(d), Some(q)) =>
        (
          date(row.rowNum, Headers.Date)(d),
          DateQualifier.valueOf(q)
            .map(_.toValidated)
            .getOrElse {
              q.toLowerCase match {
                case "created" => ParseError(row.rowNum, s"${ Headers.DateQualifier } value '$q' is not allowed here. Use column ${ Headers.Created } instead.").toInvalid
                case "available" => ParseError(row.rowNum, s"${ Headers.DateQualifier } value '$q' is not allowed here. Use column ${ Headers.Available } instead.").toInvalid
                case _ => ParseError(row.rowNum, s"Value '$q' is not a valid date qualifier").toInvalid
              }
            },
        ).mapN(QualifiedDate).some
      case (Some(d), None) => TextualDate(d).toValidated.some
      case (None, Some(_)) =>
        ParseError(row.rowNum, s"${ Headers.DateQualifier } is only allowed to have a value if ${ Headers.Date } has a well formatted date to go with it").toInvalid.some
      case (None, None) => none
    }
  }

  def contributor(row: DepositRow): Option[Validated[Contributor]] = {
    val titles = row.find(Headers.ContributorTitles)
    val initials = row.find(Headers.ContributorInitials)
    val insertions = row.find(Headers.ContributorInsertions)
    val surname = row.find(Headers.ContributorSurname)
    val organization = row.find(Headers.ContributorOrganization)
    val dai = row.find(Headers.ContributorDAI)
    val cRole = row.find(Headers.ContributorRole)

    (titles, initials, insertions, surname, organization, dai, cRole) match {
      case (None, None, None, None, None, None, None) => none
      case (None, None, None, None, Some(org), None, _) =>
        (
          org.toValidated,
          cRole.map(contributorRole(row.rowNum)).sequence,
        ).mapN(ContributorOrganization).some
      case (_, Some(init), _, Some(sur), _, _, _) =>
        (
          titles.toValidated,
          init.toValidated,
          insertions.toValidated,
          sur.toValidated,
          organization.toValidated,
          cRole.map(contributorRole(row.rowNum)).sequence,
          dai.toValidated,
        ).mapN(ContributorPerson).some
      case (_, _, _, _, _, _, _) => missingRequired(row, Headers.ContributorInitials, Headers.ContributorSurname).toInvalid.some
    }
  }

  private def contributorRole(rowNum: => Int)(role: String): Validated[ContributorRole] = {
    ContributorRole.valueOf(role)
      .toValidNec(ParseError(rowNum, s"Value '$role' is not a valid contributor role"))
  }

  def subject(row: DepositRow): Option[Validated[Subject]] = {
    val subject = row.find(Headers.Subject)
    val scheme = row.find(Headers.SubjectScheme)

    (subject, scheme) match {
      case (Some(subj), sch) =>
        val subjectScheme: Validated[Option[String]] = sch.toValidated
          .andThen {
            case abr @ Some("abr:ABRcomplex") => abr.toValidated
            case Some(_) => ParseError(row.rowNum, s"The given value for ${ Headers.SubjectScheme } is not allowed. This can only be 'abr:ABRcomplex'").toInvalid
            case None => none.toValidated
          }

        (
          subj.toValidated,
          subjectScheme,
        ).mapN(Subject).some
      case (None, Some(_)) => Subject(scheme = scheme).toValidated.some
      case (None, None) => none
    }
  }

  def spatialPoint(row: DepositRow): Option[Validated[SpatialPoint]] = {
    val maybeX = row.find(Headers.SpatialX)
    val maybeY = row.find(Headers.SpatialY)
    val maybeScheme = row.find(Headers.SpatialScheme)

    (maybeX, maybeY, maybeScheme) match {
      case (Some(x), Some(y), scheme) => SpatialPoint(x, y, scheme).toValidated.some
      case (None, None, _) => none
      case _ => missingRequired(row, Headers.SpatialX, Headers.SpatialY).toInvalid.some
    }
  }

  def spatialBox(row: DepositRow): Option[Validated[SpatialBox]] = {
    val west = row.find(Headers.SpatialWest)
    val east = row.find(Headers.SpatialEast)
    val south = row.find(Headers.SpatialSouth)
    val north = row.find(Headers.SpatialNorth)
    val maybeScheme = row.find(Headers.SpatialScheme)

    (west, east, south, north, maybeScheme) match {
      case (Some(w), Some(e), Some(s), Some(n), scheme) =>
        SpatialBox(n, s, e, w, scheme).toValidated.some
      case (None, None, None, None, _) => none
      case _ =>
        missingRequired(row, Headers.SpatialWest, Headers.SpatialEast, Headers.SpatialSouth, Headers.SpatialNorth).toInvalid.some
    }
  }

  def temporal(row: DepositRow): Option[Validated[Temporal]] = {
    val temporal = row.find(Headers.Temporal)
    val scheme = row.find(Headers.TemporalScheme)

    (temporal, scheme) match {
      case (Some(temp), sch) =>
        val temporalScheme: Validated[Option[String]] = sch.toValidated
          .andThen {
            case abr @ Some("abr:ABRperiode") => abr.toValidated
            case Some(_) => ParseError(row.rowNum, s"The given value for ${ Headers.TemporalScheme } is not allowed. This can only be 'abr:ABRperiode'").toInvalid
            case None => none.toValidated
          }

        (
          temp.toValidated,
          temporalScheme,
        ).mapN(Temporal).some
      case (None, Some(_)) => Temporal(scheme = scheme).toValidated.some
      case (None, None) => none
    }
  }

  def userLicense(rowNum: => Int, columnName: => Header)(licenseString: String): Validated[UserLicense] = {
    if (userLicenses contains licenseString)
      UserLicense(licenseString).toValidated
    else
      ParseError(rowNum, s"User license '$licenseString' is not allowed in column $columnName.").toInvalid
  }
}
