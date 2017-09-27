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
package nl.knaw.dans.easy.multideposit.actions

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddDatasetMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.model._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, Null, PrefixedAttribute }

case class AddDatasetMetadataToDeposit(deposit: Deposit)(implicit settings: Settings) extends UnitAction[Unit] {

  /**
   * Verifies whether all preconditions are met for this specific action.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    checkSpringFieldDepositHasAVformat(deposit)
  }

  override def execute(): Try[Unit] = writeDatasetMetadataXml(deposit)
}
object AddDatasetMetadataToDeposit {

  def writeDatasetMetadataXml(deposit: Deposit)(implicit settings: Settings): Try[Unit] = {
    Try {
      stagingDatasetMetadataFile(deposit.depositId).writeXml(depositToDDM(deposit))
    } recoverWith {
      case NonFatal(e) => Failure(ActionException(deposit.row, s"Could not write deposit metadata: $e", e))
    }
  }

  def checkSpringFieldDepositHasAVformat(deposit: Deposit): Try[Unit] = {

    deposit.audioVideo.springfield match {
      case None => Success(())
      case Some(_) => deposit.metadata.formats
        .find(s => s.startsWith("audio/") || s.startsWith("video/"))
        .map(_ => Success(()))
        .getOrElse(Failure(ActionException(deposit.row,
          "No audio/video Format found for this column: [DC_FORMAT]\n" +
            "cause: this column should contain at least one " +
            "audio/ or video/ value because SF columns are present")))
    }
  }

  def depositToDDM(deposit: Deposit)(implicit settings: Settings): Elem = {
    <ddm:DDM
      xmlns:ddm="http://easy.dans.knaw.nl/schemas/md/ddm/"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:dct="http://purl.org/dc/terms/"
      xmlns:dcterms="http://purl.org/dc/terms/"
      xmlns:dcmitype="http://purl.org/dc/dcmitype/"
      xmlns:dcx="http://easy.dans.knaw.nl/schemas/dcx/"
      xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
      xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
      xmlns:narcis="http://easy.dans.knaw.nl/schemas/vocab/narcis-type/"
      xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/"
      xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
      xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd">
      {createProfile(deposit.profile)}
      {createMetadata(deposit.metadata, deposit.audioVideo.springfield)}
    </ddm:DDM>
  }

  def createProfile(profile: Profile): Elem = {
    <ddm:profile>
      {profile.titles.map(elem("dc:title"))}
      {profile.descriptions.map(elem("dcterms:description"))}
      {profile.creators.map(createCreator)}
      {elem("ddm:created")(formatDate(profile.created))}
      {elem("ddm:available")(formatDate(profile.available))}
      {profile.audiences.map(elem("ddm:audience"))}
      {elem("ddm:accessRights")(profile.accessright.toString)}
    </ddm:profile>
  }

  def formatDate(dateTime: DateTime): String = {
    dateTime.toString(ISODateTimeFormat.date())
  }

  private def createOrganisation(org: String, role: Option[ContributorRole.Value] = Option.empty): Elem = {
    <dcx-dai:organization>{
      <dcx-dai:name xml:lang="en">{org}</dcx-dai:name> ++
      role.map(createRole)
    }</dcx-dai:organization>
  }

  private def createRole(role: ContributorRole.Value): Elem = {
    <dcx-dai:role>{role.toString}</dcx-dai:role>
  }

  def createCreator(creator: Creator): Elem = {
    creator match {
      case CreatorOrganization(org, role) =>
        <dcx-dai:creatorDetails>{createOrganisation(org, role)}</dcx-dai:creatorDetails>
      case CreatorPerson(titles, initials, insertions, surname, organization, role, dai) =>
        <dcx-dai:creatorDetails>
          <dcx-dai:author>{
            titles.map(ts => <dcx-dai:titles>{ts}</dcx-dai:titles>) ++
              <dcx-dai:initials>{initials}</dcx-dai:initials> ++
              insertions.map(is => <dcx-dai:insertions>{is}</dcx-dai:insertions>) ++
              <dcx-dai:surname>{surname}</dcx-dai:surname> ++
              role.map(createRole) ++
              dai.map(d => <dcx-dai:DAI>{d}</dcx-dai:DAI>) ++
              organization.map(createOrganisation(_))
          }</dcx-dai:author>
        </dcx-dai:creatorDetails>
    }
  }

  def createContributor(contributor: Contributor): Elem = {
    contributor match {
      case ContributorOrganization(org, role) =>
        <dcx-dai:contributorDetails>{createOrganisation(org, role)}</dcx-dai:contributorDetails>
      case ContributorPerson(titles, initials, insertions, surname, organization, role, dai) =>
        <dcx-dai:contributorDetails>
          <dcx-dai:author>{
            titles.map(ts => <dcx-dai:titles>{ts}</dcx-dai:titles>) ++
              <dcx-dai:initials>{initials}</dcx-dai:initials> ++
              insertions.map(is => <dcx-dai:insertions>{is}</dcx-dai:insertions>) ++
              <dcx-dai:surname>{surname}</dcx-dai:surname> ++
              role.map(createRole) ++
              dai.map(d => <dcx-dai:DAI>{d}</dcx-dai:DAI>) ++
              organization.map(createOrganisation(_))
          }</dcx-dai:author>
        </dcx-dai:contributorDetails>
    }
  }

  def createSrsName(scheme: String): String = {
    Map(
      "degrees" -> "http://www.opengis.net/def/crs/EPSG/0/4326",
      "RD" -> "http://www.opengis.net/def/crs/EPSG/0/28992"
    ).getOrElse(scheme, "")
  }

  def createSpatialPoint(point: SpatialPoint): Elem = {
    val srsName = point.scheme.map(createSrsName).getOrElse("")

    // coordinate order x, y = longitude (DCX_SPATIAL_X), latitude (DCX_SPATIAL_Y)
    lazy val xy = s"${ point.x } ${ point.y }"
    // coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X)
    lazy val yx = s"${ point.y } ${ point.x }"

    val pos = srsName match {
      case "http://www.opengis.net/def/crs/EPSG/0/28992" => xy
      case "http://www.opengis.net/def/crs/EPSG/0/4326" => yx
      case _ => yx
    }

    <dcx-gml:spatial srsName={srsName}>
      <Point xmlns="http://www.opengis.net/gml">
        <pos>{pos}</pos>
      </Point>
    </dcx-gml:spatial>
  }

  /*
   Note that Y is along North - South and X is along East - West
   The lower corner is with the minimal coordinate values and upper corner with the maximal coordinate values
   If x was increasing from West to East and y was increasing from South to North we would have
   lower corner (x,y) = (West,South) and upper corner (x,y) = (East,North)
   as shown in the schematic drawing of the box below.
   This is the case for the WGS84 and RD coordinate systems, but not per se for any other system!

                         upper(x,y)=(E,N)
                *---N---u
                |       |
                W       E
                |       |
    ^           l---S---*
    |           lower(x,y)=(W,S)
    y
     x -->

   */
  def createSpatialBox(box: SpatialBox): Elem = {
    val srsName = box.scheme.map(createSrsName).getOrElse("")

    lazy val xy = (s"${ box.west } ${ box.south }", s"${ box.east } ${ box.north }")
    lazy val yx = (s"${ box.south } ${ box.west }", s"${ box.north } ${ box.east }")

    val (lower, upper) = srsName match {
      case "http://www.opengis.net/def/crs/EPSG/0/28992" => xy
      case "http://www.opengis.net/def/crs/EPSG/0/4326" => yx
      case _ => yx
    }

    <dcx-gml:spatial>
      <boundedBy xmlns="http://www.opengis.net/gml">
        <Envelope srsName={srsName}>
          <lowerCorner>{lower}</lowerCorner>
          <upperCorner>{upper}</upperCorner>
        </Envelope>
      </boundedBy>
    </dcx-gml:spatial>
  }

  def createTemporal(temporal: Temporal): Elem = {
    temporal.scheme
      .map(scheme => <dcterms:temporal xsi:type={scheme}>{temporal.temporal}</dcterms:temporal>)
      .getOrElse(<dcterms:temporal>{temporal.temporal}</dcterms:temporal>)
  }

  def createSubject(subject: Subject): Elem = {
    subject.scheme
      .map(scheme => <dc:subject xsi:type={scheme}>{subject.subject}</dc:subject>)
      .getOrElse(<dc:subject>{subject.subject}</dc:subject>)
  }

  def createRelation(relation: Relation): Elem = {
    relation match {
      case QualifiedRelation(qualifier, Some(link), Some(title)) =>
        <key href={link}>{title}</key>.copy(label = s"ddm:${ qualifier.toString }")
      case QualifiedRelation(qualifier, Some(link), None) =>
        <key href={link}/>.copy(label = s"ddm:${ qualifier.toString }")
      case QualifiedRelation(qualifier, None, Some(title)) =>
        <key>{title}</key>.copy(label = s"dcterms:${ qualifier.toString }")
      case UnqualifiedRelation(Some(link), Some(title)) =>
        <ddm:relation href={link}>{title}</ddm:relation>
      case UnqualifiedRelation(Some(link), None) =>
        <ddm:relation href={link}/>
      case UnqualifiedRelation(None, Some(title)) =>
        <dc:relation>{title}</dc:relation>
      case other => throw new UnsupportedOperationException(s"Relation $other is not supported. You should not even be able to create this object!")
    }
  }

  def createSurrogateRelation(springfield: Springfield): Elem = {
    <ddm:relation scheme="STREAMING_SURROGATE_RELATION">{
      s"/domain/${ springfield.domain }/user/${ springfield.user }/collection/${ springfield.collection }/presentation/$$sdo-id"
    }</ddm:relation>
  }

  def createDate(date: Date): Elem = {
    date match {
      case QualifiedDate(d, q) => elem(s"dcterms:$q")(formatDate(d))
      case TextualDate(text) => elem("dc:date")(text)
    }
  }

  def createIdentifier(identifier: Identifier): Elem = {
    identifier.idType
      .map(idType => <dc:identifier xsi:type={s"id-type:$idType"}>{identifier.id}</dc:identifier>)
      .getOrElse(<dc:identifier>{identifier.id}</dc:identifier>)
  }

  def createType(dcType: DcType.Value): Elem = {
    <dcterms:type xsi:type="dcterms:DCMIType">{dcType.toString}</dcterms:type>
  }

  def createFormat(format: String)(implicit settings: Settings): Elem = {
    val xml = elem("dc:format")(format)

    if (settings.formats.contains(format))
      xml % new PrefixedAttribute("xsi", "type", "dcterms:IMT", Null)
    else
      xml
  }

  def createLanguage(lang: String): Elem = {
    <dc:language xsi:type="dcterms:ISO639-2">{lang}</dc:language>
  }

  def createMetadata(metadata: Metadata, maybeSpringfield: Option[Springfield] = Option.empty)(implicit settings: Settings): Elem = {
    <ddm:dcmiMetadata>
      {metadata.alternatives.map(elem("dcterms:alternative"))}
      {metadata.publishers.map(elem("dcterms:publisher"))}
      {metadata.types.map(createType)}
      {metadata.formats.map(createFormat)}
      {metadata.identifiers.map(createIdentifier)}
      {metadata.sources.map(elem("dc:source"))}
      {metadata.languages.map(createLanguage)}
      {metadata.spatials.map(elem("dcterms:spatial"))}
      {metadata.rightsholder.map(elem("dcterms:rightsHolder"))}
      {metadata.relations.map(createRelation) ++ maybeSpringfield.map(createSurrogateRelation) }
      {metadata.dates.map(createDate)}
      {metadata.contributors.map(createContributor)}
      {metadata.subjects.map(createSubject)}
      {metadata.spatialPoints.map(createSpatialPoint)}
      {metadata.spatialBoxes.map(createSpatialBox)}
      {metadata.temporal.map(createTemporal)}
    </ddm:dcmiMetadata>
  }

  def elem(key: String)(value: String): Elem = {
    <key>{value}</key>.copy(label = key)
  }
}
