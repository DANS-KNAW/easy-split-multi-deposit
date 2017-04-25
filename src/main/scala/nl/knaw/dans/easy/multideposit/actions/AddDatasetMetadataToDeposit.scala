/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
import nl.knaw.dans.easy.multideposit.parser.{ Dataset, _ }
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{ Failure, Try }
import scala.xml.Elem

case class AddDatasetMetadataToDeposit(dataset: Dataset)(implicit settings: Settings) extends UnitAction[Unit] {

  override def execute(): Try[Unit] = writeDatasetMetadataXml(dataset)
}
object AddDatasetMetadataToDeposit {

  def writeDatasetMetadataXml(dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    Try {
      stagingDatasetMetadataFile(dataset.datasetId).writeXml(datasetToXml(dataset))
    } recoverWith {
      case NonFatal(e) => Failure(ActionException(dataset.row, s"Could not write dataset metadata: $e", e))
    }
  }

  def datasetToXml(dataset: Dataset): Elem = {
    // @formatter:off
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
      xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd">
      {createProfile(dataset.profile)}
      {createMetadata(dataset.metadata, dataset.audioVideo.springfield)}
    </ddm:DDM>
    // @formatter:on
  }

  def createProfile(profile: Profile): Elem = {
    // @formatter:off
    <ddm:profile>
      {profile.titles.map(elem("dc:title"))}
      {profile.descriptions.map(elem("dcterms:description"))}
      {profile.creators.map(createCreator)}
      {elem("ddm:created")(date(profile.created))}
      {elem("ddm:available")(date(profile.available))}
      {profile.audiences.map(elem("ddm:audience"))}
      {elem("ddm:accessRights")(profile.accessright.toString)}
    </ddm:profile>
    // @formatter:on
  }

  def date(dateTime: DateTime): String = {
    dateTime.toString(ISODateTimeFormat.date())
  }

  def createCreator(creator: Creator): Elem = {
    creator match {
      case CreatorOrganization(org) =>
        // @formatter:off
        <dcx-dai:creatorDetails>
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">{org}</dcx-dai:name>
          </dcx-dai:organization>
        </dcx-dai:creatorDetails>
        // @formatter:on
      case CreatorPerson(titles, initials, insertions, surname, organization, dai) =>
        // @formatter:off
        <dcx-dai:creatorDetails>
          <dcx-dai:author>{
            titles.map(ts => <dcx-dai:titles>{ts}</dcx-dai:titles>) ++
            <dcx-dai:initials>{initials}</dcx-dai:initials> ++
            insertions.map(is => <dcx-dai:insertions>{is}</dcx-dai:insertions>) ++
            <dcx-dai:surname>{surname}</dcx-dai:surname> ++
            dai.map(d => <dcx-dai:DAI>{d}</dcx-dai:DAI>) ++
            organization.map(org => <dcx-dai:name xml:lang="en">{org}</dcx-dai:name>)
          }</dcx-dai:author>
        </dcx-dai:creatorDetails>
        // @formatter:on
    }
  }

  def createContributor(contributor: Contributor): Elem = {
    contributor match {
      case ContributorOrganization(org) =>
        // @formatter:off
        <dcx-dai:contributorDetails>
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">{org}</dcx-dai:name>
          </dcx-dai:organization>
        </dcx-dai:contributorDetails>
        // @formatter:on
      case ContributorPerson(titles, initials, insertions, surname, organization, dai) =>
        // @formatter:off
        <dcx-dai:contributorDetails>
          <dcx-dai:author>{
            titles.map(ts => <dcx-dai:titles>{ts}</dcx-dai:titles>) ++
            <dcx-dai:initials>{initials}</dcx-dai:initials> ++
            insertions.map(is => <dcx-dai:insertions>{is}</dcx-dai:insertions>) ++
            <dcx-dai:surname>{surname}</dcx-dai:surname> ++
            dai.map(d => <dcx-dai:DAI>{d}</dcx-dai:DAI>) ++
            organization.map(org => <dcx-dai:name xml:lang="en">{org}</dcx-dai:name>)
          }</dcx-dai:author>
        </dcx-dai:contributorDetails>
        // @formatter:on
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
    lazy val xy = s"${point.x} ${point.y}"
    // coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X)
    lazy val yx = s"${point.y} ${point.x}"

    val pos = srsName match {
      case "http://www.opengis.net/def/crs/EPSG/0/28992" => xy
      case "http://www.opengis.net/def/crs/EPSG/0/4326" => yx
      case _ => yx
    }

    // @formatter:off
    <dcx-gml:spatial srsName={srsName}>
      <Point xmlns="http://www.opengis.net/gml">
        <pos>{pos}</pos>
      </Point>
    </dcx-gml:spatial>
    // @formatter:on
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
  def createSpatialBox(box: SpatialBoxx): Elem = {
    val srsName = box.scheme.map(createSrsName).getOrElse("")

    lazy val xy = (s"${ box.west } ${ box.south }", s"${ box.east } ${ box.north }")
    lazy val yx = (s"${ box.south } ${ box.west }", s"${ box.north } ${ box.east }")

    val (lower, upper) = srsName match {
      case "http://www.opengis.net/def/crs/EPSG/0/28992" => xy
      case "http://www.opengis.net/def/crs/EPSG/0/4326" => yx
      case _ => yx
    }

    // @formatter:off
    <dcx-gml:spatial>
      <boundedBy xmlns="http://www.opengis.net/gml">
        <Envelope srsName={srsName}>
          <lowerCorner>{lower}</lowerCorner>
          <upperCorner>{upper}</upperCorner>
        </Envelope>
      </boundedBy>
    </dcx-gml:spatial>
    // @formatter:on
  }

  def createTemporal(temporal: Temporal): Elem = {
    // @formatter:off
    temporal.scheme
      .map(scheme => <dcterms:temporal xsi:type={scheme}>{temporal.temporal}</dcterms:temporal>)
      .getOrElse(<dcterms:temporal>{temporal.temporal}</dcterms:temporal>)
    // @formatter:on
  }

  def createSubject(subject: Subject): Elem = {
    // @formatter:off
    subject.scheme
      .map(scheme => <dc:subject xsi:type={scheme}>{subject.subject}</dc:subject>)
      .getOrElse(<dc:subject>{subject.subject}</dc:subject>)
    // @formatter:on
  }

  def createRelation(relation: Relation): Elem = {
    relation match {
      case QualifiedLinkRelation(qualifier, link) => elem(s"dcterms:$qualifier")(link)
      case QualifiedTitleRelation(qualifier, title) => elem(s"dcterms:$qualifier")(title)
      case LinkRelation(link) => elem("dc:relation")(link)
      case TitleRelation(title) => elem("dc:relation")(title)
    }
  }

  def createSurrogateRelation(springfield: Springfield): Elem = {
    // @formatter:off
    <ddm:relation scheme="STREAMING_SURROGATE_RELATION">{
      s"/domain/${springfield.domain}/user/${springfield.user}/collection/${springfield.collection}/presentation/$$sdo-id"
    }</ddm:relation>
    // @formatter:on
  }

  def createMetadata(metadata: Metadata, maybeSpringfield: Option[Springfield] = Option.empty): Elem = {
    // @formatter:off
    <ddm:dcmiMetadata>
      {metadata.alternatives.map(elem("dcterms:alternative"))}
      {metadata.publishers.map(elem("dcterms:publisher"))}
      {metadata.types.map(elem("dcterms:type"))}
      {metadata.formats.map(elem("dc:format"))}
      {metadata.identifiers.map(elem("dc:identifier"))}
      {metadata.sources.map(elem("dc:source"))}
      {metadata.languages.map(elem("dc:language"))}
      {metadata.spatials.map(elem("dcterms:spatial"))}
      {metadata.rightsholder.map(elem("dcterms:rightsHolder"))}
      {metadata.relations.map(createRelation) ++ maybeSpringfield.map(createSurrogateRelation) }
      {metadata.contributors.map(createContributor)}
      {metadata.subjects.map(createSubject)}
      {metadata.spatialPoints.map(createSpatialPoint)}
      {metadata.spatialBoxes.map(createSpatialBox)}
      {metadata.temporal.map(createTemporal)}
    </ddm:dcmiMetadata>
    // @formatter:on
  }

  def elem(key: String)(value: String): Elem = {
    // @formatter:off
    <key>{value}</key>.copy(label=key)
    // @formatter:on
  }
}
