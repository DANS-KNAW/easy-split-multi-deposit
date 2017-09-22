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

import java.nio.file.Paths

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddDatasetMetadataToDeposit.depositToDDM
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.parser._
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }
import scala.xml.{ Elem, Node }

class AddDatasetMetadataToDepositSpec extends UnitSpec with CustomMatchers with BeforeAndAfterEach {

  implicit val settings: Settings = Settings(
    multidepositDir = testDir.resolve("md"),
    stagingDir = testDir.resolve("sd"),
    formats = Set("text/xml")
  )

  val depositId = "ds1"
  val deposit: Deposit = Deposit(
    depositId = depositId,
    row = 1,
    depositorUserId = "dep",
    profile = Profile(
      titles = List("dataset title"),
      descriptions = List("omschr1"),
      creators = List(
        CreatorPerson(initials = "A.", surname = "Jones", organization = Option("Lorem ipsum dolor sit amet")),
        CreatorOrganization("consectetur adipiscing elit"),
        CreatorOrganization("sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")),
      created = DateTime.parse("1992-07-30"),
      available = DateTime.parse("1992-07-31"),
      audiences = List("everyone", "nobody", "some people", "people with yellow hear"),
      accessright = AccessCategory.NO_ACCESS
    ),
    metadata = Metadata(
      alternatives = List("foobar"),
      publishers = List("random publisher"),
      identifiers = List(Identifier("123456", Some(IdentifierType.ISBN)), Identifier("id"))
    )
  )

  "checkPreconditions" should "fail if the deposit contains SF_* fields, but no AV DC_FORMAT is given" in {
    val deposit = testDeposit1.copy(
      depositId = depositId,
      metadata = Metadata(
        formats = List("text/plain")
      )
    )
    inside(AddDatasetMetadataToDeposit(deposit).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should include("No audio/video Format found for this column: [DC_FORMAT]")
    }
  }

  val expectedXml: Elem = <ddm:DDM
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
    <ddm:profile>
      <dc:title>dataset title</dc:title>
      <dcterms:description>omschr1</dcterms:description>
      <dcx-dai:creatorDetails>
        <dcx-dai:author>
          <dcx-dai:initials>A.</dcx-dai:initials>
          <dcx-dai:surname>Jones</dcx-dai:surname>
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">Lorem ipsum dolor sit amet</dcx-dai:name>
          </dcx-dai:organization>
        </dcx-dai:author>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">consectetur adipiscing elit</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <dcx-dai:creatorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">sed do eiusmod tempor incididunt ut labore et dolore magna aliqua</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:creatorDetails>
      <ddm:created>1992-07-30</ddm:created>
      <ddm:available>1992-07-31</ddm:available>
      <ddm:audience>everyone</ddm:audience>
      <ddm:audience>nobody</ddm:audience>
      <ddm:audience>some people</ddm:audience>
      <ddm:audience>people with yellow hear</ddm:audience>
      <ddm:accessRights>NO_ACCESS</ddm:accessRights>
    </ddm:profile>
    <ddm:dcmiMetadata>
      <dcterms:alternative>foobar</dcterms:alternative>
      <dcterms:publisher>random publisher</dcterms:publisher>
      <dcterms:type xsi:type="dcterms:DCMIType">Dataset</dcterms:type>
      <dc:identifier xsi:type="id-type:ISBN">123456</dc:identifier>
      <dc:identifier>id</dc:identifier>
    </ddm:dcmiMetadata>
  </ddm:DDM>

  override def beforeEach(): Unit = {
    Paths.get(getClass.getResource("/allfields/input/ruimtereis01/reisverslag/centaur.mpg").toURI)
      .copyFile(settings.multidepositDir.resolve(s"$depositId/reisverslag/centaur.mpg"))
    Paths.get(getClass.getResource("/allfields/input/ruimtereis01/reisverslag/centaur.srt").toURI)
      .copyFile(settings.multidepositDir.resolve(s"$depositId/reisverslag/centaur.srt"))
  }

  "execute" should "write the metadata to a file at the correct place" in {
    val file = stagingDatasetMetadataFile(depositId)

    file.toFile should not(exist)

    AddDatasetMetadataToDeposit(deposit).execute shouldBe a[Success[_]]

    file.toFile should exist
  }

  "depositToDDM" should "return the expected xml" in {
    depositToDDM(deposit) should equalTrimmed(expectedXml)
  }

  it should "return xml on reading from the allfields input instructions csv" in {
    implicit val s2: Settings = settings.copy(multidepositDir = Paths.get(getClass.getResource("/allfields/input").toURI))
    val csv = Paths.get(getClass.getResource("/allfields/input/instructions.csv").toURI)
    inside(MultiDepositParser()(s2).parse(csv).map(_.map(depositToDDM(_)(s2)))) {
      case Success(xmls) => xmls should have size 4
    }
  }

  "createDcmiMetadata" should "return the expected dcmidata" in {
    val metadata = Metadata(
      alternatives = List("alt1", "alt2"),
      publishers = List("pub1"),
      types = List(DcType.INTERACTIVERESOURCE, DcType.SOFTWARE),
      formats = List("arbitrary format", "text/xml"),
      identifiers = List(Identifier("123456", Some(IdentifierType.ISBN)), Identifier("id")),
      sources = List("src", "test"),
      languages = List("eng", "nld"),
      spatials = List("sp1"),
      rightsholder = List("rh1"),
      relations = List(
        QualifiedRelation(RelationQualifier.Replaces, link = Some("l1"), title = Some("t1")),
        QualifiedRelation(RelationQualifier.IsVersionOf, link = Some("l2")),
        QualifiedRelation(RelationQualifier.HasVersion, title = Some("t3")),
        UnqualifiedRelation(link = Some("l4"), title = Some("t4")),
        UnqualifiedRelation(link = Some("l5")),
        UnqualifiedRelation(title = Some("t6"))),
      dates = List(
        QualifiedDate(new DateTime(2017, 7, 30, 0, 0), DateQualifier.VALID),
        QualifiedDate(new DateTime(2017, 7, 31, 0, 0), DateQualifier.DATE_SUBMITTED),
        TextualDate("foobar")
      ),
      contributors = List(
        ContributorOrganization("contr1"),
        ContributorPerson(initials = "A.B.", surname = "Jones"),
        ContributorPerson(Option("dr."), "C.", Option("X"), "Jones", Option("contr2"), Option("dai"))),
      subjects = List(
        Subject("me"),
        Subject("you"),
        Subject("him"),
        Subject("GX", Option("abr:ABRcomplex"))),
      spatialPoints = List(
        SpatialPoint("1", "2", Option("RD")),
        SpatialPoint("3", "4"),
        SpatialPoint("5", "6", Option("degrees"))),
      spatialBoxes = List(
        SpatialBox("1", "2", "3", "4", Option("RD")),
        SpatialBox("5", "6", "7", "8", None),
        SpatialBox("9", "10", "11", "12", Option("degrees"))),
      temporal = List(
        Temporal("1992-2016"),
        Temporal("PALEOV", Option("abr:ABRperiode")),
        Temporal("some arbitrary text"))
    )

    val actual: Node = AddDatasetMetadataToDeposit.createMetadata(metadata)
    val expectedXml: Node = <ddm:dcmiMetadata>
      <dcterms:alternative>alt1</dcterms:alternative>
      <dcterms:alternative>alt2</dcterms:alternative>
      <dcterms:publisher>pub1</dcterms:publisher>
      <dcterms:type xsi:type="dcterms:DCMIType">InteractiveResource</dcterms:type>
      <dcterms:type xsi:type="dcterms:DCMIType">Software</dcterms:type>
      <dc:format>arbitrary format</dc:format>
      <dc:format xsi:type="dcterms:IMT">text/xml</dc:format>
      <dc:identifier xsi:type="id-type:ISBN">123456</dc:identifier>
      <dc:identifier>id</dc:identifier>
      <dc:source>src</dc:source>
      <dc:source>test</dc:source>
      <dc:language xsi:type='dcterms:ISO639-2'>eng</dc:language>
      <dc:language xsi:type='dcterms:ISO639-2'>nld</dc:language>
      <dcterms:spatial>sp1</dcterms:spatial>
      <dcterms:rightsHolder>rh1</dcterms:rightsHolder>
      <ddm:replaces href="l1">t1</ddm:replaces>
      <ddm:isVersionOf href="l2"/>
      <dcterms:hasVersion>t3</dcterms:hasVersion>
      <ddm:relation href="l4">t4</ddm:relation>
      <ddm:relation href="l5"/>
      <dc:relation>t6</dc:relation>
      <dcterms:valid>2017-07-30</dcterms:valid>
      <dcterms:dateSubmitted>2017-07-31</dcterms:dateSubmitted>
      <dc:date>foobar</dc:date>
      <dcx-dai:contributorDetails>
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">contr1</dcx-dai:name>
        </dcx-dai:organization>
      </dcx-dai:contributorDetails>
      <dcx-dai:contributorDetails>
        <dcx-dai:author>
          <dcx-dai:initials>A.B.</dcx-dai:initials>
          <dcx-dai:surname>Jones</dcx-dai:surname>
        </dcx-dai:author>
      </dcx-dai:contributorDetails>
      <dcx-dai:contributorDetails>
        <dcx-dai:author>
          <dcx-dai:titles>dr.</dcx-dai:titles>
          <dcx-dai:initials>C.</dcx-dai:initials>
          <dcx-dai:insertions>X</dcx-dai:insertions>
          <dcx-dai:surname>Jones</dcx-dai:surname>
          <dcx-dai:DAI>dai</dcx-dai:DAI>
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">contr2</dcx-dai:name>
          </dcx-dai:organization>
        </dcx-dai:author>
      </dcx-dai:contributorDetails>
      <dc:subject>me</dc:subject>
      <dc:subject>you</dc:subject>
      <dc:subject>him</dc:subject>
      <dc:subject xsi:type="abr:ABRcomplex">GX</dc:subject>
      <dcx-gml:spatial srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
        <Point xmlns="http://www.opengis.net/gml">
          <pos>1 2</pos>
        </Point>
      </dcx-gml:spatial>
      <dcx-gml:spatial srsName="">
        <Point xmlns="http://www.opengis.net/gml">
          <pos>4 3</pos>
        </Point>
      </dcx-gml:spatial>
      <dcx-gml:spatial srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
        <Point xmlns="http://www.opengis.net/gml">
          <pos>6 5</pos>
        </Point>
      </dcx-gml:spatial>
      <dcx-gml:spatial>
        <boundedBy xmlns="http://www.opengis.net/gml">
          <Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/28992">
            <lowerCorner>4 2</lowerCorner>
            <upperCorner>3 1</upperCorner>
          </Envelope>
        </boundedBy>
      </dcx-gml:spatial>
      <dcx-gml:spatial>
        <boundedBy xmlns="http://www.opengis.net/gml">
          <Envelope srsName="">
            <lowerCorner>6 8</lowerCorner>
            <upperCorner>5 7</upperCorner>
          </Envelope>
        </boundedBy>
      </dcx-gml:spatial>
      <dcx-gml:spatial>
        <boundedBy xmlns="http://www.opengis.net/gml">
          <Envelope srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
            <lowerCorner>10 12</lowerCorner>
            <upperCorner>9 11</upperCorner>
          </Envelope>
        </boundedBy>
      </dcx-gml:spatial>
      <dcterms:temporal>1992-2016</dcterms:temporal>
      <dcterms:temporal xsi:type="abr:ABRperiode">PALEOV</dcterms:temporal>
      <dcterms:temporal>some arbitrary text</dcterms:temporal>
    </ddm:dcmiMetadata>

    actual should equalTrimmed(expectedXml)
  }

  "createSurrogateRelation" should "return the expected streaming surrogate relation" in {
    val springfield = Springfield("randomdomainname", "randomusername", "randomcollectionname")
    val expectedXml = <ddm:relation scheme="STREAMING_SURROGATE_RELATION">/domain/randomdomainname/user/randomusername/collection/randomcollectionname/presentation/$sdo-id</ddm:relation>

    AddDatasetMetadataToDeposit.createSurrogateRelation(springfield) should equalTrimmed(expectedXml)
  }

  it should "return a path with the default domain when no domain is specified" in {
    val springfield = Springfield(user = "randomusername", collection = "randomcollectionname")
    val expectedXml = <ddm:relation scheme="STREAMING_SURROGATE_RELATION">/domain/dans/user/randomusername/collection/randomcollectionname/presentation/$sdo-id</ddm:relation>

    AddDatasetMetadataToDeposit.createSurrogateRelation(springfield) should equalTrimmed(expectedXml)
  }
}
