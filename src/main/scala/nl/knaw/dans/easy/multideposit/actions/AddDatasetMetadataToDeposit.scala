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

import nl.knaw.dans.easy.multideposit.DDM._
import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.AddDatasetMetadataToDeposit._
import nl.knaw.dans.lib.error.TraversableTryExtensions
import org.joda.time.DateTime

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.Elem

case class AddDatasetMetadataToDeposit(row: Int, entry: (DatasetID, Dataset))(implicit settings: Settings) extends Action {

  val (datasetID, dataset) = entry

  /**
   * Verifies whether all preconditions are met for this specific action.
   * Required metadata fields and some allowed values are checked,
   * field interdependencies are taken into consideration
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    (datasetRowValidations(row, dataset) ++ datasetColumnValidations(row, dataset))
      .collectResults
      .map(_ => ())
  }

  override def execute(): Try[Unit] = writeDatasetMetadataXml(row, datasetID, dataset)
}
object AddDatasetMetadataToDeposit {

  def datasetColumnValidations(row: Int, dataset: Dataset): Seq[Try[Unit]] = {
    import validators._
    List(
      checkColumnHasOnlyOneValue(row, dataset, "DDM_CREATED"),
      checkColumnsHaveAtMostOneRowWithValues(row, dataset, "SF_DOMAIN", "SF_USER", "SF_COLLECTION")
    )
  }

  def datasetRowValidations(row: Int, dataset: Dataset): Seq[Try[Unit]] = {
    import validators._
    dataset.toRows.flatMap(rowVals => {
      List(
        // date created format
        checkDateFormatting(row, rowVals, "DDM_CREATED"),
        checkDateFormatting(row, rowVals, "DDM_AVAILABLE"),

        // only valid chars allowed
        checkValidChars(row, rowVals, "SF_COLLECTION"),
        checkValidChars(row, rowVals, "SF_DOMAIN"),
        checkValidChars(row, rowVals, "SF_USER"),
        checkValidChars(row, rowVals, "DATASET"),

        // coordinates
        // point
        checkAllOrNone(row, rowVals, "DCX_SPATIAL_X", "DCX_SPATIAL_Y"),
        // box
        checkAllOrNone(row, rowVals, "DCX_SPATIAL_NORTH", "DCX_SPATIAL_SOUTH", "DCX_SPATIAL_EAST", "DCX_SPATIAL_WEST"),

        // persons
        // note that the DCX_{}_ORGANISATION can have a value independent of the other fields
        // creator
        checkRequiredWithGroup(row, rowVals,
          List("DCX_CREATOR_INITIALS", "DCX_CREATOR_SURNAME"),
          List("DCX_CREATOR_TITLES", "DCX_CREATOR_INSERTIONS", "DCX_CREATOR_DAI")),
        // contributor
        checkRequiredWithGroup(row, rowVals,
          List("DCX_CONTRIBUTOR_INITIALS", "DCX_CONTRIBUTOR_SURNAME"),
          List("DCX_CONTRIBUTOR_TITLES", "DCX_CONTRIBUTOR_INSERTIONS", "DCX_CONTRIBUTOR_DAI")),

        // check allowed value(s)
        // scheme
        checkValueIsOneOf(row, rowVals, "DCT_TEMPORAL_SCHEME", "abr:ABRperiode"),
        checkValueIsOneOf(row, rowVals, "DC_SUBJECT_SCHEME", "abr:ABRcomplex"),

        checkAccessRights(row, rowVals),

        // {link, title} can not be defined both, a qualifier is only allowed if link or title are present
        rowVals.get("DCX_RELATION_QUALIFIER").filterNot(_.isBlank)
          .map(_ => checkOneOf(row, rowVals, "DCX_RELATION_LINK", "DCX_RELATION_TITLE"))
          .getOrElse(checkNotAll(row, rowVals, "DCX_RELATION_LINK", "DCX_RELATION_TITLE"))
      )
    })
  }

  def writeDatasetMetadataXml(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    Try {
      stagingDatasetMetadataFile(datasetID).writeXml(datasetToXml(dataset))
    } recoverWith {
      case NonFatal(e) => Failure(ActionException(row, s"Could not write dataset metadata: $e", e))
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
      xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2012/11/ddm.xsd">
      {createProfile(dataset)}
      {createMetadata(dataset)}
    </ddm:DDM>
    // @formatter:on
  }

  def createProfile(dataset: Dataset): Elem = {
    // @formatter:off
    <ddm:profile>
      {profileElems(dataset, "DC_TITLE")}
      {profileElems(dataset, "DC_DESCRIPTION")}
      {createCreators(dataset)}
      {profileElems(dataset, "DDM_CREATED")}
      {profileElems(dataset, "DDM_AUDIENCE")}
      {profileElems(dataset, "DDM_ACCESSRIGHTS")}
    </ddm:profile>
    // @formatter:on
  }

  def profileElems(dataset: Dataset, key: MultiDepositKey): Seq[Elem] = {
    elemsFromKeyValues(key, dataset.getOrElse(key, List()))
  }

  def elemsFromKeyValues(key: MultiDepositKey, values: MultiDepositValues): Seq[Elem] = {
    values.filter(_.nonEmpty)
      .map(elem(profileFields.getOrElse(key, key)))
  }

  def createCreators(dataset: Dataset): Seq[Elem] = {
    dataset.rowsWithValuesFor(composedCreatorFields).map(mdKeyValues =>
      // @formatter:off
      <dcx-dai:creatorDetails>{
        if (isOrganization(mdKeyValues))
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">{
              mdKeyValues.find(field => organizationKeys.contains(field._1)).map(_._2).getOrElse("")
            }</dcx-dai:name>
          </dcx-dai:organization>
        else
          <dcx-dai:author>{
            mdKeyValues.map(composedEntry(composedCreatorFields))
          }</dcx-dai:author>
      }</dcx-dai:creatorDetails>
      // @formatter:off
    )
  }

  def isOrganization(authorFields: Iterable[(MultiDepositKey, String)]): Boolean = {
    val othersEmpty = authorFields
      .filterNot(field => organizationKeys.contains(field._1))
      .forall(_._2 == "")
    val hasOrganization = authorFields.toList.exists(field => organizationKeys.contains(field._1))
    othersEmpty && hasOrganization
  }

  def createContributors(dataset: Dataset): Seq[Elem] = {
    dataset.rowsWithValuesFor(composedContributorFields).map(mdKeyValues =>
      // @formatter:off
      <dcx-dai:contributorDetails>{
        if (isOrganization(mdKeyValues))
          <dcx-dai:organization>
            <dcx-dai:name xml:lang="en">{
              mdKeyValues.find(field => organizationKeys.contains(field._1)).map(_._2).getOrElse("")
            }</dcx-dai:name>
          </dcx-dai:organization>
        else
          <dcx-dai:author>{
            mdKeyValues.map(composedEntry(composedContributorFields))
          }</dcx-dai:author>
      }</dcx-dai:contributorDetails>
      // @formatter:on
    )
  }

  def composedEntry(dictionary: Dictionary)(entry: (MultiDepositKey, String)): Elem = {
    val (key, value) = entry
    if (organizationKeys.contains(key)) {
      // @formatter:off
      <dcx-dai:organization>
        <dcx-dai:name xml:lang="en">{value}</dcx-dai:name>
      </dcx-dai:organization>
      // @formatter:on
    }
    else elem(dictionary.getOrElse(key, key))(value)
  }

  def createSrsName(fields: DatasetRow): String = {
    Map(
      "degrees" -> "http://www.opengis.net/def/crs/EPSG/0/4326",
      "RD" -> "http://www.opengis.net/def/crs/EPSG/0/28992"
    ).getOrElse(fields.getOrElse("DCX_SPATIAL_SCHEME", ""), "")
  }

  def createSpatialPoints(dataset: Dataset): Seq[Elem] = {
    dataset.rowsWithValuesForAllOf(composedSpatialPointFields).map(mdKeyValues => {
      val srsName = createSrsName(mdKeyValues)
      // coordinate order x, y = longitude (DCX_SPATIAL_X), latitude (DCX_SPATIAL_Y)
      lazy val xy = s"${mdKeyValues.getOrElse("DCX_SPATIAL_X", "")} ${mdKeyValues.getOrElse("DCX_SPATIAL_Y", "")}"
      // coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X)
      lazy val yx = s"${mdKeyValues.getOrElse("DCX_SPATIAL_Y", "")} ${mdKeyValues.getOrElse("DCX_SPATIAL_X", "")}"

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
    })
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
  def createSpatialBoxes(dataset: Dataset): Seq[Elem] = {
    dataset.rowsWithValuesForAllOf(composedSpatialBoxFields).map(mdKeyValues => {
      val srsName = createSrsName(mdKeyValues)

      lazy val lowerX = mdKeyValues.getOrElse("DCX_SPATIAL_WEST", "")
      lazy val upperX = mdKeyValues.getOrElse("DCX_SPATIAL_EAST", "")
      lazy val lowerY = mdKeyValues.getOrElse("DCX_SPATIAL_SOUTH", "")
      lazy val upperY = mdKeyValues.getOrElse("DCX_SPATIAL_NORTH", "")
      lazy val xy = (s"$lowerX $lowerY", s"$upperX $upperY")
      lazy val yx = (s"$lowerY $lowerX", s"$upperY $upperX")

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
    })
  }

  def createSchemedMetadata(dataset: Dataset, fields: Dictionary, key: MultiDepositKey, schemeKey: MultiDepositKey): Seq[Elem] = {
    val xmlKey = fields.getOrElse(key, key)
    dataset.rowsWithValuesFor(fields).map(mdKeyValues => {
      val value = mdKeyValues.getOrElse(key, "")
      mdKeyValues.get(schemeKey)
        // @formatter:off
        .map(scheme => <key xsi:type={scheme}>{value}</key>.copy(label = xmlKey))
        // @formatter:on
        .getOrElse(elem(xmlKey)(value))
    })
  }

  def createTemporal(dataset: Dataset): Seq[Elem] = {
    createSchemedMetadata(dataset, composedTemporalFields, "DCT_TEMPORAL", "DCT_TEMPORAL_SCHEME")
  }

  def createSubject(dataset: Dataset): Seq[Elem] = {
    createSchemedMetadata(dataset, composedSubjectFields, "DC_SUBJECT", "DC_SUBJECT_SCHEME")
  }

  /*
    qualifier   link   title   valid
        1        1       1       0
        1        1       0       1
        1        0       1       1
        1        0       0       0
        0        1       1       0
        0        1       0       1
        0        0       1       1
        0        0       0       1

    observation: if the qualifier is present, either DCX_RELATION_LINK or DCX_RELATION_TITLE must be defined
                 if the qualifier is not defined, DCX_RELATION_LINK and DCX_RELATION_TITLE must not both be defined
   */
  def createRelations(dataset: Dataset): Seq[Elem] = {
    dataset.rowsWithValuesFor(composedRelationFields).map(row =>
      (row.get("DCX_RELATION_QUALIFIER"), row.get("DCX_RELATION_LINK"), row.get("DCX_RELATION_TITLE")) match {
        // @formatter:off
        case (Some(q), Some(l), _      ) => elem(s"dcterms:$q")(l)
        case (Some(q), None,    Some(t)) => elem(s"dcterms:$q")(t)
        case (None,    Some(l), _      ) => elem(s"dc:relation")(l)
        case (None,    None,    Some(t)) => elem(s"dc:relation")(t)
        case _                           =>
          // all other cases are checked not to occur in the preconditions
          throw new IllegalArgumentException("preconditions should have reported this as an error")
        // @formatter:on
      })
  }

  def createSurrogateRelation(dataset: Dataset): Option[Elem] = {
    CreateSpringfieldActions.getSpringfieldPath(dataset, 0)
      .map(path =>
        // @formatter:off
        <ddm:relation scheme="STREAMING_SURROGATE_RELATION">{path}</ddm:relation>
        // @formatter:on
      )
  }

  def createMetadata(dataset: Dataset): Elem = {
    def isMetaData(key: MultiDepositKey, values: MultiDepositValues): Boolean = {
      metadataFields.contains(key) && values.nonEmpty
    }

    // @formatter:off
    <ddm:dcmiMetadata>
      {dataset.filter(isMetaData _ tupled).flatMap(simpleMetadataEntryToXML _ tupled)}
      {createRelations(dataset) ++ createSurrogateRelation(dataset) }
      {createContributors(dataset)}
      {createSubject(dataset)}
      {createSpatialPoints(dataset)}
      {createSpatialBoxes(dataset)}
      {createTemporal(dataset)}
    </ddm:dcmiMetadata>
    // @formatter:on
  }

  def simpleMetadataEntryToXML(key: MultiDepositKey, values: MultiDepositValues): List[Elem] = {
    values.filter(_.nonEmpty).map(elem(metadataFields.getOrElse(key, key)))
  }

  def elem(key: String)(value: String): Elem = {
    // @formatter:off
    <key>{value}</key>.copy(label=key)
    // @formatter:on
  }
}

object validators {

  def checkValidChars(row: Int, datasetRow: DatasetRow, key: String): Try[Unit] = {
    datasetRow.get(key)
      .map(value => "[^a-zA-Z0-9_-]".r.findAllIn(value).toSet)
      .map(illegalCharacters => if (illegalCharacters.isEmpty)
                                  Success(())
                                else
                                  Failure(ActionException(row, s"The column '$key' contains the following invalid characters: ${ illegalCharacters.map(s => s"'$s'").mkString("{ ", ", ", " }") }"))
      ).getOrElse(Success(()))
  }
  /**
   * Check if either non of the keys have values or all of them have values
   * If some are missing, mention them in the exception message
   */
  def checkAllOrNone(row: Int, datasetRow: DatasetRow, keys: String*): Try[Unit] = {
    val emptyVals = keys.filter(key => datasetRow.get(key).forall(_.isBlank))

    if (emptyVals.nonEmpty && emptyVals.size < keys.size)
      Failure(ActionException(row, s"Missing value(s) for: ${ emptyVals.mkString("[", ", ", "]") }"))
    else
      Success(())
  }

  /**
   * Check if only one of the keys contains a non-blank value
   */
  def checkOneOf(row: Int, map: scala.collection.Map[MultiDepositKey, String], keys: String*): Try[Unit] = {
    if (keys.map(map.get(_).filterNot(_.isBlank)).count(_.isDefined) == 1)
      Success(())
    else
      Failure(ActionException(row, s"Only one of the following columns must contain a value: ${ keys.mkString("[", ", ", "]") }"))
  }

  /**
   * Check if not all keys contain non-blank values
   */
  def checkNotAll(row: Int, map: scala.collection.Map[MultiDepositKey, String], keys: String*): Try[Unit] = {
    if (keys.map(map.get(_).filterNot(_.isBlank)).forall(_.isDefined))
      Failure(ActionException(row, s"The columns ${ keys.mkString("[", ", ", "]") } must not all contain a value at the same time"))
    else
      Success(())
  }

  /**
   * If any of the keys (optional and required) has a value all required keys should have a value
   */
  def checkRequiredWithGroup(row: Int, datasetRow: DatasetRow, requiredKeys: List[String], optionalKeys: List[String]): Try[Unit] = {
    val emptyOptionalVals = optionalKeys.filter(optionalKey => datasetRow.get(optionalKey).forall(_.isBlank))
    val emptyRequiredVals = requiredKeys.filter(requiredKey => datasetRow.get(requiredKey).forall(_.isBlank))

    // note that it has values if not all are empty
    val hasOptionalVals = emptyOptionalVals.size < optionalKeys.size
    val hasRequiredVals = emptyRequiredVals.size < requiredKeys.size

    if ((hasOptionalVals || hasRequiredVals) && emptyRequiredVals.nonEmpty)
      Failure(ActionException(row, s"Missing value(s) for: ${ emptyRequiredVals.mkString("[", ", ", "]") }"))
    else
      Success(())
  }

  /**
   * When it contains something, this should be from a list af allowed values
   */
  def checkValueIsOneOf(row: Int, datasetRow: DatasetRow, key: String, allowed: String*): Try[Unit] = {
    val value = datasetRow.getOrElse(key, "")
    if (value.isEmpty || allowed.contains(value))
      Success(Unit)
    else
      Failure(ActionException(row, s"Wrong value: $value should be empty or one of: ${ allowed.mkString("[", ", ", "]") }"))
  }

  /**
   * Check if a column in the dataset contains only one non-blank value
   */
  def checkColumnHasOnlyOneValue(row: Int, dataset: Dataset, key: String): Try[Unit] = {
    dataset.get(key)
      .map(_.filterNot(_.isBlank).size)
      .map {
        case 0 => Failure(ActionException(row, s"No value defined for $key"))
        case 1 => Success(())
        case _ => Failure(ActionException(row, s"More than one value is defined for $key"))
      }
      .getOrElse(Failure(ActionException(row, s"The column $key is not present in this instructions file")))
  }

  def checkColumnsHaveAtMostOneRowWithValues(row: Int, dataset: Dataset, keys: String*): Try[Unit] = {
    dataset.rowsWithValuesFor(keys: _*).size match {
      case 0 | 1 => Success(())
      case _ => Failure(ActionException(row, s"Only one row can contain values for all these columns: ${ keys.mkString("[", ", ", "]") }"))
    }
  }

  def checkAccessRights(row: Int, datasetRow: DatasetRow): Try[Unit] = {
    (datasetRow.get("DDM_ACCESSRIGHTS"), datasetRow.get("DDM_AUDIENCE")) match {
      case (Some("GROUP_ACCESS"), Some("D37000")) => Success(Unit)
      case (Some("GROUP_ACCESS"), Some(code)) => Failure(ActionException(row, s"When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeologie), but it is: $code"))
      case (Some("GROUP_ACCESS"), None) => Failure(ActionException(row, "When DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeologie), but it is not defined"))
      case (_, _) => Success(())
    }
  }

  def checkDateFormatting(row: Int, datasetRow: DatasetRow, key: String): Try[Unit] = {
    datasetRow.get(key)
      .map(date => Try { DateTime.parse(date) }.map(_ => ()).recoverWith {
        case _: IllegalArgumentException => Failure(ActionException(row, s"$key '$date' does not represent a date"))
      })
      .getOrElse(Success(()))
  }
}
