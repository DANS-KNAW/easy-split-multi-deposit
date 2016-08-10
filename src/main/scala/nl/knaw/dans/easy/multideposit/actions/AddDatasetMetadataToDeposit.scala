/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
import org.apache.commons.logging.LogFactory

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.{Failure, Try}
import scala.xml.Elem

case class AddDatasetMetadataToDeposit(row: Int, dataset: (DatasetID, Dataset))(implicit settings: Settings) extends Action {

  val log = LogFactory.getLog(getClass)

  // TODO preconditions for verifying the metadata from `dataset` in future release

  def run() = {
    log.debug(s"Running $this")

    writeDatasetMetadataXml(row, dataset._1, dataset._2)
  }
}
object AddDatasetMetadataToDeposit {
  def writeDatasetMetadataXml(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    Try {
      outputDatasetMetadataFile(settings, datasetID).writeXml(datasetToXml(dataset))
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write dataset metadata: $e", e))
    }
  }

  def datasetToXml(dataset: Dataset): Elem = {
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
  }

  def createProfile(dataset: Dataset) = {
    <ddm:profile>
      {profileElems(dataset, "DC_TITLE")}
      {profileElems(dataset, "DC_DESCRIPTION")}
      {createCreators(dataset)}
      {profileElems(dataset, "DDM_CREATED")}
      {profileElems(dataset, "DDM_AUDIENCE")}
      {profileElems(dataset, "DDM_ACCESSRIGHTS")}
    </ddm:profile>
  }

  def profileElems(dataset: Dataset, key: MultiDepositKey) = {
    elemsFromKeyValues(key, dataset.getOrElse(key, List()))
  }

  def elemsFromKeyValues(key: MultiDepositKey, values: MultiDepositValues) = {
    values.filter(_.nonEmpty)
      .map(elem(profileFields.getOrElse(key, key)))
  }

  def createCreators(dataset: Dataset) = {
    dataset.rowsWithValuesFor(composedCreatorFields).map(mdKeyValues =>
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
    )
  }

  def isOrganization(authorFields: Iterable[(MultiDepositKey, String)]): Boolean = {
    val othersEmpty = authorFields
      .filterNot(field => organizationKeys.contains(field._1))
      .forall(_._2 == "")
    val hasOrganization = authorFields.toList.exists(field => organizationKeys.contains(field._1))
    othersEmpty && hasOrganization
  }

  def createContributors(dataset: Dataset) = {
    dataset.rowsWithValuesFor(composedContributorFields).map(mdKeyValues =>
      <dcx-dai:contributorDetails>{
        if (isOrganization(mdKeyValues))
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">{mdKeyValues.find(field => organizationKeys.contains(field._1)).map(_._2).getOrElse("")}</dcx-dai:name>
        </dcx-dai:organization>
      else
        <dcx-dai:author>
          {mdKeyValues.map(composedEntry(composedContributorFields))}
        </dcx-dai:author>
      }</dcx-dai:contributorDetails>
    )
  }

  def composedEntry(dictionary: Dictionary)(entry: (MultiDepositKey, String)) = {
    val (key, value) = entry
    if (organizationKeys.contains(key)) {
      <dcx-dai:organization>
        <dcx-dai:name xml:lang="en">{value}</dcx-dai:name>
      </dcx-dai:organization>
    }
    else {
      elem(dictionary.getOrElse(key, key))(value)
    }
  }

  def createSrsName(fields: mutable.HashMap[MultiDepositKey, String]) = Map(
    "degrees" -> "http://www.opengis.net/def/crs/EPSG/0/4326",
    "RD" -> "http://www.opengis.net/def/crs/EPSG/0/28992"
  ).getOrElse(fields.getOrElse("DCX_SPATIAL_SCHEME", ""),"")

  def createSpatialPoints(dataset: Dataset) = {
    dataset.rowsWithValuesForAllOf(composedSpatialPointFields).map(mdKeyValues =>
      <dcx-gml:spatial srsName={createSrsName(mdKeyValues)}>
        <Point xmlns="http://www.opengis.net/gml">
          <pos>{mdKeyValues.getOrElse("DCX_SPATIAL_Y", "")} {mdKeyValues.getOrElse("DCX_SPATIAL_X", "")}</pos>
        </Point>
      </dcx-gml:spatial>
    )
  }

  def createSpatialBoxes(dataset: Dataset) = {
    dataset.rowsWithValuesForAllOf(composedSpatialBoxFields).map(mdKeyValues =>
      <dcx-gml:spatial>
        <boundedBy xmlns="http://www.opengis.net/gml">
          <Envelope srsName={createSrsName(mdKeyValues)}>
            <lowerCorner>{mdKeyValues.getOrElse("DCX_SPATIAL_NORTH", "")} {mdKeyValues.getOrElse("DCX_SPATIAL_EAST", "")}</lowerCorner>
            <upperCorner>{mdKeyValues.getOrElse("DCX_SPATIAL_SOUTH", "")} {mdKeyValues.getOrElse("DCX_SPATIAL_WEST", "")}</upperCorner>
          </Envelope>
        </boundedBy>
      </dcx-gml:spatial>
    )
  }

  def createRelations(dataset: Dataset) = {
    dataset.rowsWithValuesFor(composedRelationFields).map { row =>
      (row.get("DCX_RELATION_QUALIFIER"), row.get("DCX_RELATION_LINK"), row.get("DCX_RELATION_TITLE")) match {
        case (Some(q), Some(l),_      ) => elem(s"dcterms:$q")(l)
        case (Some(q), None,   Some(t)) => elem(s"dcterms:$q")(t)
        case (None,    Some(l),_      ) => elem(s"dc:relation")(l)
        case (None,    None,   Some(t)) => elem(s"dc:relation")(t)
        case _                          =>
          // TODO this case needs to be checked to not occur in the preconditions
          // (see also comment in https://github.com/DANS-KNAW/easy-split-multi-deposit/commit/dbda6cc2b78f93196be62b323a988e3781cb6926#diff-efd2dc8d9655ba9c6b577f13dd66627bR32)
          throw new IllegalArgumentException("preconditions should have reported this as an error")
      }
    }
  }

  def createMetadata(dataset: Dataset) = {
    def isMetaData(key: MultiDepositKey, values: MultiDepositValues): Boolean = {
      metadataFields.contains(key) && values.nonEmpty
    }

    <ddm:dcmiMetadata>
      {dataset.filter(isMetaData _ tupled).flatMap(simpleMetadataEntryToXML _ tupled)}
      {createRelations(dataset)}
      {createContributors(dataset)}
      {createSpatialPoints(dataset)}
      {createSpatialBoxes(dataset)}
    </ddm:dcmiMetadata>
  }

  def simpleMetadataEntryToXML(key: MultiDepositKey, values: MultiDepositValues): List[Elem] = {
    values.filter(_.nonEmpty).map(elem(metadataFields.getOrElse(key, key)))
  }

  def elem(key: String)(value: String) = <key>{value}</key>.copy(label=key)
}
