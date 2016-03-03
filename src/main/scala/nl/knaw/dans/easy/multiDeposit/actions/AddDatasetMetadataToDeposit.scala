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

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, PrettyPrinter}

case class AddDatasetMetadataToDeposit(row: Int, dataset: (DatasetID, Dataset))(implicit settings: Settings) extends Action {

  val log = LogFactory.getLog(getClass)

  def checkPreconditions = Success(Unit)

  def run() = {
    log.debug(s"Running $this")

    writeDatasetMetadataXml(row, dataset._1, dataset._2)
  }

  def rollback() = Success(Unit)
}
object AddDatasetMetadataToDeposit {
  def writeDatasetMetadataXml(row: Int, datasetID: DatasetID, dataset: Dataset)(implicit settings: Settings): Try[Unit] = {
    Try {
      outputDatasetMetadataFile(settings, datasetID).write(datasetToXml(dataset))
    } recoverWith {
      case e => Failure(ActionException(row, s"Could not write dataset metadata: $e"))
    }
  }

  def isPartOfProfile(key: MultiDepositKey)             = profileFields.contains(key)
  def isPartOfMetadata(key: MultiDepositKey)            = metadataFields.contains(key)
  def isPartOfComposedCreator(key: MultiDepositKey)     = composedCreatorFields.contains(key)
  def isPartOfComposedContributor(key: MultiDepositKey) = composedContributorFields.contains(key)

  def datasetToXml(dataset: Dataset) = {
    new PrettyPrinter(160, 2).format(
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
      </ddm:DDM>)
  }

  def createProfile(dataset: Dataset) = {
    <ddm:profile>
      {profileElems(dataset, "DC_TITLE")}
      {profileElems(dataset, "DC_DESCRIPTION")}
      {createComposedCreators(dataset)}
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
      .map(value => <key>{value}</key>.copy(label=profileFields.getOrElse(key, key)))
  }

  def createComposedCreators(dataset: Dataset) =
    createComposedAuthors(dataset, isPartOfComposedCreator, createComposedCreator(composedCreatorFields, _))

  def createComposedContributors(dataset: Dataset) =
    createComposedAuthors(dataset, isPartOfComposedContributor, createComposedContributor(composedContributorFields, _))

  def createComposedAuthors(dataset: Dataset, isPartOfAuthor: (MultiDepositKey => Boolean), createAuthor: Iterable[(MultiDepositKey, String)] => Elem) = {
    val authorsData = dataset.filter(x => isPartOfAuthor(x._1))

    if(authorsData.isEmpty)
      Seq.empty
    else
      authorsData.values.head.indices
        .map(i => authorsData.map { case (key, values) => (key, values(i)) })
        .filter(_.values.exists(x => x != null && !x.isBlank))
        .map(createAuthor)
  }

  def createComposedCreator(dictionary: Dictionary, authorFields: Iterable[(MultiDepositKey, String)]) = {
    <dcx-dai:creatorDetails>
      {
      if (isOrganization(authorFields))
        <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">
            { authorFields.find(field => isOrganizationKey(field._1)).map(_._2).getOrElse("") }
          </dcx-dai:name>
        </dcx-dai:organization>
      else
        <dcx-dai:author>
          { authorFields.map(composedEntry(dictionary)) }
        </dcx-dai:author>
      }
    </dcx-dai:creatorDetails>
  }

  def isOrganization(authorFields: Iterable[(MultiDepositKey, String)]): Boolean = {
    val othersEmpty = authorFields
      .filterNot(field => isOrganizationKey(field._1))
      .forall(_._2 == "")
    val hasOrganization = authorFields.toList.exists(field => isOrganizationKey(field._1))
    othersEmpty && hasOrganization
  }

  def isOrganizationKey(key: MultiDepositKey) = key match {
    case "DCX_CREATOR_ORGANIZATION" => true
    case "DCX_CONTRIBUTOR_ORGANIZATION" => true
    case _ => false
  }

  def createComposedContributor(dictionary: Dictionary, authorFields: Iterable[(MultiDepositKey, String)]) = {
    <dcx-dai:contributorDetails>
      <dcx-dai:author>
        {authorFields.filter(x => x._2 != null && !x._2.isBlank).map(composedEntry(dictionary))}
      </dcx-dai:author>
    </dcx-dai:contributorDetails>
  }

  def composedEntry(dictionary: Dictionary)(entry: (MultiDepositKey, String)) = {
    if (entry._1.endsWith("_ORGANIZATION")) {
      <dcx-dai:organization>
        <dcx-dai:name xml:lang="en">{entry._2}</dcx-dai:name>
      </dcx-dai:organization>
    } else {
      <key>{entry._2}</key>.copy(label=dictionary.getOrElse(entry._1, entry._1))
    }
  }

  def createMetadata(dataset: Dataset) = {
    <ddm:dcmiMetadata>
      {dataset.filter(kv => isPartOfMetadata(kv._1) && kv._2.nonEmpty)
      .flatMap { case (key, values) => simpleMetadataEntryToXML(key, values) }}
      {createComposedContributors(dataset)}
    </ddm:dcmiMetadata>
  }

  def simpleMetadataEntryToXML(key: MultiDepositKey, values: MultiDepositValues) = {
    values.filter(_.nonEmpty)
      .map(value => <key>{value}</key>.copy(label=metadataFields.getOrElse(key, key)))
  }
}
