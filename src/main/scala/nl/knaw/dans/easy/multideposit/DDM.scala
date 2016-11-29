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
package nl.knaw.dans.easy.multideposit

object DDM {
  type Dictionary = Map[MultiDepositKey, String]

  val profileFields: Dictionary =
    Map("DC_TITLE" -> "dc:title",
      "DC_DESCRIPTION" -> "dcterms:description",
      "DC_CREATOR" -> "dc:creator", // TODO verwijder
      "DDM_CREATED" -> "ddm:created",
      "DDM_AUDIENCE" -> "ddm:audience",
      "DDM_ACCESSRIGHTS" -> "ddm:accessRights")

  val metadataFields: Dictionary =
    Map("DDM_AVAILABLE" -> "ddm:available",
      "DC_CONTRIBUTOR" -> "dc:contributor", // TODO verwijder
      "DCT_ALTERNATIVE" -> "dcterms:alternative",
//      "DC_SUBJECT" -> "dc:subject",
      "DC_PUBLISHER" -> "dcterms:publisher",
      "DC_TYPE" -> "dcterms:type",
      "DC_FORMAT" -> "dc:format",
      "DC_IDENTIFIER" -> "dc:identifier",
      "DC_SOURCE" -> "dc:source",
      "DC_LANGUAGE" -> "dc:language",
      "DCT_SPATIAL" -> "dcterms:spatial",
      "DCT_RIGHTSHOLDER" -> "dcterms:rightsHolder")

  val composedCreatorFields: Dictionary =
    Map("DCX_CREATOR_TITLES" -> "dcx-dai:titles",
      "DCX_CREATOR_INITIALS" -> "dcx-dai:initials",
      "DCX_CREATOR_INSERTIONS" -> "dcx-dai:insertions",
      "DCX_CREATOR_SURNAME" -> "dcx-dai:surname",
      "DCX_CREATOR_DAI" -> "dcx-dai:DAI",
      "DCX_CREATOR_ORGANIZATION" -> "dcx-dai:name xml:lang=\"en\"")

  val composedContributorFields: Dictionary =
    Map("DCX_CONTRIBUTOR_TITLES" -> "dcx-dai:titles",
      "DCX_CONTRIBUTOR_INITIALS" -> "dcx-dai:initials",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "dcx-dai:insertions",
      "DCX_CONTRIBUTOR_SURNAME" -> "dcx-dai:surname",
      "DCX_CONTRIBUTOR_DAI" -> "dcx-dai:DAI",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "dcx-dai:name xml:lang=\"en\"")

  val composedSpatialPointFields: Dictionary =
    Map("DCX_SPATIAL_SCHEME" -> "",
      "DCX_SPATIAL_X" -> "",
      "DCX_SPATIAL_Y" -> "")

  val composedSpatialBoxFields: Dictionary =
    Map("DCX_SPATIAL_SCHEME" -> "",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SOUTH" -> "",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_WEST" -> "")

  // TODO not sure about the values of this Dictionary
  val composedRelationFields: Dictionary =
    Map("DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_TITLE" -> "",
      "DCX_RELATION_LINK" -> "")

  val composedTemporalFields: Dictionary =
    Map("DCT_TEMPORAL" -> "dcterms:temporal",
      "DCT_TEMPORAL_SCHEME" -> "")

  val composedSubjectFields: Dictionary =
    Map("DC_SUBJECT" -> "dc:subject",
      "DC_SUBJECT_SCHEME" -> "")

  val organizationKeys = Set("DCX_CREATOR_ORGANIZATION", "DCX_CONTRIBUTOR_ORGANIZATION")

  val allFields = "ROW" :: "DATASET" ::
    List(profileFields, metadataFields, composedCreatorFields, composedContributorFields, composedSpatialPointFields, composedSpatialBoxFields, composedRelationFields, composedTemporalFields, composedSubjectFields)
      .flatMap(_.keySet)
}
