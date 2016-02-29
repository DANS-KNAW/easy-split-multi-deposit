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
  type Dictionary = Map[String, String]

  val profileFields: Dictionary =
    Map("DC_TITLE" -> "dc:title",
      "DC_DESCRIPTION" -> "dcterms:description",
      "DC_CREATOR" -> "dc:creator",
      "DDM_CREATED" -> "ddm:created",
      "DDM_AUDIENCE" -> "ddm:audience",
      "DDM_ACCESSRIGHTS" -> "ddm:accessRights")

  val metadataFields: Dictionary =
    Map("DDM_AVAILABLE" -> "ddm:available",
      "DC_CONTRIBUTOR" -> "dc:contributor",
      "DCT_ALTERNATIVE" -> "dcterms:alternative",
      "DC_SUBJECT" -> "dc:subject",
      "DC_PUBLISHER" -> "dcterms:publisher",
      "DC_TYPE" -> "dcterms:type",
      "DC_FORMAT" -> "dc:format",
      "DC_IDENTIFIER" -> "dc:identifier",
      "DC_SOURCE" -> "dc:source",
      "DC_LANGUAGE" -> "dc:language",
      "DCT_SPATIAL" -> "dcterms:spatial",
      "DCT_TEMPORAL" -> "dcterms:temporal",
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

  // TODO not sure about the values of this Dictionary
  val composedSpacialFields: Dictionary =
    Map("DCX_SPATIAL_SCHEME" -> "",
      "DCX_SPATIAL_X" -> "",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SOUTH" -> "",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_WEST" -> "")

  // TODO not sure about the values of this Dictionary
  val composedRelationFields: Dictionary =
    Map("DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_TITLE" -> "",
      "DCX_RELATION_LINK" -> "")

  // TODO not sure about some of the fields, see inline:
  val filesFields: Dictionary =
    Map("DC_CONTRIBUTER" -> "dcterms:contributer",
      // coverage
      "DC_CREATOR" -> "dcterms:creator",
      // date
      "DC_DESCRIPTION" -> "dcterms:description",
      "DC_FORMAT" -> "dcterms:format",
      "DC_IDENTIFIER" -> "dcterms:identifier",
      "DC_LANGUAGE" -> "dcterms:language",
      "DC_PUBLISHER" -> "dcterms:publisher",
      // relation
      // rights
      "DC_SOURCE" -> "dcterms:source",
      "DC_SUBJECT" -> "dcterms:subject",
      "DC_TITLE" -> "dcterms:title",
      "DC_TYPE" -> "dcterms:type")

  val allFields = "ROW" :: "DATASET" ::
    List(profileFields, metadataFields, composedCreatorFields, composedContributorFields, composedSpacialFields, composedRelationFields)
      .flatMap(_.keySet)
}
