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
package nl.knaw.dans.easy.multideposit

object Headers {

  lazy val validHeaders: Set[String] = {
    administrativeHeaders ++
      profileFields ++
      metadataFields ++
      composedCreatorFields ++
      composedContributorFields ++
      composedSpatialPointFields ++
      composedSpatialBoxFields ++
      composedRelationFields ++
      composedDateFields ++
      composedTemporalFields ++
      composedSubjectFields ++
      fileDescriptorHeaders ++
      springfieldHeaders ++
      audioVideoHeaders
  }

  private lazy val profileFields = Set(
    "DC_TITLE",
    "DC_DESCRIPTION",
    "DC_CREATOR",
    "DDM_CREATED",
    "DDM_AVAILABLE",
    "DDM_AUDIENCE",
    "DDM_ACCESSRIGHTS"
  )

  private lazy val metadataFields = Set(
    "DC_CONTRIBUTOR",
    "DCT_ALTERNATIVE",
    "DC_PUBLISHER",
    "DC_TYPE",
    "DC_FORMAT",
    "DC_IDENTIFIER",
    "DC_IDENTIFIER_TYPE",
    "DC_SOURCE",
    "DC_LANGUAGE",
    "DCT_SPATIAL",
    "DCT_RIGHTSHOLDER"
  )

  private lazy val composedCreatorFields = Set(
    "DCX_CREATOR_TITLES",
    "DCX_CREATOR_INITIALS",
    "DCX_CREATOR_INSERTIONS",
    "DCX_CREATOR_SURNAME",
    "DCX_CREATOR_DAI",
    "DCX_CREATOR_ORGANIZATION",
    "DCX_CREATOR_ROLE"
  )

  private lazy val composedContributorFields = Set(
    "DCX_CONTRIBUTOR_TITLES",
    "DCX_CONTRIBUTOR_INITIALS",
    "DCX_CONTRIBUTOR_INSERTIONS",
    "DCX_CONTRIBUTOR_SURNAME",
    "DCX_CONTRIBUTOR_DAI",
    "DCX_CONTRIBUTOR_ORGANIZATION",
    "DCX_CONTRIBUTOR_ROLE"
  )

  private lazy val composedSpatialPointFields = Set(
    "DCX_SPATIAL_SCHEME",
    "DCX_SPATIAL_X",
    "DCX_SPATIAL_Y"
  )

  private lazy val composedSpatialBoxFields = Set(
    "DCX_SPATIAL_SCHEME",
    "DCX_SPATIAL_NORTH",
    "DCX_SPATIAL_SOUTH",
    "DCX_SPATIAL_EAST",
    "DCX_SPATIAL_WEST"
  )

  private lazy val composedRelationFields = Set(
    "DCX_RELATION_QUALIFIER",
    "DCX_RELATION_TITLE",
    "DCX_RELATION_LINK"
  )

  private lazy val composedDateFields = Set(
    "DCT_DATE",
    "DCT_DATE_QUALIFIER"
  )

  private lazy val composedTemporalFields = Set(
    "DCT_TEMPORAL",
    "DCT_TEMPORAL_SCHEME"
  )

  private lazy val composedSubjectFields = Set(
    "DC_SUBJECT",
    "DC_SUBJECT_SCHEME"
  )

  private lazy val fileDescriptorHeaders = Set(
    "FILE_PATH",
    "FILE_TITLE",
    "FILE_ACCESSIBILITY"
  )

  private lazy val springfieldHeaders = Set(
    "SF_DOMAIN",
    "SF_USER",
    "SF_COLLECTION",
    "SF_PLAY_MODE"
  )

  private lazy val audioVideoHeaders = Set(
    "AV_FILE_PATH",
    "AV_SUBTITLES",
    "AV_SUBTITLES_LANGUAGE"
  )

  private lazy val administrativeHeaders = Set(
    "ROW",
    "DATASET",
    "DEPOSITOR_ID"
  )
}
