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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

object Headers extends Enumeration with DebugEnhancedLogging {
  type Header = Value

  // profile fields
  val Title: Header = Value("DC_TITLE")
  val Description: Header = Value("DC_DESCRIPTION")
  val Creator: Header = Value("DC_CREATOR")
  val Created: Header = Value("DDM_CREATED")
  val Available: Header = Value("DDM_AVAILABLE")
  val Audience: Header = Value("DDM_AUDIENCE")
  val AccessRights: Header = Value("DDM_ACCESSRIGHTS")

  // metadata fields
  val Contributor: Header = Value("DC_CONTRIBUTOR")
  val AlternativeTitle: Header = Value("DCT_ALTERNATIVE")
  val Publisher: Header = Value("DC_PUBLISHER")
  val Type: Header = Value("DC_TYPE")
  val Format: Header = Value("DC_FORMAT")
  val Identifier: Header = Value("DC_IDENTIFIER")
  val IdentifierType: Header = Value("DC_IDENTIFIER_TYPE")
  val Source: Header = Value("DC_SOURCE")
  val Language: Header = Value("DC_LANGUAGE")
  val SchemeSpatial: Header = Value("DCT_SPATIAL_SCHEME")
  val Spatial: Header = Value("DCT_SPATIAL")
  val Rightsholder: Header = Value("DCT_RIGHTSHOLDER")
  val License: Header = Value("DCT_LICENSE")

  // composed creator fields
  val CreatorTitles: Header = Value("DCX_CREATOR_TITLES")
  val CreatorInitials: Header = Value("DCX_CREATOR_INITIALS")
  val CreatorInsertions: Header = Value("DCX_CREATOR_INSERTIONS")
  val CreatorSurname: Header = Value("DCX_CREATOR_SURNAME")
  val CreatorDAI: Header = Value("DCX_CREATOR_DAI")
  val CreatorOrganization: Header = Value("DCX_CREATOR_ORGANIZATION")
  val CreatorRole: Header = Value("DCX_CREATOR_ROLE")

  // composed contributor fields
  val ContributorTitles: Header = Value("DCX_CONTRIBUTOR_TITLES")
  val ContributorInitials: Header = Value("DCX_CONTRIBUTOR_INITIALS")
  val ContributorInsertions: Header = Value("DCX_CONTRIBUTOR_INSERTIONS")
  val ContributorSurname: Header = Value("DCX_CONTRIBUTOR_SURNAME")
  val ContributorDAI: Header = Value("DCX_CONTRIBUTOR_DAI")
  val ContributorOrganization: Header = Value("DCX_CONTRIBUTOR_ORGANIZATION")
  val ContributorRole: Header = Value("DCX_CONTRIBUTOR_ROLE")

  // spatial point fields
  val SpatialScheme: Header = Value("DCX_SPATIAL_SCHEME")
  val SpatialX: Header = Value("DCX_SPATIAL_X")
  val SpatialY: Header = Value("DCX_SPATIAL_Y")

  // spatial box fields
  val SpatialNorth: Header = Value("DCX_SPATIAL_NORTH")
  val SpatialSouth: Header = Value("DCX_SPATIAL_SOUTH")
  val SpatialEast: Header = Value("DCX_SPATIAL_EAST")
  val SpatialWest: Header = Value("DCX_SPATIAL_WEST")

  // relation fields
  val RelationQualifier: Header = Value("DCX_RELATION_QUALIFIER")
  val RelationTitle: Header = Value("DCX_RELATION_TITLE")
  val RelationLink: Header = Value("DCX_RELATION_LINK")

  // date fields
  val Date: Header = Value("DCT_DATE")
  val DateQualifier: Header = Value("DCT_DATE_QUALIFIER")

  // temporal fields
  val Temporal: Header = Value("DCT_TEMPORAL")
  val TemporalScheme: Header = Value("DCT_TEMPORAL_SCHEME")

  // subject fields
  val Subject: Header = Value("DC_SUBJECT")
  val SubjectScheme: Header = Value("DC_SUBJECT_SCHEME")

  // file fields
  val FilePath: Header = Value("FILE_PATH")
  val FileTitle: Header = Value("FILE_TITLE")
  val FileAccessibility: Header = Value("FILE_ACCESSIBILITY")
  val FileVisibility: Header = Value("FILE_VISIBILITY")

  // springfield fields
  val SpringfieldDomain: Header = Value("SF_DOMAIN")
  val SpringfieldUser: Header = Value("SF_USER")
  val SpringfieldCollection: Header = Value("SF_COLLECTION")
  val SpringfieldPlayMode: Header = Value("SF_PLAY_MODE")

  // A/V fields
  val AudioVideoFilePath: Header = Value("AV_FILE_PATH")
  val AudioVideoSubtitles: Header = Value("AV_SUBTITLES")
  val AudioVideoSubtitlesLanguage: Header = Value("AV_SUBTITLES_LANGUAGE")

  // administrative fields
  val Row: Header = Value("ROW")
  val Dataset: Header = Value("DATASET")
  val DepositorId: Header = Value("DEPOSITOR_ID")
  val BaseRevision: Header = Value("BASE_REVISION")

  logger.debug(s"VALID HEADERS $values")
}
