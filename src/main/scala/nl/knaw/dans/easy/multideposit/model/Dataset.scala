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
package nl.knaw.dans.easy.multideposit.model

import java.io.File

import nl.knaw.dans.common.lang.dataset.AccessCategory
import org.joda.time.DateTime

case class Dataset(datasetId: DatasetId,
                   row: Int,
                   depositorId: DepositorId,
                   profile: Profile,
                   metadata: Metadata = Metadata(),
                   audioVideo: AudioVideo = AudioVideo())

case class Profile(titles: NonEmptyList[String],
                   descriptions: NonEmptyList[String],
                   creators: NonEmptyList[Creator],
                   created: DateTime,
                   available: DateTime = DateTime.now(),
                   audiences: NonEmptyList[String], // or List[enum values]?
                   accessright: AccessCategory) // only one allowed? not yet in validation

case class Metadata(alternatives: List[String] = List.empty,
                    publishers: List[String] = List.empty,
                    types: NonEmptyList[DcType.Value] = List(DcType.DATASET),
                    formats: List[String] = List.empty,
                    identifiers: List[Identifier] = List.empty,
                    sources: List[String] = List.empty,
                    languages: List[String] = List.empty,
                    spatials: List[String] = List.empty,
                    rightsholder: List[String] = List.empty,
                    relations: List[Relation] = List.empty,
                    contributors: List[Contributor] = List.empty,
                    subjects: List[Subject] = List.empty,
                    spatialPoints: List[SpatialPoint] = List.empty,
                    spatialBoxes: List[SpatialBox] = List.empty,
                    temporal: List[Temporal] = List.empty)

case class AudioVideo(springfield: Option[Springfield] = Option.empty,
                      accessibility: Option[FileAccessRights.Value] = Option.empty,
                      avFiles: Set[AVFile] = Set.empty)

sealed abstract class Creator
case class CreatorOrganization(organization: String) extends Creator
case class CreatorPerson(titles: Option[String] = Option.empty,
                         initials: String,
                         insertions: Option[String] = Option.empty,
                         surname: String,
                         organization: Option[String] = Option.empty,
                         dai: Option[String] = Option.empty) extends Creator

sealed abstract class Contributor
case class ContributorOrganization(organization: String) extends Contributor
case class ContributorPerson(titles: Option[String] = Option.empty,
                             initials: String,
                             insertions: Option[String] = Option.empty,
                             surname: String,
                             organization: Option[String] = Option.empty,
                             dai: Option[String] = Option.empty) extends Contributor

case class Identifier(id: String, idType: Option[IdentifierType.Value] = Option.empty)

sealed abstract class Relation
case class QualifiedLinkRelation(qualifier: String, link: String) extends Relation
case class QualifiedTitleRelation(qualifier: String, title: String) extends Relation
case class LinkRelation(link: String) extends Relation
case class TitleRelation(title: String) extends Relation

case class Subject(subject: String = "", scheme: Option[String] = Option.empty)

case class Temporal(temporal: String = "", scheme: Option[String] = Option.empty)

case class SpatialPoint(x: String, y: String, scheme: Option[String] = Option.empty)

case class SpatialBox(north: String, south: String, east: String, west: String, scheme: Option[String] = Option.empty)

case class Springfield(domain: String = "dans", user: String, collection: String)

case class Subtitles(file: File, language: Option[String] = Option.empty)

case class AVFile(file: File, title: Option[String] = Option.empty, subtitles: Seq[Subtitles] = Seq.empty)
