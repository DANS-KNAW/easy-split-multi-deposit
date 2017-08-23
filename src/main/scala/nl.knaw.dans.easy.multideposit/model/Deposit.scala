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

import java.nio.file.Path

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.PlayMode.PlayMode
import org.joda.time.DateTime

case class Deposit(depositId: DepositId,
                   row: Int,
                   depositorUserId: DepositorUserId,
                   profile: Profile,
                   metadata: Metadata = Metadata(),
                   files: Map[Path, FileDescriptor] = Map.empty,
                   audioVideo: AudioVideo = AudioVideo())

// Profile
case class Profile(titles: NonEmptyList[String],
                   descriptions: NonEmptyList[String],
                   creators: NonEmptyList[Creator],
                   created: DateTime,
                   available: DateTime = DateTime.now(),
                   audiences: NonEmptyList[String], // or List[enum values]?
                   accessright: AccessCategory) // only one allowed? not yet in validation

sealed abstract class Creator
case class CreatorOrganization(organization: String) extends Creator
case class CreatorPerson(titles: Option[String] = Option.empty,
                         initials: String,
                         insertions: Option[String] = Option.empty,
                         surname: String,
                         organization: Option[String] = Option.empty,
                         dai: Option[String] = Option.empty) extends Creator

// Metadata
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
                    dates: List[Date] = List.empty,
                    contributors: List[Contributor] = List.empty,
                    subjects: List[Subject] = List.empty,
                    spatialPoints: List[SpatialPoint] = List.empty,
                    spatialBoxes: List[SpatialBox] = List.empty,
                    temporal: List[Temporal] = List.empty)

case class Identifier(id: String, idType: Option[IdentifierType.Value] = Option.empty)

sealed abstract class Relation
case class QualifiedRelation(qualifier: RelationQualifier.Value,
                             link: Option[String] = Option.empty,
                             title: Option[String] = Option.empty) extends Relation {
  require(link.isDefined || title.isDefined, "at least one of [link, title] must be filled in")
}
case class UnqualifiedRelation(link: Option[String] = Option.empty,
                               title: Option[String] = Option.empty) extends Relation {
  require(link.isDefined || title.isDefined, "at least one of [link, title] must be filled in")
}

sealed abstract class Date
case class QualifiedDate(date: DateTime, qualifier: DateQualifier.Value) extends Date
case class TextualDate(text: String) extends Date

sealed abstract class Contributor
case class ContributorOrganization(organization: String) extends Contributor
case class ContributorPerson(titles: Option[String] = Option.empty,
                             initials: String,
                             insertions: Option[String] = Option.empty,
                             surname: String,
                             organization: Option[String] = Option.empty,
                             dai: Option[String] = Option.empty) extends Contributor

case class Subject(subject: String = "", scheme: Option[String] = Option.empty)

case class SpatialPoint(x: String, y: String, scheme: Option[String] = Option.empty)

case class SpatialBox(north: String,
                      south: String,
                      east: String,
                      west: String,
                      scheme: Option[String] = Option.empty)

case class Temporal(temporal: String = "", scheme: Option[String] = Option.empty)

// files
case class FileDescriptor(title: Option[String] = Option.empty,
                          accessibility: Option[FileAccessRights.Value] = Option.empty)

// Audio/Video
case class AudioVideo(springfield: Option[Springfield] = Option.empty,
                      avFiles: Map[Path, Set[Subtitles]] = Map.empty)

case class Springfield(domain: String = "dans",
                       user: String,
                       collection: String,
                       playMode: PlayMode = PlayMode.Continuous)

case class Subtitles(path: Path, language: Option[String] = Option.empty)
