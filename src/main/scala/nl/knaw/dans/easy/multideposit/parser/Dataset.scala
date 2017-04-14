package nl.knaw.dans.easy.multideposit.parser

import java.io.File

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.actions.FileAccessRights
import org.joda.time.DateTime

case class Dataset(id: DatasetID,
                   row: Int,
                   depositorID: String,
                   profile: Profile,
                   metadata: Metadata,
                   audioVideo: Option[AudioVideo])

case class Profile(titles: NonEmptyList[String],
                   descriptions: NonEmptyList[String],
                   creators: NonEmptyList[Creator],
                   created: DateTime,
                   available: DateTime = DateTime.now(),
                   audiences: NonEmptyList[String], // or List[enum values]?
                   accessright: AccessCategory) // only one allowed? not yet in validation

case class Metadata(alternatives: List[String],
                    publishers: List[String],
                    types: List[String],
                    formats: List[String],
                    identifiers: List[String],
                    sources: List[String],
                    languages: List[String],
                    spatials: List[String],
                    rightsholder: List[String],
                    relations: List[Relation],
                    contributors: List[Contributor],
                    subjects: List[Subject],
                    spatialPoints: List[SpatialPoint],
                    spatialBoxes: List[SpatialBox],
                    temporal: List[Temporal])

case class AudioVideo(springfield: Springfield,
                      accessibility: Option[FileAccessRights.Value],
                      avFiles: Map[File, List[AVFile]])

sealed abstract class Creator
case class CreatorOrganization(organization: String) extends Creator
case class CreatorPerson(titles: Option[String],
                         initials: String,
                         insertions: Option[String],
                         surname: String,
                         organization: Option[String],
                         dai: Option[String]) extends Creator

sealed abstract class Contributor
case class ContributorOrganization(organization: String) extends Contributor
case class ContributorPerson(titles: Option[String],
                             initials: String,
                             insertions: Option[String],
                             surname: String,
                             organization: Option[String],
                             dai: Option[String]) extends Contributor

sealed abstract class Relation
case class QualifiedLinkRelation(qualifier: String, link: String) extends Relation
case class QualifiedTitleRelation(qualifier: String, title: String) extends Relation
case class LinkRelation(link: String) extends Relation
case class TitleRelation(title: String) extends Relation

case class Subject(scheme: Option[String], subject: String = "")

case class Temporal(scheme: Option[String], temporal: String = "")

case class SpatialPoint(x: String, y: String, scheme: Option[String])

case class SpatialBox(north: String, south: String, east: String, west: String, scheme: Option[String])

case class Springfield(user: String, collection: String, domain: String = "dans")

case class Subtitles(file: File, language: Option[String])

case class AVFile(file: File, title: Option[String], subtitles: Option[Subtitles])
