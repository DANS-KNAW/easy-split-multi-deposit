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

import java.nio.file.Paths
import java.util.UUID

import better.files.File
import better.files.File.currentWorkingDirectory
import cats.data.NonEmptyList
import cats.scalatest.{ EitherMatchers, EitherValues, ValidatedValues }
import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, OutputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model._
import org.apache.commons.configuration.PropertiesConfiguration
import org.joda.time.DateTime
import org.scalatest.{ Inside, OptionValues }
import org.scalatest.enablers.Existence
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

trait TestSupportFixture extends AnyFlatSpec with Matchers with OptionValues with EitherMatchers with EitherValues with ValidatedValues with Inside with InputPathExplorer with StagingPathExplorer with OutputPathExplorer {

  implicit def existenceOfFile[FILE <: better.files.File]: Existence[FILE] = _.exists

  lazy val testDir: File = {
    val path = currentWorkingDirectory / s"target/test/${ getClass.getSimpleName }"
    if (path.exists) path.delete()
    path.createDirectories()
    path
  }
  override val multiDepositDir: File = testDir / "md"
  override val stagingDir: File = testDir / "sd"
  override val outputDepositDir: File = testDir / "od"
  override val reportFile: File = testDir / "report.csv"

  implicit val inputPathExplorer: InputPathExplorer = this
  implicit val stagingPathExplorer: StagingPathExplorer = this
  implicit val outputPathExplorer: OutputPathExplorer = this

  val licensesDir = Paths.get("target/easy-licenses/licenses")
  val userLicenses: Set[MimeType] = new PropertiesConfiguration(licensesDir.resolve("licenses.properties").toFile)
    .getKeys.asScala.filterNot(_.isEmpty).toSet

  def testInstructions1: Instructions = {
    Instructions(
      depositId = "ruimtereis01",
      row = 2,
      depositorUserId = "ruimtereiziger1",
      profile = Profile(
        titles = NonEmptyList.of("Reis naar Centaur-planetoïde", "Trip to Centaur asteroid"),
        descriptions = NonEmptyList.of("Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.", "A two day mission to boldly go where no man has gone before"),
        creators = NonEmptyList.of(
          CreatorPerson(
            titles = Some("Captain"),
            initials = "J.T.",
            surname = "Kirk",
            organization = Some("United Federation of Planets")
          )
        ),
        created = DateTime.parse("2015-05-19"),
        audiences = NonEmptyList.of("D30000"),
        accessright = AccessCategory.OPEN_ACCESS
      ),
      baseUUID = Option(UUID.fromString("1de3f841-0f0d-048b-b3db-4b03ad4834d7")),
      metadata = Metadata(
        formats = List("video/mpeg", "text/plain"),
        languages = List("NL", "encoding=UTF-8"),
        subjects = List(Subject("astronomie"), Subject("ruimtevaart"), Subject("planetoïden")),
        rightsholder = NonEmptyList.one("Mr. Anderson"),
      ),
      files = Map(
        testDir / "md/ruimtereis01/reisverslag/centaur.mpg" -> FileDescriptor(2, Option("flyby of centaur")),
        testDir / "md/ruimtereis01/path/to/a/random/video/hubble.mpg" -> FileDescriptor(3, Option("video about the hubble space telescope")),
      ),
      audioVideo = AudioVideo(
        springfield = Option(Springfield("dans", "janvanmansum", "Jans-test-files", PlayMode.Menu)),
        avFiles = Map(
          testDir / "md/ruimtereis01/reisverslag/centaur.mpg" -> Set(
            SubtitlesFile(testDir / "md/ruimtereis01/reisverslag/centaur.srt", Option("en")),
            SubtitlesFile(testDir / "md/ruimtereis01/reisverslag/centaur-nederlands.srt", Option("nl"))
          )
        )
      )
    )
  }

  def testInstructions2: Instructions = {
    Instructions(
      depositId = "deposit-2",
      row = 5,
      depositorUserId = "ruimtereiziger2",
      profile = Profile(
        titles = NonEmptyList.of("Title 1 of deposit 2", "Title 2 of deposit 2"),
        descriptions = NonEmptyList.of("A sample deposit with a not very long description"),
        creators = NonEmptyList.of(CreatorOrganization("Creator A")),
        created = DateTime.now(),
        available = DateTime.parse("2016-07-30"),
        audiences = NonEmptyList.of("D37000"),
        accessright = AccessCategory.REQUEST_PERMISSION
      ),
      baseUUID = Option(UUID.fromString("1de3f841-0f0d-048b-b3db-4b03ad4834d7")),
      metadata = Metadata(
        contributors = List(ContributorOrganization("Contributor 1"), ContributorOrganization("Contributor 2")),
        subjects = List(Subject("subject 1", Option("abr:ABRcomplex")), Subject("subject 2"), Subject("subject 3")),
        publishers = List("publisher 1"),
        types = NonEmptyList.of(DcType.STILLIMAGE),
        identifiers = List(Identifier("id1234")),
        rightsholder = NonEmptyList.of("Neo"),
      ),
      files = Map(
        testDir / "md/ruimtereis02/path/to/images/Hubble_01.jpg" -> FileDescriptor(5, Some("Hubble"), Some(FileAccessRights.RESTRICTED_REQUEST))
      )
    )
  }
}
