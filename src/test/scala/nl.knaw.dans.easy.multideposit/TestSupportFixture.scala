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

import java.nio.file.{ Files, Path, Paths }

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.PathExplorer.{ InputPathExplorer, OutputPathExplorer, StagingPathExplorer }
import nl.knaw.dans.easy.multideposit.model._
import org.joda.time.DateTime
import org.scalatest.{ FlatSpec, Inside, Matchers, OptionValues }

trait TestSupportFixture extends FlatSpec with Matchers with OptionValues with Inside with InputPathExplorer with StagingPathExplorer with OutputPathExplorer {

  lazy val testDir: Path = {
    val path = Paths.get(s"target/test/${ getClass.getSimpleName }").toAbsolutePath
    path.deleteDirectory()
    Files.createDirectories(path)
    path
  }
  override val multiDepositDir: Path = testDir.resolve("md")
  override val stagingDir: Path = testDir.resolve("sd")
  override val outputDepositDir: Path = testDir.resolve("od")

  implicit val inputPathExplorer: InputPathExplorer = this
  implicit val stagingPathExplorer: StagingPathExplorer = this
  implicit val outputPathExplorer: OutputPathExplorer = this

  def testInstructions1: Instructions = {
    Instructions(
      depositId = "ruimtereis01",
      row = 2,
      depositorUserId = "ruimtereiziger1",
      profile = Profile(
        titles = List("Reis naar Centaur-planetoïde", "Trip to Centaur asteroid"),
        descriptions = List("Een tweedaagse reis per ruimteschip naar een bijzondere planetoïde in de omgeving van Jupiter.", "A two day mission to boldly go where no man has gone before"),
        creators = List(
          CreatorPerson(
            titles = Some("Captain"),
            initials = "J.T.",
            surname = "Kirk",
            organization = Some("United Federation of Planets")
          )
        ),
        created = DateTime.parse("2015-05-19"),
        audiences = List("D30000"),
        accessright = AccessCategory.OPEN_ACCESS
      ),
      metadata = Metadata(
        formats = List("video/mpeg", "text/plain"),
        languages = List("NL", "encoding=UTF-8"),
        subjects = List(Subject("astronomie"), Subject("ruimtevaart"), Subject("planetoïden"))
      ),
      files = Map(
        testDir.resolve("md/ruimtereis01/reisverslag/centaur.mpg") -> FileDescriptor(Option("flyby of centaur"))
      ),
      audioVideo = AudioVideo(
        springfield = Option(Springfield("dans", "janvanmansum", "Jans-test-files", PlayMode.Menu)),
        avFiles = Map(
          testDir.resolve("md/ruimtereis01/reisverslag/centaur.mpg") -> Set(
            Subtitles(testDir.resolve("md/ruimtereis01/reisverslag/centaur.srt"), Option("en")),
            Subtitles(testDir.resolve("md/ruimtereis01/reisverslag/centaur-nederlands.srt"), Option("nl"))
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
        titles = List("Title 1 of deposit 2", "Title 2 of deposit 2"),
        descriptions = List("A sample deposit with a not very long description"),
        creators = List(CreatorOrganization("Creator A")),
        created = DateTime.now(),
        available = DateTime.parse("2016-07-30"),
        audiences = List("D37000"),
        accessright = AccessCategory.GROUP_ACCESS
      ),
      metadata = Metadata(
        contributors = List(ContributorOrganization("Contributor 1"), ContributorOrganization("Contributor 2")),
        subjects = List(Subject("subject 1", Option("abr:ABRcomplex")), Subject("subject 2"), Subject("subject 3")),
        publishers = List("publisher 1"),
        types = List(DcType.STILLIMAGE),
        identifiers = List(Identifier("id1234"))
      ),
      files = Map(
        testDir.resolve("md/ruimtereis02/path/to/images/Hubble_01.jpg") -> FileDescriptor(Some("Hubble"), Some(FileAccessRights.RESTRICTED_REQUEST))
      )
    )
  }

  def testInstructions: Seq[Instructions] = Seq(testInstructions1, testInstructions2)
}
