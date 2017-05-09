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

import java.io.File

import nl.knaw.dans.common.lang.dataset.AccessCategory
import org.scalatest._

import nl.knaw.dans.easy.multideposit.model._
import org.joda.time.DateTime

abstract class UnitSpec extends FlatSpec with Matchers with OptionValues with Inside with OneInstancePerTest with BeforeAndAfterAll {

  val testDir = new File(s"target/test/${ getClass.getSimpleName }")
  val formatsFile = new File(testDir, "formats.txt")

  override def beforeAll(): Unit = {
    super.beforeAll()
    testDir.mkdirs()
    new File(getClass.getResource("/debug-config/formats.txt").toURI).copyFile(formatsFile)
  }

  override def afterAll: Unit = {
    super.afterAll()
    testDir.getParentFile.deleteDirectory()
  }

  def testDeposit1: Deposit = {
    Deposit(
      depositId = "ruimtereis01",
      row = 2,
      depositorId = "ruimtereiziger1",
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
      audioVideo = AudioVideo(
        springfield = Option(Springfield("dans", "janvanmansum", "Jans-test-files")),
        avFiles = Set(AVFile(
          file = new File("ruimtereis01/reisverslag/centaur.mpg"),
          title = Option("flyby of centaur"),
          subtitles = List(Subtitles(new File("ruimtereis01/reisverslag/centaur.srt"), Option("en")))
        ))
      )
    )
  }

  def testDeposit2: Deposit = {
    Deposit(
      depositId = "deposit-2",
      row = 5,
      depositorId = "ruimtereiziger2",
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
      )
    )
  }

  def testDeposits: Seq[Deposit] = List(testDeposit1, testDeposit2)
}
