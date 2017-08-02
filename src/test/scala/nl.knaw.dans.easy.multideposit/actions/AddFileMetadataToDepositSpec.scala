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
package nl.knaw.dans.easy.multideposit.actions

import java.nio.file.{ NoSuchFileException, Paths }

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model.{ AVFile, AudioVideo, FileAccessRights, Springfield, Subtitles }
import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }
import scala.xml.XML

class AddFileMetadataToDepositSpec extends UnitSpec with BeforeAndAfter with CustomMatchers {

  implicit val settings = Settings(
    multidepositDir = testDir.resolve("md").toAbsolutePath,
    stagingDir = testDir.resolve("sd").toAbsolutePath
  )
  val depositId = "ruimtereis01"

  before {
    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(settings.multidepositDir)
    Paths.get(getClass.getResource("/mimetypes").toURI).copyDir(testDir.resolve("mimetypes"))
  }

  "checkPreconditions" should "succeed if the deposit contains the SF_* fields in case an A/V file is found" in {
    val deposit = testDeposit1.copy(
      depositId = depositId,
      audioVideo = testDeposit1.audioVideo.copy(
        springfield = Option(Springfield("domain", "user", "collection")),
        accessibility = Option(FileAccessRights.NONE)
      )
    )
    AddFileMetadataToDeposit(deposit).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the deposit contains A/V files but the SF_* fields are not present" in {
    val deposit = testDeposit1.copy(
      depositId = depositId,
      audioVideo = AudioVideo(springfield = Option.empty, accessibility = Option(FileAccessRights.NONE))
    )
    inside(AddFileMetadataToDeposit(deposit).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg")
        }
    }
  }

  it should "succeed if the deposit contains A/V files and SF_ACCESSIBILITY isn't present, but DDM_ACCESSRIGHTS is present" in {
    val deposit = testDeposit1.copy(
      depositId = depositId,
      profile = testDeposit1.profile.copy(accessright = AccessCategory.NO_ACCESS),
      audioVideo = AudioVideo(
        springfield = Option(Springfield("domain", "user", "collection")),
        accessibility = Option.empty
      )
    )
    AddFileMetadataToDeposit(deposit).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the deposit contains no A/V files and the SF_* fields are not present" in {
    val depositId = "ruimtereis02"
    val deposit = testDeposit2.copy(
      depositId = depositId,
      audioVideo = AudioVideo()
    )
    AddFileMetadataToDeposit(deposit).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the deposit contains no A/V files and any of the SF_* fields are present" in {
    val depositId = "ruimtereis02"
    val deposit = testDeposit2.copy(
      row = 1,
      depositId = depositId,
      audioVideo = testDeposit2.audioVideo.copy(
        springfield = Option(Springfield(user = "user", collection = "collection"))
      )
    )
    inside(AddFileMetadataToDeposit(deposit).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]") and
            include("these columns should be empty because there are no audio/video files found in this deposit")
        }
    }
  }

  it should "create an empty list of file metadata if the deposit directory corresponding with the depositId does not exist and therefore succeed" in {
    val depositId = "ruimtereis03"
    val deposit = testDeposit2.copy(depositId = depositId)
    multiDepositDir(depositId).toFile should not(exist)
    AddFileMetadataToDeposit(deposit).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if a dataset has both audio and video material in it" in {
    val depositId = "ruimtereis01"
    val deposit = testDeposit1.copy(depositId = depositId)

    val audioFile = multiDepositDir(depositId).resolve("path/to/a/random/audio/chicken.mp3")
    testDir.resolve(s"md/ruimtereis04/path/to/a/random/sound/chicken.mp3").copyFile(audioFile)

    val currentAV = deposit.audioVideo.avFiles
    val newAV = currentAV + AVFile(
      path = audioFile,
      title = Option("our daily wake up call"),
      subtitles = List.empty)
    val failingDeposit = deposit.copy(audioVideo = deposit.audioVideo.copy(avFiles = newAV))

    inside(AddFileMetadataToDeposit(failingDeposit).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message shouldBe "Found both audio and video in this dataset. Only one of them is allowed."
    }
  }

  "execute" should "write the file metadata to an xml file" in {
    val deposit = testDeposit1.copy(
      depositId = depositId,
      audioVideo = testDeposit1.audioVideo.copy(
        accessibility = Option(FileAccessRights.NONE),
        avFiles = Set(AVFile(Paths.get("ruimtereis01/reisverslag/centaur.mpg")))
      )
    )
    val action = AddFileMetadataToDeposit(deposit)
    val metadataDir = stagingBagMetadataDir(deposit.depositId)

    action.execute() shouldBe a[Success[_]]

    metadataDir.toFile should exist
    stagingFileMetadataFile(deposit.depositId).toFile should exist
  }

  it should "produce the xml for all the files" in {
    val deposit = testDeposit1.copy(
      depositId = depositId,
      audioVideo = AudioVideo(
        springfield = Option(Springfield("dans", "janvanmansum", "Jans-test-files")),
        accessibility = Option(FileAccessRights.RESTRICTED_GROUP),
        avFiles = Set(
          AVFile(
            path = settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.mpg").toAbsolutePath,
            title = Option("video about the centaur meteorite"),
            subtitles = List(
              Subtitles(settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur.srt").toAbsolutePath, Option("en")),
              Subtitles(settings.multidepositDir.resolve("ruimtereis01/reisverslag/centaur-nederlands.srt").toAbsolutePath, Option("nl"))
            )
          )
        )
      )
    )
    AddFileMetadataToDeposit(deposit).execute() shouldBe a[Success[_]]

    val expected = XML.loadFile(Paths.get(getClass.getResource("/allfields/output/input-ruimtereis01/bag/metadata/files.xml").toURI).toFile)
    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toFile)

    actual should equalTrimmed(expected)
  }

  it should "produce the xml for a deposit with no A/V files" in {
    val depositId = "ruimtereis02"
    val deposit = testDeposit2.copy(depositId = depositId)
    AddFileMetadataToDeposit(deposit).execute() shouldBe a[Success[_]]

    val expected = XML.loadFile(Paths.get(getClass.getResource("/allfields/output/input-ruimtereis02/bag/metadata/files.xml").toURI).toFile)
    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toFile)

    actual should equalTrimmed(expected)
  }

  it should "produce the xml for a deposit with no files" in {
    val depositId = "ruimtereis03"
    val deposit = testDeposit2.copy(depositId = depositId)
    AddFileMetadataToDeposit(deposit).execute() shouldBe a[Success[_]]

    val expected = XML.loadFile(Paths.get(getClass.getResource("/allfields/output/input-ruimtereis03/bag/metadata/files.xml").toURI).toFile)
    val actual = XML.loadFile(stagingFileMetadataFile(depositId).toFile)

    actual should equalTrimmed(expected)
  }

  "getMimeType" should "produce the correct doc mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-ms-doc.doc"))) {
      case Success(mimetype) => mimetype shouldBe "application/msword"
    }
  }

  it should "produce the correct docx mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-ms-docx.docx"))) {
      case Success(mimetype) => mimetype shouldBe "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    }
  }

  it should "produce the correct xlsx mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-ms-excel.xlsx"))) {
      case Success(mimetype) => mimetype shouldBe "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
  }

  it should "produce the correct pdf mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-pdf.pdf"))) {
      case Success(mimetype) => mimetype shouldBe "application/pdf"
    }
  }

  it should "produce the correct plain text mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-plain-text.txt"))) {
      case Success(mimetype) => mimetype shouldBe "text/plain"
    }
  }

  it should "produce the correct json mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-json.json"))) {
      case Success(mimetype) => mimetype shouldBe "application/json"
    }
  }

  it should "produce the correct xml mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-xml.xml"))) {
      case Success(mimetype) => mimetype shouldBe "application/xml"
    }
  }

  it should "give the correct mimetype if the file is plain text and has no extension" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-unknown"))) {
      case Success(mimetype) => mimetype shouldBe "text/plain"
    }
  }

  it should "give the correct mimetype if the file has no extension" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-unknown-pdf"))) {
      case Success(mimetype) => mimetype shouldBe "application/pdf"
    }
  }

  it should "give the correct mimetype if the file is hidden" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/.file-hidden-pdf"))) {
      case Success(mimetype) => mimetype shouldBe "application/pdf"
    }
  }

  it should "fail if the file does not exist" in {
    inside(AddFileMetadataToDeposit.getMimeType(testDir.resolve("mimetypes/file-does-not-exist.doc"))) {
      case Failure(e: NoSuchFileException) => e.getMessage should include("mimetypes/file-does-not-exist.doc")
    }
  }
}
