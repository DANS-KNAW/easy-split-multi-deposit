/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.multideposit.actions

import java.io.{ File, FileNotFoundException }

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.parser.{ AVFile, AudioVideo, FileAccessRights, Springfield, Subtitles }
import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }
import scala.xml.{ Node, Utility, XML }

class AddFileMetadataToDepositSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md").getAbsoluteFile,
    stagingDir = new File(testDir, "sd").getAbsoluteFile
  )
  val datasetID = "ruimtereis01"

  before {
    new File(getClass.getResource("/allfields/input").toURI)
      .copyDir(settings.multidepositDir)
    new File(getClass.getResource("/mimetypes").toURI)
      .copyDir(new File(testDir, "mimetypes"))
  }

  after {
    settings.stagingDir.deleteDirectory()
  }

  "checkPreconditions" should "succeed if the dataset contains the SF_* fields in case a A/V file is found" in {
    val dataset = testDataset1.copy(
      datasetId = datasetID,
      audioVideo = testDataset1.audioVideo.copy(
        springfield = Option(Springfield("domain", "user", "collection")),
        accessibility = Option(FileAccessRights.NONE)
      )
    )
    AddFileMetadataToDeposit(dataset).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the dataset contains A/V files but the SF_* fields are not present" in {
    val dataset = testDataset1.copy(
      datasetId = datasetID,
      audioVideo = AudioVideo(springfield = Option.empty, accessibility = Option(FileAccessRights.NONE))
    )
    inside(AddFileMetadataToDeposit(dataset).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg") and
            include("path/to/a/random/sound/chicken.mp3")
        }
    }
  }

  it should "succeed if the dataset contains A/V files and SF_ACCESSIBILITY isn't present, but DDM_ACCESSRIGHTS is present" in {
    val dataset = testDataset1.copy(
      datasetId = datasetID,
      profile = testDataset1.profile.copy(accessright = AccessCategory.NO_ACCESS),
      audioVideo = AudioVideo(
        springfield = Option(Springfield("domain", "user", "collection")),
        accessibility = Option.empty
      )
    )
    AddFileMetadataToDeposit(dataset).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the dataset contains no A/V files and the SF_* fields are not present" in {
    val datasetID = "ruimtereis02"
    val dataset = testDataset2.copy(
      datasetId = datasetID,
      audioVideo = AudioVideo()
    )
    AddFileMetadataToDeposit(dataset).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the dataset contains no A/V files and any of the SF_* fields are present" in {
    val datasetID = "ruimtereis02"
    val dataset = testDataset2.copy(
      row = 1,
      datasetId = datasetID,
      audioVideo = testDataset2.audioVideo.copy(
        springfield = Option(Springfield(user = "user", collection = "collection"))
      )
    )
    inside(AddFileMetadataToDeposit(dataset).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]") and
            include("these columns should be empty because there are no audio/video files found in this dataset")
        }
    }
  }

  it should "create an empty list of file metadata if the dataset directory corresponding with the datasetId does not exist and therefore succeed" in {
    val datasetID = "ruimtereis03"
    val dataset = testDataset2.copy(datasetId = datasetID)
    multiDepositDir(datasetID) should not (exist)
    AddFileMetadataToDeposit(dataset).checkPreconditions shouldBe a[Success[_]]
  }

  "execute" should "write the file metadata to an xml file" in {
    val dataset = testDataset1.copy(
      datasetId = datasetID,
      audioVideo = testDataset1.audioVideo.copy(
        accessibility = Option(FileAccessRights.NONE),
        avFiles = Set(AVFile(new File("ruimtereis01/reisverslag/centaur.mpg")))
      )
    )
    val action = AddFileMetadataToDeposit(dataset)
    val metadataDir = stagingBagMetadataDir(dataset.datasetId)

    action.execute() shouldBe a[Success[_]]

    metadataDir should exist
    stagingFileMetadataFile(dataset.datasetId) should exist
  }

  it should "produce the xml for all the files" in {
    val dataset = testDataset1.copy(
      datasetId = datasetID,
      audioVideo = AudioVideo(
        springfield = Option(Springfield("dans", "janvanmansum", "Jans-test-files")),
        accessibility = Option(FileAccessRights.RESTRICTED_GROUP),
        avFiles = Set(
          AVFile(
            file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile,
            title = Option("video about the centaur meteorite"),
            subtitles = List(
              Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile, Option("en")),
              Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur-nederlands.srt").getAbsoluteFile, Option("nl"))
            )
          ),
          AVFile(
            file = new File(settings.multidepositDir, "ruimtereis01/path/to/a/random/sound/chicken.mp3").getAbsoluteFile,
            title = Option("our daily wake up call")
          )
        )
      )
    )
    AddFileMetadataToDeposit(dataset).execute() shouldBe a[Success[_]]

    val expected = XML.loadFile(new File(getClass.getResource("/allfields/output/input-ruimtereis01/bag/metadata/files.xml").toURI))
    val actual = XML.loadFile(stagingFileMetadataFile(datasetID))

    verify(actual, expected)
  }

  it should "produce the xml for a dataset with no A/V files" in {
    val datasetID = "ruimtereis02"
    val dataset = testDataset2.copy(datasetId = datasetID)
    AddFileMetadataToDeposit(dataset).execute() shouldBe a[Success[_]]

    val expected = XML.loadFile(new File(getClass.getResource("/allfields/output/input-ruimtereis02/bag/metadata/files.xml").toURI))
    val actual = XML.loadFile(stagingFileMetadataFile(datasetID))

    verify(actual, expected)
  }

  it should "produce the xml for a dataset with no files" in {
    val datasetID = "ruimtereis03"
    val dataset = testDataset2.copy(datasetId = datasetID)
    AddFileMetadataToDeposit(dataset).execute() shouldBe a[Success[_]]

    val expected = XML.loadFile(new File(getClass.getResource("/allfields/output/input-ruimtereis03/bag/metadata/files.xml").toURI))
    val actual = XML.loadFile(stagingFileMetadataFile(datasetID))

    verify(actual, expected)
  }

  "getMimeType" should "produce the correct doc mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-ms-doc.doc"))) {
      case Success(mimetype) => mimetype shouldBe "application/msword"
    }
  }

  it should "produce the correct docx mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-ms-docx.docx"))) {
      case Success(mimetype) => mimetype shouldBe "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    }
  }

  it should "produce the correct xlsx mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-ms-excel.xlsx"))) {
      case Success(mimetype) => mimetype shouldBe "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
  }

  it should "produce the correct pdf mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-pdf.pdf"))) {
      case Success(mimetype) => mimetype shouldBe "application/pdf"
    }
  }

  it should "produce the correct plain text mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-plain-text.txt"))) {
      case Success(mimetype) => mimetype shouldBe "text/plain"
    }
  }

  it should "produce the correct json mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-json.json"))) {
      case Success(mimetype) => mimetype shouldBe "application/json"
    }
  }

  it should "produce the correct xml mimetype" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-xml.xml"))) {
      case Success(mimetype) => mimetype shouldBe "application/xml"
    }
  }

  it should "give the correct mimetype if the file is plain text and has no extension" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-unknown"))) {
      case Success(mimetype) => mimetype shouldBe "text/plain"
    }
  }

  it should "give the correct mimetype if the file has no extension" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-unknown-pdf"))) {
      case Success(mimetype) => mimetype shouldBe "application/pdf"
    }
  }

  it should "give the correct mimetype if the file is hidden" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/.file-hidden-pdf"))) {
      case Success(mimetype) => mimetype shouldBe "application/pdf"
    }
  }

  it should "fail if the file does not exist" in {
    inside(AddFileMetadataToDeposit.getMimeType(new File(testDir, "mimetypes/file-does-not-exist.doc"))) {
      case Failure(e: FileNotFoundException) => e.getMessage should include ("mimetypes/file-does-not-exist.doc")
    }
  }

  def verify(actualXml: Node, expectedXml: Node): Unit = {
    Utility.trim(actualXml).toString() shouldBe Utility.trim(expectedXml).toString()
  }
}
