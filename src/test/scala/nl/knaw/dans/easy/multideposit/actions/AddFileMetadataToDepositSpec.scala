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

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.collection.mutable
import scala.util.{ Failure, Success }
import scala.xml.{ Utility, XML }

class AddFileMetadataToDepositSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    stagingDir = new File(testDir, "sd")
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
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", "")
    )
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the dataset does not contain the SF_DOMAIN (which is optional)" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", "")
    )
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "succeed if the dataset only contains empty values for the SF_DOMAIN (which is optional)" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("user", ""),
      "SF_COLLECTION" -> List("collection", "")
    )
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the dataset contains A/V files but the SF_* fields are all blank" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", "")
    )
    inside(new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg") and
            include("path/to/a/random/sound/chicken.mp3")
        }
    }
  }

  it should "fail if the dataset contains A/V files but the SF_* fields are not all non-blank" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "SF_DOMAIN" -> List("domain", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", "")
    )
    inside(new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg") and
            include("path/to/a/random/sound/chicken.mp3")
        }
    }
  }

  it should "fail if the dataset contains A/V files but the SF_* fields are not present" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID)
    )
    inside(new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg") and
            include("path/to/a/random/sound/chicken.mp3")
        }
    }
  }

  it should "fail if the dataset contains A/V files but the SF_* fields are not all present" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "SF_COLLECTION" -> List("collection", "")
    )
    inside(new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg") and
            include("path/to/a/random/sound/chicken.mp3")
        }
    }
  }

  it should "succeed if the dataset contains no A/V files and the SF_* fields are not present" in {
    val datasetID = "ruimtereis02"
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", "")
    )
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the dataset contains no A/V files and any of the SF_* fields are present" in {
    val datasetID = "ruimtereis02"
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "SF_COLLECTION" -> List("collection", "")
    )
    inside(new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions) {
      case Failure(ActionException(_, message, _)) =>
        message should {
          include("Values found for these columns: [SF_COLLECTION]") and
            include("these columns should be empty because there are no audio/video")
        }
    }
  }

  it should "fail if the dataset contains no A/V files and all SF_* fields are present but only contain empty values" in {
    val datasetID = "ruimtereis02"
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", ""),
      "SF_DOMAIN" -> List("", ""),
      "SF_USER" -> List("", ""),
      "SF_COLLECTION" -> List("", "")
    )
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  it should "create an empty list of mimetypes if the dataset directory corresponding with the datasetId does not exist" in {
    val datasetID = "ruimtereis03"
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "DDM_CREATED" -> List("2017-07-30", "")
    )
    multiDepositDir(datasetID) should not (exist)
    new AddFileMetadataToDeposit(1, (datasetID, dataset)).checkPreconditions shouldBe a[Success[_]]
  }

  "execute" should "write the file metadata to an xml file" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "AV_FILE" -> List("ruimtereis01/reisverslag/centaur.mpg", "")
    )
    val action = new AddFileMetadataToDeposit(1, (datasetID, dataset))
    val metadataDir = stagingBagMetadataDir(datasetID)

    action.execute() shouldBe a[Success[_]]

    metadataDir should exist
    stagingFileMetadataFile(datasetID) should exist
  }

  it should "produce the xml for all the files" in {
    val dataset = mutable.HashMap(
      "DATASET" -> List(datasetID, datasetID),
      "AV_FILE" -> List("ruimtereis01/reisverslag/centaur.mpg", "")
    )
    AddFileMetadataToDeposit(1, (datasetID, dataset)).execute() shouldBe a[Success[_]]

    Utility.trim(XML.loadFile(stagingFileMetadataFile(datasetID)))
      .child
      .map(node => (node \@ "filepath", node.child.filter(_.label == "format").head.text)) should {
      have length 10 and
        contain allOf(
        ("data/ruimtereis01_verklaring.txt", "text/plain"),
        ("data/reisverslag/deel01.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ("data/reisverslag/deel01.txt", "text/plain"),
        ("data/reisverslag/deel02.txt", "text/plain"),
        ("data/reisverslag/deel03.txt", "text/plain"),
        ("data/path/to/a/random/video/hubble.mpg", "video/mpeg"),
        ("data/path/to/a/random/sound/chicken.mp3", "audio/mpeg"),
        ("data/reisverslag/centaur.mpg", "video/mpeg"),
        ("data/reisverslag/centaur.srt", "text/plain"),
        ("data/reisverslag/centaur-nederlands.srt", "text/plain")
      )
    }
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
}
