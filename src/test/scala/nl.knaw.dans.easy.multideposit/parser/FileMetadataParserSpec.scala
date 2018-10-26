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

import better.files.File
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.lib.error.CompositeException
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

trait FileMetadataTestObjects {
  this: InputPathExplorer =>

  lazy val fileMetadata @ fileMetadata1 :: fileMetadata2 :: fileMetadata4 :: Nil = List(
    List(
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/path/to/a/random/video/hubble.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "video about the hubble space telescoop",
        accessibleTo = FileAccessRights.ANONYMOUS,
        visibleTo = FileAccessRights.ANONYMOUS,
        subtitles = Set.empty
      ),
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "flyby of centaur",
        accessibleTo = FileAccessRights.ANONYMOUS,
        visibleTo = FileAccessRights.ANONYMOUS,
        subtitles = Set(
          Subtitles(multiDepositDir / "ruimtereis01/reisverslag/centaur.srt", Some("en")),
          Subtitles(multiDepositDir / "ruimtereis01/reisverslag/centaur-nederlands.srt", Some("nl"))
        )
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/centaur.srt",
        mimeType = "text/plain"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/centaur-nederlands.srt",
        mimeType = "text/plain"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/deel01.docx",
        mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/deel01.txt",
        mimeType = "text/plain"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/deel02.txt",
        mimeType = "text/plain"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/reisverslag/deel03.txt",
        mimeType = "text/plain"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/ruimtereis01_verklaring.txt",
        mimeType = "text/plain"
      )
    ),
    List(
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis02/path/to/images/Hubble_01.jpg",
        mimeType = "image/jpeg"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis02/path/to/images/Hubbleshots.jpg",
        mimeType = "image/jpeg"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis02/hubble-wiki-en.txt",
        mimeType = "text/plain"
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis02/hubble-wiki-nl.txt",
        mimeType = "text/plain"
      )
    ),
    List(
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis04/path/to/a/random/file/file.txt",
        mimeType = "text/plain"
      ),
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis04/path/to/a/random/sound/chicken.mp3",
        mimeType = "audio/mpeg",
        vocabulary = Audio,
        title = "chicken.mp3",
        accessibleTo = FileAccessRights.ANONYMOUS,
        visibleTo = FileAccessRights.ANONYMOUS,
        subtitles = Set.empty
      ),
      DefaultFileMetadata(
        filepath = multiDepositDir / "ruimtereis04/Quicksort.hs",
        mimeType = "text/x-haskell"
      )
    )
  )
}

class FileMetadataParserSpec extends TestSupportFixture with FileMetadataTestObjects with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()

    if (multiDepositDir.exists) multiDepositDir.delete()
    File(getClass.getResource("/allfields/input").toURI).copyTo(multiDepositDir)
  }

  private val parser = new FileMetadataParser {}

  import parser._

  "extractFileMetadata" should "collect the metadata for all files in ruimtereis01" in {
    inside(extractFileMetadata(multiDepositDir / "ruimtereis01", testInstructions1)) {
      case Success(fms) =>
        fms should { have size 9 and contain allElementsOf fileMetadata1 }
    }
  }

  it should "return an empty list if the directory does not exist" in {
    val ruimtereis03Path = multiDepositDir / "ruimtereis03"

    // presupposition
    ruimtereis03Path shouldNot exist

    // test
    extractFileMetadata(ruimtereis03Path, testInstructions1) should matchPattern {
      case Success(Nil) =>
    }
  }

  it should "collect the metadata for all files in ruimtereis04" in {
    val instructions = testInstructions1.copy(
      audioVideo = testInstructions1.audioVideo.copy(
        springfield = Some(Springfield("dans", "janvanmansum", "Jans-test-files", PlayMode.Continuous))
      )
    )
    inside(extractFileMetadata(multiDepositDir / "ruimtereis04", instructions)) {
      case Success(fms) =>
        fms should { have size 3 and contain allElementsOf fileMetadata4 }
    }
  }

  it should "fail when the deposit contains A/V files, Springfield PlayMode is Menu, and a FileTitle is not present" in {
    val fileWithNoDescription = testDir / "md/ruimtereis01/path/to/a/random/video/hubble.mpg"
    val instructions = testInstructions1.copy(
      files = testInstructions1.files.updated(fileWithNoDescription, FileDescriptor(title = Option.empty)),
    )
    inside(extractFileMetadata(multiDepositDir / "ruimtereis01", instructions)) {
      case Failure(CompositeException(List(e1: ParseException))) =>
        e1 should have message s"No FILE_TITLE given for A/V file $fileWithNoDescription."
    }
  }

  it should "fail when the deposit contains A/V files, Springfield PlayMode is Menu, and an A/V file is not listed" in {
    val fileWithNoDescription = testDir / "md/ruimtereis01/path/to/a/random/video/hubble.mpg"
    val instructions = testInstructions1.copy(
      files = testInstructions1.files - fileWithNoDescription,
    )
    inside(extractFileMetadata(multiDepositDir / "ruimtereis01", instructions)) {
      case Failure(CompositeException(List(e1: ParseException))) =>
        e1 should have message s"Not listed A/V file detected: $fileWithNoDescription. Because Springfield PlayMode 'MENU' was choosen, all A/V files must be listed with a human readable title in the FILE_TITLE field."
    }
  }

  it should "fail if the deposit contains A/V files but the SF_* fields are not present" in {
    val instructions = testInstructions1.copy(
      depositId = "ruimtereis01",
      audioVideo = AudioVideo(springfield = Option.empty)
    )
    inside(extractFileMetadata(multiDepositDir / "ruimtereis01", instructions)) {
      case Failure(ParseException(_, message, _)) =>
        message should {
          include("No values found for these columns: [SF_USER, SF_COLLECTION]") and
            include("reisverslag/centaur.mpg") and
            include("path/to/a/random/video/hubble.mpg")
        }
    }
  }

  it should "fail if the deposit contains no A/V files but any of the SF_* fields are not present" in {
    val instructions = testInstructions2.copy(
      depositId = "ruimtereis02",
      audioVideo = testInstructions2.audioVideo.copy(
        springfield = Option(Springfield(user = "user", collection = "collection", playMode = PlayMode.Continuous))
      )
    )
    inside(extractFileMetadata(multiDepositDir / "ruimtereis02", instructions)) {
      case Failure(ParseException(_, message, _)) =>
        message should {
          include("Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]") and
            include("these columns should be empty because there are no audio/video files found in this deposit")
        }
    }
  }

  it should "fail if a dataset has both audio and video material in it" in {
    val instructions = testInstructions1.copy(depositId = "ruimtereis01")

    val audioFile = multiDepositDir / "ruimtereis01/path/to/a/random/audio/chicken.mp3"
    audioFile.parent.createDirectories()
    (multiDepositDir / "ruimtereis04/path/to/a/random/sound/chicken.mp3").copyTo(audioFile)

    val currentAV = instructions.audioVideo.avFiles
    val newAV = currentAV + (audioFile -> Set.empty[Subtitles])
    val failingInstructions = instructions.copy(
      audioVideo = instructions.audioVideo.copy(avFiles = newAV),
      files = instructions.files + (audioFile -> FileDescriptor(title = Option("crowing rooster")))
    )

    inside(extractFileMetadata(multiDepositDir / "ruimtereis01", failingInstructions)) {
      case Failure(ParseException(_, message, _)) =>
        message shouldBe "Found both audio and video in this dataset. Only one of them is allowed."
    }
  }
}
