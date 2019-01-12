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
import cats.data.Validated.{ Invalid, Valid }
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model.{ AVFileMetadata, Audio, DefaultFileMetadata, FileAccessRights, FileDescriptor, PlayMode, Springfield, SubtitlesFile, Video }
import org.scalatest.BeforeAndAfterEach

trait FileMetadataTestObjects {
  this: InputPathExplorer =>

  lazy val fileMetadata @ fileMetadata1 :: fileMetadata2 :: fileMetadata4 :: Nil = List(
    List(
      AVFileMetadata(
        filepath = multiDepositDir / "ruimtereis01/path/to/a/random/video/hubble.mpg",
        mimeType = "video/mpeg",
        vocabulary = Video,
        title = "video about the hubble space telescope",
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
          SubtitlesFile(multiDepositDir / "ruimtereis01/reisverslag/centaur.srt", Some("en")),
          SubtitlesFile(multiDepositDir / "ruimtereis01/reisverslag/centaur-nederlands.srt", Some("nl"))
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
      case Valid(fs) =>
        fs should { have size 9 and contain allElementsOf fileMetadata1 }
    }
  }

  it should "return an empty list if the directory does not exist" in {
    val ruimtereis03Path = multiDepositDir / "ruimtereis03"

    // presupposition
    ruimtereis03Path shouldNot exist

    // test
    extractFileMetadata(ruimtereis03Path, testInstructions1) shouldBe Valid(Nil)
  }

  it should "collect the metadata for all files in ruimtereis04" in {
    val instructions = testInstructions1.copy(
      audioVideo = testInstructions1.audioVideo.copy(
        springfield = Some(Springfield("dans", "janvanmansum", "Jans-test-files", PlayMode.Continuous))
      )
    )

    inside(extractFileMetadata(multiDepositDir / "ruimtereis04", instructions)) {
      case Valid(fs) =>
        fs should { have size 3 and contain allElementsOf fileMetadata4 }
    }
  }

  it should "fail when the deposit contains A/V files, Springfield PlayMode is Menu, and a FileTitle is not present" in {
    val fileWithNoDescription = testDir / "md/ruimtereis01/path/to/a/random/video/hubble.mpg"
    val instructions = testInstructions1.copy(
      files = testInstructions1.files.updated(fileWithNoDescription, FileDescriptor(title = Option.empty)),
    )

    extractFileMetadata(multiDepositDir / "ruimtereis01", instructions) shouldBe
      ParseError(2, s"No FILE_TITLE given for A/V file $fileWithNoDescription.").toInvalid
  }

  it should "fail when the deposit contains A/V files, Springfield PlayMode is Menu, and an A/V file is not listed" in {
    val fileWithNoDescription = testDir / "md/ruimtereis01/path/to/a/random/video/hubble.mpg"
    val instructions = testInstructions1.copy(
      files = testInstructions1.files - fileWithNoDescription,
    )

    extractFileMetadata(multiDepositDir / "ruimtereis01", instructions) shouldBe
      ParseError(2, s"Not listed A/V file detected: $fileWithNoDescription. Because Springfield PlayMode 'MENU' was choosen, all A/V files must be listed with a human readable title in the FILE_TITLE field.").toInvalid
  }

  it should "collect multiple errors" in {
    val file1 = testDir / "md/ruimtereis01/reisverslag/centaur.mpg"
    val file2 = testDir / "md/ruimtereis01/path/to/a/random/video/hubble.mpg"
    val instructions = testInstructions1.copy(
      files = testInstructions1.files
        .updated(file1, FileDescriptor(Option.empty))
        .updated(file2, FileDescriptor(Option.empty))
    )

    inside(extractFileMetadata(multiDepositDir / "ruimtereis01", instructions)) {
      case Invalid(chain) => chain.toNonEmptyList.toList should contain inOrderOnly(
        ParseError(2, s"No FILE_TITLE given for A/V file $file2."),
        ParseError(2, s"No FILE_TITLE given for A/V file $file1."),
      )
    }
  }
}
