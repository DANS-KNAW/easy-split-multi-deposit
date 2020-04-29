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
import nl.knaw.dans.easy.multideposit.model.{ FileAccessRights, FileDescriptor }
import org.scalatest.BeforeAndAfterEach
import cats.syntax.option._

trait FileDescriptorTestObjects {
  this: InputPathExplorer =>

  lazy val fileDescriptorCSV @ fileDescriptorCSVRow1 :: fileDescriptorCSVRow2 :: Nil = List(
    Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "video about the centaur meteorite",
      Headers.FileAccessibility -> "RESTRICTED_REQUEST"
    ),
    Map(
      Headers.FilePath -> "path/to/a/random/video/hubble.mpg",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> "RESTRICTED_REQUEST"
    ),
  )

  lazy val fileDescriptorCSVRow = List(
    DepositRow(2, fileDescriptorCSVRow1),
    DepositRow(3, fileDescriptorCSVRow2),
  )

  lazy val fileDescriptors: Map[File, FileDescriptor] = Map(
    multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg" ->
      FileDescriptor(
        rowNum = 2,
        title = Option("video about the centaur meteorite"),
        accessibility = Option(FileAccessRights.RESTRICTED_REQUEST)
      ),
    multiDepositDir / "ruimtereis01/path/to/a/random/video/hubble.mpg" ->
      FileDescriptor(
        rowNum = 3,
        title = Option.empty,
        accessibility = Option(FileAccessRights.RESTRICTED_REQUEST)
      )
  )
}

class FileDescriptorParserSpec extends TestSupportFixture with FileDescriptorTestObjects with BeforeAndAfterEach {
  self =>

  override def beforeEach(): Unit = {
    super.beforeEach()

    if (multiDepositDir.exists) multiDepositDir.delete()
    File(getClass.getResource("/allfields/input").toURI).copyTo(multiDepositDir)
  }

  private val parser = new FileDescriptorParser with ParserUtils with InputPathExplorer {
    override val multiDepositDir: File = self.multiDepositDir
  }

  import parser._

  "extractFileDescriptors" should "convert the csv input to the corresponding output" in {
    val row1 = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))
    val row2 = DepositRow(3, Map(
      Headers.FilePath -> "",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> ""
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil).value should {
      have size 1 and contain only
        file -> FileDescriptor(2, Some("some title"), Some(FileAccessRights.ANONYMOUS))
    }
  }

  it should "succeed if no title or accessibility is given for a single file" in {
    val row1 = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> ""
    ))
    val row2 = DepositRow(3, Map(
      Headers.FilePath -> "",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> ""
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil).value should {
      have size 1 and contain only file -> FileDescriptor(2)
    }
  }

  it should "succeed if only a title is given for a single file" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> ""
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row :: Nil).value should {
      have size 1 and contain only
        file -> FileDescriptor(2, Some("some title"), None)
    }
  }

  it should "succeed if only an accessibility is given for a single file" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row :: Nil).value should {
      have size 1 and contain only
        file -> FileDescriptor(2, accessibility = Some(FileAccessRights.ANONYMOUS))
    }
  }

  it should "fail if multiple titles are given for a single file" in {
    val row1 = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "title1",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))
    val row2 = DepositRow(3, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "title2",
      Headers.FileAccessibility -> ""
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil).invalidValue shouldBe
      ParseError(2, s"FILE_TITLE defined multiple values for file '$file': [title1, title2]").chained
  }

  it should "fail if multiple accessibilities are given for a single file" in {
    val row1 = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "title",
      Headers.FileAccessibility -> "NONE"
    ))
    val row2 = DepositRow(3, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil).invalidValue shouldBe
      ParseError(2, s"FILE_ACCESSIBILITY defined multiple values for file '$file': [NONE, ANONYMOUS]").chained
  }

  it should "fail if multiple titles and accessibilities are given for a single file" in {
    val row1 = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "title1",
      Headers.FileAccessibility -> "NONE"
    ))
    val row2 = DepositRow(3, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "title2",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil).invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, s"FILE_TITLE defined multiple values for file '$file': [title1, title2]"),
      ParseError(2, s"FILE_ACCESSIBILITY defined multiple values for file '$file': [NONE, ANONYMOUS]"),
    )
  }

  it should "fail if visibility is more restricted than accessibility" in {
    val row1 = DepositRow(2, Map(
      Headers.FilePath -> "path/to/a/random/video/hubble.mpg",
      Headers.FileVisibility -> "NONE",
      Headers.FileAccessibility -> "NONE"
    ))
    val row2 = DepositRow(3, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileVisibility -> "NONE",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil).invalidValue shouldBe
      ParseError(3, s"FILE_VISIBILITY (NONE) is more restricted than FILE_ACCESSIBILITY (ANONYMOUS) for file '$file'. (User will potentially have access to an invisible file.)").chained
  }

  it should "succeed if visibility is equally restricted as accessibility" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileVisibility -> "NONE",
      Headers.FileAccessibility -> "NONE"
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    val expectedOutput = Map(
      file -> FileDescriptor(2, none, FileAccessRights.NONE.some, FileAccessRights.NONE.some),
    )
    extractFileDescriptors("ruimtereis01", 2, row :: Nil).value shouldBe expectedOutput
  }

  "fileDescriptor" should "convert the csv input to the corresponding output" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    fileDescriptor("ruimtereis01")(row).value.value shouldBe (2, file, Some("some title"), Some(FileAccessRights.ANONYMOUS), None)
  }

  it should "fail if the path does not exist" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "path/that/does/not/exist.txt",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> "NONE"
    ))

    fileDescriptor("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "unable to find path 'path/that/does/not/exist.txt'").chained
  }

  it should "fail if the path represents a directory" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    fileDescriptor("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, s"path 'reisverslag/' exists, but is not a regular file").chained
  }

  it should "succeed if no FILE_ACCESSIBILITY is given" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> ""
    ))

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    fileDescriptor("ruimtereis01")(row).value.value shouldBe (2, file, Some("some title"), None, None)
  }

  it should "fail if an invalid FILE_ACCESSIBILITY is given" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "reisverslag/centaur.mpg",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> "invalid"
    ))

    fileDescriptor("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "Value 'invalid' is not a valid file accessright").chained
  }

  it should "fail if FILE_PATH is not given but FILE_TITLE and FILE_ACCESSIBILITY are given" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> "ANONYMOUS"
    ))

    fileDescriptor("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given").chained
  }

  it should "fail if only FILE_TITLE is given" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "",
      Headers.FileTitle -> "some title",
      Headers.FileAccessibility -> ""
    ))

    fileDescriptor("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given").chained
  }

  it should "fail if only FILE_ACCESSIBILITY is given" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> "NONE"
    ))

    fileDescriptor("ruimtereis01")(row).value.invalidValue shouldBe
      ParseError(2, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given").chained
  }

  it should "succeed when all fields are empty" in {
    val row = DepositRow(2, Map(
      Headers.FilePath -> "",
      Headers.FileTitle -> "",
      Headers.FileAccessibility -> ""
    ))

    fileDescriptor("ruimtereis01")(row) shouldBe empty
  }

  "fileAccessibility" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    fileAccessibility(2)("NONE").value shouldBe FileAccessRights.NONE
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    fileAccessibility(2)("unknown value").invalidValue shouldBe
      ParseError(2, "Value 'unknown value' is not a valid file accessright").chained
  }

  "fileVisibility" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    fileVisibility(2)("NONE").value shouldBe FileAccessRights.NONE
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    fileVisibility(2)("unknown value").invalidValue shouldBe
      ParseError(2, "Value 'unknown value' is not a valid file visibility").chained
  }
}
