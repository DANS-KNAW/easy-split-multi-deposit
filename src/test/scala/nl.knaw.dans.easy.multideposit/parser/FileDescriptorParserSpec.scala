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
import cats.data.{ Chain, NonEmptyList }
import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model.{ FileAccessRights, FileDescriptor }
import org.scalatest.BeforeAndAfterEach

trait FileDescriptorTestObjects {
  this: InputPathExplorer =>

  lazy val fileDescriptorCSV @ fileDescriptorCSVRow1 :: fileDescriptorCSVRow2 :: Nil = List(
    Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "video about the centaur meteorite",
      "FILE_ACCESSIBILITY" -> "RESTRICTED_GROUP"
    ),
    Map(
      "FILE_PATH" -> "path/to/a/random/video/hubble.mpg",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> "RESTRICTED_GROUP"
    )
  )

  lazy val fileDescriptors: Map[File, FileDescriptor] = Map(
    multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg" ->
      FileDescriptor(
        title = Option("video about the centaur meteorite"),
        accessibility = Option(FileAccessRights.RESTRICTED_GROUP)
      ),
    multiDepositDir / "ruimtereis01/path/to/a/random/video/hubble.mpg" ->
      FileDescriptor(
        title = Option.empty,
        accessibility = Option(FileAccessRights.RESTRICTED_GROUP)
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
    val row1 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )
    val row2 = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    inside(extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil)) {
      case Valid(result) => result should (have size 1 and contain only
        file -> FileDescriptor(Some("some title"), Some(FileAccessRights.KNOWN)))
    }
  }

  it should "succeed if no title or accessibility is given for a single file" in {
    val row1 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> ""
    )
    val row2 = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    inside(extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil)) {
      case Valid(result) => result should (have size 1 and contain only file -> FileDescriptor())
    }
  }

  it should "succeed if only a title is given for a single file" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    inside(extractFileDescriptors("ruimtereis01", 2, row :: Nil)) {
      case Valid(result) => result should (have size 1 and contain only
        file -> FileDescriptor(Some("some title"), None))
    }
  }

  it should "succeed if only an accessibility is given for a single file" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    inside(extractFileDescriptors("ruimtereis01", 2, row :: Nil)) {
      case Valid(result) => result should (have size 1 and contain only
        file -> FileDescriptor(accessibility = Some(FileAccessRights.KNOWN)))
    }
  }

  it should "fail if multiple titles are given for a single file" in {
    val row1 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "title1",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )
    val row2 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "title2",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil) shouldBe Invalid(Chain(ParseError(2, s"FILE_TITLE defined multiple values for file '$file': [title1, title2]")))
  }

  it should "fail if multiple accessibilities are given for a single file" in {
    val row1 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )
    val row2 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> "ANONYMOUS"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil) shouldBe Invalid(Chain(ParseError(2, s"FILE_ACCESSIBILITY defined multiple values for file '$file': [KNOWN, ANONYMOUS]")))
  }

  it should "fail if multiple titles and accessibilities are given for a single file" in {
    val row1 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "title1",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )
    val row2 = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "title2",
      "FILE_ACCESSIBILITY" -> "ANONYMOUS"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    inside(extractFileDescriptors("ruimtereis01", 2, row1 :: row2 :: Nil)) {
      case Invalid(chain) =>
        chain.toNonEmptyList shouldBe NonEmptyList.of(
          ParseError(2, s"FILE_TITLE defined multiple values for file '$file': [title1, title2]"),
          ParseError(2, s"FILE_ACCESSIBILITY defined multiple values for file '$file': [KNOWN, ANONYMOUS]"),
        )
    }
  }

  it should "fail if visibility is more restricted than accessibility" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_VISIBILITY" -> "NONE",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    extractFileDescriptors("ruimtereis01", 2, row :: Nil) shouldBe Invalid(Chain(ParseError(2, s"FILE_VISIBILITY (NONE) is more restricted than FILE_ACCESSIBILITY (KNOWN) for file '$file'. (User will potentially have access to an invisible file.)")))
  }

  "fileDescriptor" should "convert the csv input to the corresponding output" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Valid((file, Some("some title"), Some(FileAccessRights.KNOWN), None))
  }

  it should "fail if the path does not exist" in {
    val row = Map(
      "FILE_PATH" -> "path/that/does/not/exist.txt",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Invalid(Chain(ParseError(2, "unable to find path 'path/that/does/not/exist.txt'")))
  }

  it should "fail if the path represents a directory" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Invalid(Chain(ParseError(2, s"path 'reisverslag/' exists, but is not a regular file")))
  }

  it should "succeed if no FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Valid((file, Some("some title"), None, None))
  }

  it should "fail if an invalid FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "invalid"
    )

    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Invalid(Chain(ParseError(2, "Value 'invalid' is not a valid file accessright")))
  }

  it should "fail if FILE_PATH is not given but FILE_TITLE and FILE_ACCESSIBILITY are given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Invalid(Chain(ParseError(2, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given")))
  }

  it should "fail if only FILE_TITLE is given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Invalid(Chain(ParseError(2, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given")))
  }

  it should "fail if only FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value shouldBe Invalid(Chain(ParseError(2, "FILE_TITLE, FILE_ACCESSIBILITY and FILE_VISIBILITY are only allowed if FILE_PATH is also given")))
  }

  it should "succeed when all fields are empty" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> ""
    )

    fileDescriptor("ruimtereis01")(2)(row) shouldBe empty
  }

  "fileAccessibility" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    fileAccessibility(2)("NONE").right.value shouldBe FileAccessRights.NONE
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    fileAccessibility(2)("unknown value").left.value shouldBe ParseError(2, "Value 'unknown value' is not a valid file accessright")
  }

  "fileVisibility" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    fileVisibility(2)("NONE").right.value shouldBe FileAccessRights.NONE
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    fileVisibility(2)("unknown value").left.value shouldBe ParseError(2, "Value 'unknown value' is not a valid file visibility")
  }
}
