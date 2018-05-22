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
import nl.knaw.dans.lib.error.CompositeException
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

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

  lazy val fileDescriptors = Map(
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

class FileDescriptorParserSpec extends TestSupportFixture with FileDescriptorTestObjects with BeforeAndAfterEach { self =>

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

    inside(extractFileDescriptors(row1 :: row2 :: Nil, 2, "ruimtereis01")) {
      case Success(result) => result should (have size 1 and contain only
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

    inside(extractFileDescriptors(row1 :: row2 :: Nil, 2, "ruimtereis01")) {
      case Success(result) => result should (have size 1 and contain only file -> FileDescriptor())
    }
  }

  it should "succeed if only a title is given for a single file" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"

    inside(extractFileDescriptors(row :: Nil, 2, "ruimtereis01")) {
      case Success(result) => result should (have size 1 and contain only
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

    inside(extractFileDescriptors(row :: Nil, 2, "ruimtereis01")) {
      case Success(result) => result should (have size 1 and contain only
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
    val msg = s"FILE_TITLE defined multiple values for file '$file': [title1, title2]"

    extractFileDescriptors(row1 :: row2 :: Nil, 2, "ruimtereis01") should matchPattern {
      case Failure(CompositeException(ParseException(2, `msg`, _) :: Nil)) =>
    }
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
    val msg = s"FILE_ACCESSIBILITY defined multiple values for file '$file': [KNOWN, ANONYMOUS]"

    extractFileDescriptors(row1 :: row2 :: Nil, 2, "ruimtereis01") should matchPattern {
      case Failure(CompositeException(ParseException(2, `msg`, _) :: Nil)) =>
    }
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
    val msg1 = s"FILE_TITLE defined multiple values for file '$file': [title1, title2]"
    val msg2 = s"FILE_ACCESSIBILITY defined multiple values for file '$file': [KNOWN, ANONYMOUS]"

    extractFileDescriptors(row1 :: row2 :: Nil, 2, "ruimtereis01") should matchPattern {
      case Failure(CompositeException(ParseException(2, `msg1`, _) :: ParseException(2, `msg2`, _) :: Nil)) =>
    }
  }

  "fileDescriptor" should "convert the csv input to the corresponding output" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Success((`file`, Some("some title"), Some(FileAccessRights.KNOWN))) =>
    }
  }

  it should "fail if the path does not exist" in {
    val row = Map(
      "FILE_PATH" -> "path/that/does/not/exist.txt",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "FILE_PATH does not represent a valid path", _)) =>
    }
  }

  it should "fail if the path represents a directory" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag"
    val msg = s"FILE_PATH '$file' is not a file"
    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, `msg`, _)) =>
    }
  }

  it should "succeed if no FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag/centaur.mpg"
    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Success((`file`, Some("some title"), None)) =>
    }
  }

  it should "fail if no FILE_ACCESSIBILITY is given and the path does not exist" in {
    val row = Map(
      "FILE_PATH" -> "path/that/does/not/exist.txt",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "FILE_PATH does not represent a valid path", _)) =>
    }
  }

  it should "fail if no FILE_ACCESSIBILITY is given and the path represents a directory" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag"
    val msg = s"FILE_PATH '$file' is not a file"
    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, `msg`, _)) =>
    }
  }

  it should "fail if an invalid FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/centaur.mpg",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "invalid"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'invalid' is not a valid file accessright", _)) =>
    }
  }

  it should "fail if an invalid FILE_ACCESSIBILITY is given and the path does not exist" in {
    val row = Map(
      "FILE_PATH" -> "path/that/does/not/exist.txt",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "invalid"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(CompositeException(
      ParseException(2, "FILE_PATH does not represent a valid path", _) ::
        ParseException(2, "Value 'invalid' is not a valid file accessright", _) :: Nil)) =>
    }
  }

  it should "fail if an invalid FILE_ACCESSIBILITY is given and the path represents a directory" in {
    val row = Map(
      "FILE_PATH" -> "reisverslag/",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "invalid"
    )

    val file = multiDepositDir / "ruimtereis01/reisverslag"
    val msg = s"FILE_PATH '$file' is not a file"
    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(CompositeException(
      ParseException(2, `msg`, _) ::
        ParseException(2, "Value 'invalid' is not a valid file accessright", _) :: Nil)) =>
    }
  }

  it should "fail if FILE_PATH is not given but FILE_TITLE and FILE_ACCESSIBILITY are given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "FILE_TITLE and FILE_ACCESSIBILITY are not allowed, since FILE_PATH isn't given", _)) =>
    }
  }

  it should "fail if FILE_PATH is not given but FILE_TITLE and FILE_ACCESSIBILITY are given and FILE_ACCESSIBILITY is invalid" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> "invalid"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(CompositeException(
        ParseException(2, "FILE_TITLE and FILE_ACCESSIBILITY are not allowed, since FILE_PATH isn't given", _) ::
          ParseException(2, "Value 'invalid' is not a valid file accessright", _) :: Nil)) =>
    }
  }

  it should "fail if only FILE_TITLE is given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "some title",
      "FILE_ACCESSIBILITY" -> ""
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "FILE_TITLE is not allowed, since FILE_PATH isn't given", _)) =>
    }
  }

  it should "fail if only FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> "KNOWN"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "FILE_ACCESSIBILITY is not allowed, since FILE_PATH isn't given", _)) =>
    }
  }

  it should "fail if only an invalid FILE_ACCESSIBILITY is given" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> "invalid"
    )

    fileDescriptor("ruimtereis01")(2)(row).value should matchPattern {
      case Failure(CompositeException(
        ParseException(2, "FILE_ACCESSIBILITY is not allowed, since FILE_PATH isn't given", _) ::
          ParseException(2, "Value 'invalid' is not a valid file accessright", _) :: Nil)) =>
    }
  }

  it should "succeed when all fields are empty" in {
    val row = Map(
      "FILE_PATH" -> "",
      "FILE_TITLE" -> "",
      "FILE_ACCESSIBILITY" -> ""
    )

    fileDescriptor("ruimtereis01")(2)(row) shouldBe empty
  }

  "fileAccessRight" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    val row = Map("FILE_ACCESSIBILITY" -> "NONE")
    fileAccessRight(2)(row).value should matchPattern { case Success(FileAccessRights.NONE) => }
  }

  it should "return None if SF_ACCESSIBILITY is not defined" in {
    val row = Map("FILE_ACCESSIBILITY" -> "")
    fileAccessRight(2)(row) shouldBe empty
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    val row = Map("FILE_ACCESSIBILITY" -> "unknown value")
    fileAccessRight(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid file accessright", _)) =>
    }
  }
}
