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

import java.nio.file.{ Path, Paths }
import java.util.UUID._

import nl.knaw.dans.easy.multideposit.PathExplorer.InputPathExplorer
import nl.knaw.dans.easy.multideposit.model.Instructions
import nl.knaw.dans.easy.multideposit.{ FileExtensions, TestSupportFixture }
import nl.knaw.dans.lib.error.CompositeException
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

trait DepositTestObjects extends AudioVideoTestObjects
  with FileDescriptorTestObjects
  with FileMetadataTestObjects
  with MetadataTestObjects
  with ProfileTestObjects {
  this: TestSupportFixture =>

  lazy val depositCSV @ depositCSVRow1 :: depositCSVRow2 :: Nil = List(
    Map("ROW" -> "2", "DATASET" -> "ruimtereis01", "DEPOSITOR_ID" -> "ikke", "BASE_REVISION" ->"1de3f841-0f0d-048b-b3db-4b03ad4834d7") ++ profileCSVRow1 ++ metadataCSVRow1 ++ fileDescriptorCSVRow1 ++ audioVideoCSVRow1,
    Map("ROW" -> "3", "DATASET" -> "ruimtereis01", "BASE_REVISION" -> "") ++ profileCSVRow2 ++ metadataCSVRow2 ++ fileDescriptorCSVRow2 ++ audioVideoCSVRow2
  )

  lazy val instructions = Instructions(
    depositId = "ruimtereis01",
    row = 2,
    depositorUserId = "ikke",
    profile = profile,
    baseUUID = Option(fromString("1de3f841-0f0d-048b-b3db-4b03ad4834d7")),
    metadata = metadata,
    files = fileDescriptors,
    audioVideo = audioVideo
  )
}

class MultiDepositParserSpec extends TestSupportFixture with DepositTestObjects with BeforeAndAfterEach { self =>

  override def beforeEach(): Unit = {
    super.beforeEach()
    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(multiDepositDir)
  }

  private val parser = new MultiDepositParser with InputPathExplorer {
    val multiDepositDir: Path = self.multiDepositDir
  }

  import parser._

  "parse" should "load the input csv file into the object model" in {
    val instructionsFile = multiDepositDir.resolve("instructions.csv")
    instructionsFile.toFile should exist

    inside(MultiDepositParser.parse(testDir.resolve("md"))) {
      case Success(datasets) =>
        datasets should have size 4
        val deposit1 :: deposit2 :: deposit3 :: deposit4 :: Nil = datasets.toList.sortBy(_.depositId)

        deposit1 should have(
          'depositId ("ruimtereis01"),
          'row (2)
        )
        deposit2 should have(
          'depositId ("ruimtereis02"),
          'row (5)
        )
        deposit3 should have(
          'depositId ("ruimtereis03"),
          'row (10)
        )
        deposit4 should have(
          'depositId ("ruimtereis04"),
          'row (11)
        )
    }
  }

  "read" should "parse the input csv file into a list of headers and a table of data" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      List("2", "abc", "def", "ghi", "jkl"),
      List("3", "mno", "pqr", "stu", "vwx"),
      List("4", "yzy", "xwv", "uts", "rqp"),
      List("5", "onm", "lkj", "ihg", "fed"),
      List("6", "cba", "abc", "def", "ghi")
    )

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "correctly parse newlines in the data (using quotes according to RFC4180) and still correctly do the row numbering" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |a  bc,def,ghi,jkl
        |mno,"pq
        |r",stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      List("2", "a  bc", "def", "ghi", "jkl"),
      List("3", "mno", "pq\nr", "stu", "vwx"),
      List("4", "yzy", "xwv", "uts", "rqp"),
      List("5", "onm", "lkj", "ihg", "fed"),
      List("6", "cba", "abc", "def", "ghi")
    )

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "parse the input when some cells are empty" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,,jkl
        |mno,,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      List("2", "abc", "def", "", "jkl"),
      List("3", "mno", "", "stu", "vwx"),
      List("4", "yzy", "xwv", "uts", "rqp"),
      List("5", "onm", "lkj", "", "fed"),
      List("6", "cba", "abc", "def", "ghi")
    )

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "parse the input when some cells are blank and leave these cells empty in the result" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,  ,stu,vwx
        |,xwv,uts,rqp
        |onm,lkj, ,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      List("2", "abc", "def", "ghi", "jkl"),
      List("3", "mno", "", "stu", "vwx"),
      List("4", "", "xwv", "uts", "rqp"),
      List("5", "onm", "lkj", "", "fed"),
      List("6", "cba", "abc", "def", "ghi")
    )

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "parse the input while leaving out blank rows" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      List("2", "abc", "def", "ghi", "jkl"),
      List("3", "mno", "pqr", "stu", "vwx"),
      List("5", "onm", "lkj", "ihg", "fed"),
      List("6", "cba", "abc", "def", "ghi")
    )

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "parse the input if it only contains a row of headers and no data" in {
    val csv = "DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN"
    val file = testDir.resolve("input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List.empty[String]

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "fail when the input csv file is empty" in {
    val csv = ""
    val file = testDir.resolve("input.csv")
    file.write(csv)

    read(file) should matchPattern { case Failure(EmptyInstructionsFileException(`file`)) => }
  }

  it should "fail when the input contains invalid headers" in {
    val csv =
      """DATASET,foo,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    inside(read(file)) {
      case Failure(ParseException(0, msg, _)) =>
        msg should include("unknown headers: [foo]")
    }
  }

  it should "fail when the input contains duplicate valid headers" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,DEPOSITOR_ID
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir.resolve("input.csv")
    file.write(csv)

    inside(read(file)) {
      case Failure(ParseException(0, msg, _)) =>
        msg should include("duplicate headers: [DEPOSITOR_ID]")
    }
  }

  "detectEmptyDepositCells" should "succeed when no elements in the input are empty" in {
    val dsIds = List("ds1", "ds1", "ds2", "ds2", "ds2", "ds3")

    detectEmptyDepositCells(dsIds) shouldBe a[Success[_]]
  }

  it should "fail when any number of elements in the input are blank" in {
    val dsIds = List("ds1", "", "ds2", "ds2", "   ", "ds3")

    inside(detectEmptyDepositCells(dsIds)) {
      case Failure(CompositeException(e1 :: e2 :: Nil)) =>
        e1 should have message "Row 3 does not have a depositId in column DATASET"
        e2 should have message "Row 6 does not have a depositId in column DATASET"
    }
  }

  "extractInstructions" should "convert the csv input to the corresponding output" in {
    extractInstructions("ruimtereis01", depositCSV) should matchPattern { case Success(`instructions`) => }
  }

  it should "throw an exception if a row number is not found on each row" in {
    // This is supposed to throw an exception rather than fail, because the ROW is a column
    // that is created by our program itself. If the ROW is not present, something has gone
    // terribly wrong!
    val rows = depositCSVRow1 :: (depositCSVRow2 - "ROW") :: Nil

    the[NoSuchElementException] thrownBy extractInstructions("ruimtereis01", rows) should have message "key not found: ROW"
  }

  it should "fail if there are multiple distinct depositorUserIDs" in {
    val rows = depositCSVRow1 :: (depositCSVRow2 + ("DEPOSITOR_ID" -> "ikke2")) :: Nil

    extractInstructions("ruimtereis01", rows) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [ikke, ikke2]", _)) =>
    }
  }

  it should "succeed if there are multiple depositorUserIDs that are all equal" in {
    val rows = depositCSVRow1 :: (depositCSVRow2 + ("DEPOSITOR_ID" -> "ikke")) :: Nil

    extractInstructions("ruimtereis01", rows) should matchPattern { case Success(`instructions`) => }
  }

  it should "fail if the depositId contains invalid characters" in {
    extractInstructions("ruimtereis01#", depositCSV) should matchPattern {
      case Failure(ParseException(2, "The column 'DATASET' contains the following invalid characters: {#}", _)) =>
    }
  }

  it should "fail if there are multiple distinct base revisions" in {
    val row = Map("ROW" -> "3", "DATASET" -> "ruimtereis01", "BASE_REVISION" -> "9de3f841-0f0d-048b-b3db-4b03ad4834d7") ++ profileCSVRow2 ++ metadataCSVRow2 ++ fileDescriptorCSVRow2 ++ audioVideoCSVRow2
    val rows = depositCSVRow1 :: row :: Nil

    extractInstructions("ruimtereis01", rows) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'BASE_REVISION'. Found: [1de3f841-0f0d-048b-b3db-4b03ad4834d7, 9de3f841-0f0d-048b-b3db-4b03ad4834d7]", _)) =>
    }
  }

  it should "not fail if there are multiple nondistinct base revisions" in {
    val row = Map("ROW" -> "3", "DATASET" -> "ruimtereis01", "BASE_REVISION" -> "1de3f841-0f0d-048b-b3db-4b03ad4834d7") ++ profileCSVRow2 ++ metadataCSVRow2 ++ fileDescriptorCSVRow2 ++ audioVideoCSVRow2
    val rows = depositCSVRow1 :: row :: Nil

    extractInstructions("ruimtereis01", rows) should matchPattern {
      case Success(_) =>
    }
  }

  "uuid" should "fail if the base revision does not conform to uuid format" in {
    val row = Map("BASE_REVISION" -> "abcd-12xy")

    uuid("BASE_REVISION")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "BASE_REVISION value base revision 'abcd-12xy' does not conform to the UUID format", _)) =>
    }
  }

  it should "not fail if the base revision conforms to uuid format when there are missing digits in the base revision" in {
    val row = Map("BASE_REVISION" -> "1de3f841-0f0d-048-b3db-4b03ad4834d7")

    uuid("BASE_REVISION")(2)(row).value should matchPattern {
       case Success(_) =>
    }
  }

  it should "not fail if the base revision conforms to uuid format" in {
    val row = Map("BASE_REVISION" -> "1de3f841-0f0d-048b-b3db-4b03ad4834d7")

    uuid("BASE_REVISION")(2)(row).value should matchPattern {
      case Success(_) =>
    }
  }
}
