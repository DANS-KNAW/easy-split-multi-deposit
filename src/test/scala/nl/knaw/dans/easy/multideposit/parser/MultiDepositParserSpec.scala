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

import java.nio.file.Paths

import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, _ }
import nl.knaw.dans.lib.error.CompositeException

import scala.util.{ Failure, Success }

trait DepositTestObjects extends AudioVideoTestObjects with MetadataTestObjects with ProfileTestObjects {

  lazy val depositCSV @ depositCSVRow1 :: depositCSVRow2 :: depositCSVRow3 :: Nil = List(
    Map("ROW" -> "2", "DATASET" -> "ruimtereis01", "DEPOSITOR_ID" -> "ikke") ++ profileCSVRow1 ++ metadataCSVRow1 ++ audioVideoCSVRow1,
    Map("ROW" -> "3", "DATASET" -> "ruimtereis01") ++ profileCSVRow2 ++ metadataCSVRow2 ++ audioVideoCSVRow2,
    Map("ROW" -> "4", "DATASET" -> "ruimtereis01") ++ audioVideoCSVRow3
  )

  lazy val deposit = Deposit(
    depositId = "ruimtereis01",
    row = 2,
    depositorUserId = "ikke",
    profile = profile,
    metadata = metadata,
    audioVideo = audioVideo
  )
}

class MultiDepositParserSpec extends UnitSpec with DepositTestObjects {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Paths.get(getClass.getResource("/allfields/input").toURI).copyDir(settings.multidepositDir)
  }

  override implicit val settings = Settings(
    multidepositDir = testDir.resolve("md").toAbsolutePath
  )
  private val parser = MultiDepositParser()

  import parser._

  "parse" should "load the input csv file into the object model" in {
    val file = settings.multidepositDir.resolve("instructions.csv")
    file.toFile should exist

    inside(parse(file)) {
      case Success(datasets) =>
        datasets should have size 3
        val deposit2 :: deposit1 :: deposit3 :: Nil = datasets.toList

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

  "extractDeposit" should "convert the csv input to the corresponding output" in {
    extractDeposit("ruimtereis01", depositCSV) should matchPattern { case Success(`deposit`) => }
  }

  it should "throw an exception if a row number is not found on each row" in {
    // This is supposed to throw an exception rather than fail, because the ROW is a column
    // that is created by our program itself. If the ROW is not present, something has gone
    // terribly wrong!
    val rows = depositCSVRow1 :: (depositCSVRow2 - "ROW") :: depositCSVRow3 :: Nil

    the[NoSuchElementException] thrownBy extractDeposit("ruimtereis01", rows) should have message "key not found: ROW"
  }

  it should "fail if there are multiple distinct depositorUserIDs" in {
    val rows = depositCSVRow1 :: (depositCSVRow2 + ("DEPOSITOR_ID" -> "ikke2")) :: depositCSVRow3 :: Nil

    extractDeposit("ruimtereis01", rows) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [ikke, ikke2]", _)) =>
    }
  }

  it should "succeed if there are multiple depositorUserIDs that are all equal" in {
    val rows = depositCSVRow1 :: (depositCSVRow2 + ("DEPOSITOR_ID" -> "ikke")) :: depositCSVRow3 :: Nil

    extractDeposit("ruimtereis01", rows) should matchPattern { case Success(`deposit`) => }
  }

  it should "fail if the depositId contains invalid characters" in {
    extractDeposit("ruimtereis01#", depositCSV) should matchPattern {
      case Failure(ParseException(2, "The column 'DATASET' contains the following invalid characters: {#}", _)) =>
    }
  }
}
