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

import java.io.File

import nl.knaw.dans.common.lang.dataset.AccessCategory
import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, _ }
import nl.knaw.dans.lib.error.CompositeException
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory

import scala.util.{ Failure, Success }

trait LanguageBehavior { this: UnitSpec =>
  def validLanguage3Tag(parser: MultiDepositParser, lang: String): Unit = {
    it should "succeed when the language tag is valid" in {
      val row = Map("taal" -> lang)
      parser.iso639_2Language("taal")(2)(row).value should matchPattern { case Success(`lang`) => }
    }
  }

  def invalidLanguage3Tag(parser: MultiDepositParser, lang: String): Unit = {
    it should "fail when the language tag is invalid" in {
      val row = Map("taal" -> lang)
      val errorMsg = s"Value '$lang' is not a valid value for taal"
      parser.iso639_2Language("taal")(2)(row).value should matchPattern { case Failure(ParseException(2, `errorMsg`, _)) => }
    }
  }
}

class MultiDepositParserSpec extends UnitSpec with MockFactory with LanguageBehavior {

  override def beforeAll(): Unit = {
    super.beforeAll()
    new File(getClass.getResource("/allfields/input").toURI).copyDir(settings.multidepositDir)
  }

  private implicit val settings = Settings(
    multidepositDir = new File(testDir, "md").getAbsoluteFile
  )
  private val parser = new MultiDepositParser

  import parser._

  "read" should "parse the input csv file into a list of headers and a table of data" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
    file.write(csv)

    val expectedHeaders = List("ROW", "DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List.empty[String]

    read(file) should matchPattern { case Success((`expectedHeaders`, `expectedData`)) => }
  }

  it should "fail when the input csv file is empty" in {
    val csv = ""
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
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
    val file = new File(testDir, "input.csv")
    file.write(csv)

    inside(read(file)) {
      case Failure(ParseException(0, msg, _)) =>
        msg should include("duplicate headers: [DEPOSITOR_ID]")
    }
  }

  "parse" should "load the input csv file into the object model" in {
    val file = new File(settings.multidepositDir, "instructions.csv")
    file should exist

    inside(parse(file)) {
      case Success(datasets) =>
        datasets should have size 3
        val dataset2 :: dataset1 :: dataset3 :: Nil = datasets.toList

        dataset1 should have(
          'datasetId ("ruimtereis01"),
          'row (2)
        )
        dataset2 should have(
          'datasetId ("ruimtereis02"),
          'row (5)
        )
        dataset3 should have(
          'datasetId ("ruimtereis03"),
          'row (10)
        )
    }
  }

  "detectEmptyDatasetCells" should "succeed when no elements in the input are empty" in {
    val dsIds = List("ds1", "ds1", "ds2", "ds2", "ds2", "ds3")

    detectEmptyDatasetCells(dsIds) shouldBe a[Success[_]]
  }

  it should "fail when any number of elements in the input are blank" in {
    val dsIds = List("ds1", "", "ds2", "ds2", "   ", "ds3")

    inside(detectEmptyDatasetCells(dsIds)) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: Nil = es.toList

        e1 should have message "Row 3 does not have a datasetId in column DATASET"
        e2 should have message "Row 6 does not have a datasetId in column DATASET"
    }
  }

  "getRowNum" should "extract the row number from the dataset row" in {
    val row = Map("ROW" -> "2", "TEST" -> "abc")

    getRowNum(row) shouldBe 2
  }

  it should "throw a NoSuchElementException when ROW is not a header in the dataset" in {
    val row = Map("TEST" -> "abc")

    the[NoSuchElementException] thrownBy getRowNum(row) should have message "key not found: ROW"
  }

  it should "throw a NumberFormatException when the value for ROW cannot be converted to an integer" in {
    val row = Map("ROW" -> "def", "TEST" -> "abc")

    the[NumberFormatException] thrownBy getRowNum(row) should have message "For input string: \"def\""
  }

  "extractNEL curried" should "for each row run the given function and collect the results" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractNEL(rows)(i => _ => Some(Success(i))) should matchPattern { case Success(List(2, 3)) => }
  }

  it should "leave out the rows for which the function returns a None" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractNEL(rows) {
      case i if i % 2 == 0 => _ => Some(Success(i))
      case _ => _ => None
    } should matchPattern { case Success(List(2)) => }
  }

  it should "iterate over all rows and aggregate all failures until the end" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    inside(extractNEL(rows)(i => _ => Some(Failure(new Exception(s"foo $i"))))) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: Nil = es.toList

        e1 should have message "foo 2"
        e2 should have message "foo 3"
    }
  }

  it should "fail fast when a row does not contain a ROW column" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    the[NoSuchElementException] thrownBy extractNEL(rows)(i => _ => Some(Success(i))) should
      have message "key not found: ROW"
  }

  it should "fail when the output is empty" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    inside(extractNEL(rows)(_ => _ => None)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message "requirement failed: the list can't be empty"
    }
  }

  "extractNEL" should "find the values for a given column" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractNEL(rows, 2, "FOO") should matchPattern { case Success(List("abc", "ghi")) => }
  }

  it should "filter out the blank values" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("FOO" -> "ghi", "BAR" -> "")
    )

    extractNEL(rows, 2, "BAR") should matchPattern { case Success(List("def")) => }
  }

  it should "fail when the output is empty" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractNEL(rows, 2, "QUX") should matchPattern {
      case Failure(ParseException(2, "There should be at least one non-empty value for QUX", _)) =>
    }
  }

  "extractList curried" should "for each row run the given function and collect the results" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractList(rows)(i => _ => Some(Success(i))) should matchPattern { case Success(List(2, 3)) => }
  }

  it should "leave out the rows for which the function returns a None" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractList(rows) {
      case i if i % 2 == 0 => _ => Some(Success(i))
      case _ => _ => None
    } should matchPattern { case Success(List(2)) => }
  }

  it should "iterate over all rows and aggregate all failures until the end" in {
    val rows = List(
      Map("ROW" -> "2", "FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    inside(extractList(rows)(i => _ => Some(Failure(new Exception(s"foo $i"))))) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: Nil = es.toList

        e1 should have message "foo 2"
        e2 should have message "foo 3"
    }
  }

  it should "fail fast when a row does not contain a ROW column" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("ROW" -> "3", "FOO" -> "ghi", "BAR" -> "jkl")
    )

    the[NoSuchElementException] thrownBy extractList(rows)(i => _ => Some(Success(i))) should
      have message "key not found: ROW"
  }

  "extractList" should "find the values for a given column" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("FOO" -> "ghi", "BAR" -> "jkl")
    )

    extractList(rows, "FOO") should contain inOrderOnly("abc", "ghi")
  }

  it should "filter out the blank values" in {
    val rows = List(
      Map("FOO" -> "abc", "BAR" -> "def"),
      Map("FOO" -> "ghi", "BAR" -> "")
    )

    extractList(rows, "BAR") should contain only "def"
  }

  "atMostOne" should "succeed when the input is empty" in {
    atMostOne(2, List("FOO", "BAR"))(List.empty) should matchPattern { case Success(None) => }
  }

  it should "succeed when the input contains one value" in {
    atMostOne(2, List("FOO", "BAR"))(List("abc")) should matchPattern { case Success(Some("abc")) => }
  }

  it should "succeed when the input contains multiple equal value" in {
    atMostOne(2, List("FOO"))(List.fill(5)("abc")) should matchPattern { case Success(Some("abc")) => }
  }

  it should "fail when the input contains more than one distinct value and one columnName is given" in {
    atMostOne(2, List("FOO"))(List("abc", "def")) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'FOO'. Found: [abc, def]", _)) =>
    }
  }

  it should "fail when the input contains more than one distinct value and multiple columnNames are given" in {
    atMostOne(2, List("FOO", "BAR"))(List("abc", "def")) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for these columns: [FOO, BAR]. Found: [abc, def]", _)) =>
    }
  }

  "exactlyOne" should "succeed when the input contains exactly one value" in {
    exactlyOne(2, List("FOO", "BAR"))(List("abc")) should matchPattern { case Success("abc") => }
  }

  it should "succeed when the input contains exactly one distinct value" in {
    exactlyOne(2, List("FOO"))(List.fill(5)("abc")) should matchPattern { case Success("abc") => }
  }

  it should "fail when the input is empty and one columnName is given" in {
    exactlyOne(2, List("FOO"))(List.empty) should matchPattern {
      case Failure(ParseException(2, "One row has to contain a value for the column: 'FOO'", _)) =>
    }
  }

  it should "fail when the input is empty and multiple columnNames are given" in {
    exactlyOne(2, List("FOO", "BAR"))(List.empty) should matchPattern {
      case Failure(ParseException(2, "One row has to contain a value for these columns: [FOO, BAR]", _)) =>
    }
  }

  it should "fail when the input contains more than one value and one columnName is given" in {
    exactlyOne(2, List("FOO"))(List("abc", "def")) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'FOO'. Found: [abc, def]", _)) =>
    }
  }

  it should "fail when the input contains more than one value and multiple columnNames are given" in {
    exactlyOne(2, List("FOO", "BAR"))(List("abc", "def")) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for these columns: [FOO, BAR]. Found: [abc, def]", _)) =>
    }
  }

  "checkValidChars" should "succeed with the input value when all characters are valid" in {
    checkValidChars(2, "TEST", "valid-input") should matchPattern { case Success("valid-input") => }
  }

  it should "fail when the input contains invalid characters" in {
    checkValidChars(2, "TEST", "#$%") should matchPattern {
      case Failure(ParseException(2, "The column 'TEST' contains the following invalid characters: {#, $, %}", _)) =>
    }
  }

  "missingRequired" should "return a Failure listing the missing columns" in {
    val row = Map("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "")

    missingRequired(2, row, Set("a", "b", "c", "d", "e")) should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [d, e]", _)) =>
    }
  }

  it should "throw an IllegalArgumentException if no columns were missing" in {
    val row = Map("a" -> "1", "b" -> "2", "c" -> "3")

    the[IllegalArgumentException] thrownBy missingRequired(2, row, Set("a", "b", "c")) should have message "requirement failed: the list of missing elements is supposed to be non-empty"
  }

  private lazy val datasetCSV @ datasetCSVRow1 :: datasetCSVRow2 :: datasetCSVRow3 :: Nil = List(
    Map("ROW" -> "2", "DATASET" -> "test", "DEPOSITOR_ID" -> "ikke") ++ profileCSVRow1 ++ metadataCSVRow1 ++ audioVideoCSVRow1,
    Map("ROW" -> "3", "DATASET" -> "test") ++ profileCSVRow2 ++ metadataCSVRow2 ++ audioVideoCSVRow2,
    Map("ROW" -> "4", "DATASET" -> "test") ++ audioVideoCSVRow3
  )

  private lazy val dataset = Dataset(
    datasetId = "test",
    row = 2,
    depositorId = "ikke",
    profile = profile,
    metadata = metadata,
    audioVideo = audioVideo
  )

  "extractDataset" should "convert the csv input to the corresponding output" in {
    extractDataset("test", datasetCSV) should matchPattern { case Success(`dataset`) => }
  }

  it should "throw an exception if a row number is not found on each row" in {
    // This is supposed to throw an exception rather than fail, because the ROW is a column
    // that is created by our program itself. If the ROW is not present, something has gone
    // terribly wrong!
    val rows = datasetCSVRow1 :: (datasetCSVRow2 - "ROW") :: datasetCSVRow3 :: Nil

    the[NoSuchElementException] thrownBy extractDataset("test", rows) should have message "key not found: ROW"
  }

  it should "fail if there are multiple distinct depositorIDs" in {
    val rows = datasetCSVRow1 :: (datasetCSVRow2 + ("DEPOSITOR_ID" -> "ikke2")) :: datasetCSVRow3 :: Nil

    extractDataset("test", rows) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [ikke, ikke2]", _)) =>
    }
  }

  it should "succeed if there are multiple depositorIDs that are all equal" in {
    val rows = datasetCSVRow1 :: (datasetCSVRow2 + ("DEPOSITOR_ID" -> "ikke")) :: datasetCSVRow3 :: Nil

    extractDataset("test", rows) should matchPattern { case Success(`dataset`) => }
  }

  it should "fail if the datasetID contains invalid characters" in {
    extractDataset("test#", datasetCSV) should matchPattern {
      case Failure(ParseException(2, "The column 'DATASET' contains the following invalid characters: {#}", _)) =>
    }
  }

  private lazy val profileCSV @ profileCSVRow1 :: profileCSVRow2 :: Nil = List(
    Map(
      "DC_TITLE" -> "title1",
      "DC_DESCRIPTION" -> "descr1",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DDM_CREATED" -> "2016-07-30",
      "DDM_AVAILABLE" -> "2016-07-31",
      "DDM_AUDIENCE" -> "D30000",
      "DDM_ACCESSRIGHTS" -> "GROUP_ACCESS"
    ),
    Map(
      "DC_TITLE" -> "title2",
      "DC_DESCRIPTION" -> "descr2",
      "DDM_AUDIENCE" -> "D37000"
    )
  )

  private lazy val profile = Profile(
    titles = List("title1", "title2"),
    descriptions = List("descr1", "descr2"),
    creators = List(CreatorPerson(initials = "A.", surname = "Jones")),
    created = DateTime.parse("2016-07-30"),
    available = DateTime.parse("2016-07-31"),
    audiences = List("D30000", "D37000"),
    accessright = AccessCategory.GROUP_ACCESS
  )

  "extractProfile" should "convert the csv input to the corresponding output" in {
    extractProfile(profileCSV, 2) should matchPattern { case Success(`profile`) => }
  }

  it should "fail if there are no values for DC_TITLE, DC_DESCRIPTION, creator, DDM_CREATED, DDM_AUDIENCE and DDM_ACCESSRIGHTS" in {
    val rows = Map.empty[MultiDepositKey, String] :: Map.empty[MultiDepositKey, String] :: Nil
    inside(extractProfile(rows, 2)) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: Nil = es.toList

        e1 should have message "There should be at least one non-empty value for DC_TITLE"
        e2 should have message "There should be at least one non-empty value for DC_DESCRIPTION"
        e3 should have message "There should be at least one non-empty value for the creator fields"
        e4 should have message "One row has to contain a value for the column: 'DDM_CREATED'"
        e5 should have message "There should be at least one non-empty value for DDM_AUDIENCE"
        e6 should have message "One row has to contain a value for the column: 'DDM_ACCESSRIGHTS'"
    }
  }

  it should "fail if there are multiple values for DDM_CREATED, DDM_AVAILABLE and DDM_ACCESSRIGHTS" in {
    val rows = profileCSVRow1 ::
      profileCSVRow2.updated("DDM_CREATED", "2015-07-30")
        .updated("DDM_AVAILABLE", "2015-07-31")
        .updated("DDM_ACCESSRIGHTS", "NO_ACCESS") :: Nil

    inside(extractProfile(rows, 2)) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: e3 :: Nil = es.toList

        e1.getMessage should include("Only one row is allowed to contain a value for the column 'DDM_CREATED'")
        e2.getMessage should include("Only one row is allowed to contain a value for the column 'DDM_AVAILABLE'")
        e3.getMessage should include("Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'")
    }
  }

  it should "fail if DDM_ACCESSRIGHTS is GROUPACCESS and DDM_AUDIENCE does not contain D37000" in {
    val rows = profileCSVRow1 :: Nil

    inside(extractProfile(rows, 2)) {
      case Failure(ParseException(2, msg, _)) =>
        msg should {
          include("DDM_AUDIENCE should be D37000 (Archaeology)") and
            include("contains: [D30000]")
        }
    }
  }

  private lazy val metadataCSV @ metadataCSVRow1 :: metadataCSVRow2 :: Nil = List(
    Map(
      "DCT_ALTERNATIVE" -> "alt1",
      "DC_PUBLISHER" -> "pub1",
      "DC_TYPE" -> "Collection",
      "DC_FORMAT" -> "format1",
      // identifier
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "ARCHIS-ZAAK-IDENTIFICATIE",
      "DC_SOURCE" -> "src1",
      "DC_LANGUAGE" -> "dut",
      "DCT_SPATIAL" -> "spat1",
      "DCT_RIGHTSHOLDER" -> "right1",
      // relation
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "foo",
      // contributor
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      // subject
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex",
      // spatialPoint
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> "degrees",
      // temporal
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    ),
    Map(
      "DCT_ALTERNATIVE" -> "alt2",
      "DC_PUBLISHER" -> "pub2",
      "DC_TYPE" -> "MovingImage",
      "DC_FORMAT" -> "format2",
      "DC_IDENTIFIER" -> "id",
      "DC_SOURCE" -> "src2",
      "DC_LANGUAGE" -> "nld",
      "DCT_SPATIAL" -> "spat2",
      "DCT_RIGHTSHOLDER" -> "right2",
      // spatialBox
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> "RD"
    )
  )

  private lazy val metadata = Metadata(
    alternatives = List("alt1", "alt2"),
    publishers = List("pub1", "pub2"),
    types = List(DcType.COLLECTION, DcType.MOVINGIMAGE),
    formats = List("format1", "format2"),
    identifiers = List(Identifier("123456", Some(IdentifierType.ARCHIS_ZAAK_IDENTIFICATIE)), Identifier("id")),
    sources = List("src1", "src2"),
    languages = List("dut", "nld"),
    spatials = List("spat1", "spat2"),
    rightsholder = List("right1", "right2"),
    relations = List(QualifiedLinkRelation("replaces", "foo")),
    contributors = List(ContributorPerson(initials = "A.", surname = "Jones")),
    subjects = List(Subject("IX", Option("abr:ABRcomplex"))),
    spatialPoints = List(SpatialPoint("12", "34", Option("degrees"))),
    spatialBoxes = List(SpatialBoxx("45", "34", "23", "12", Option("RD"))),
    temporal = List(Temporal("PALEOLB", Option("abr:ABRperiode")))
  )

  "extractMetadata" should "convert the csv input to the corresponding output" in {
    extractMetadata(metadataCSV) should matchPattern { case Success(`metadata`) => }
  }

  it should "use the default type value if no value for DC_TYPE is specified" in {
    inside(extractMetadata(metadataCSV.map(row => row - "DC_TYPE"))) {
      case Success(md) => md.types should contain only DcType.DATASET
    }
  }

  private lazy val audioVideoCSV @ audioVideoCSVRow1 :: audioVideoCSVRow2 :: audioVideoCSVRow3 :: Nil = List(
    Map(
      "SF_DOMAIN" -> "dans",
      "SF_USER" -> "janvanmansum",
      "SF_COLLECTION" -> "jans-test-files",
      "SF_ACCESSIBILITY" -> "NONE",
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "video about the centaur meteorite",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    ),
    Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "",
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur-nederlands.srt",
      "AV_SUBTITLES_LANGUAGE" -> "nl"
    ),
    Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "",
      "AV_FILE" -> "ruimtereis01/path/to/a/random/sound/chicken.mp3",
      "AV_FILE_TITLE" -> "our daily wake up call",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )
  )

  private lazy val audioVideo = AudioVideo(
    springfield = Option(Springfield("dans", "janvanmansum", "jans-test-files")),
    accessibility = Option(FileAccessRights.NONE),
    avFiles = Set(
      AVFile(
        file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile,
        title = Option("video about the centaur meteorite"),
        subtitles = List(
          Subtitles(
            file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile,
            language = Option("en")
          ),
          Subtitles(
            file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur-nederlands.srt").getAbsoluteFile,
            language = Option("nl")
          )
        )
      ),
      AVFile(
        file = new File(settings.multidepositDir, "ruimtereis01/path/to/a/random/sound/chicken.mp3").getAbsoluteFile,
        title = Option("our daily wake up call")
      )
    )
  )

  "extractAudioVideo" should "convert the csv input to the corresponding output" in {
    extractAudioVideo(audioVideoCSV, 2) should matchPattern { case Success(`audioVideo`) => }
  }

  it should "fail if there are AV_FILE values but there is no Springfield data" in {
    val rows = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "",
      "SF_ACCESSIBILITY" -> "NONE",
      "AV_FILE" -> "",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    ) :: audioVideoCSVRow2 :: audioVideoCSVRow3 :: Nil

    extractAudioVideo(rows, 2) should matchPattern {
      case Failure(ParseException(2, "The column 'AV_FILE' contains values, but the columns [SF_COLLECTION, SF_USER] do not", _)) =>
    }
  }

  it should "fail if there is more than one Springfield" in {
    val rows = audioVideoCSVRow1 ::
      audioVideoCSVRow2.updated("SF_DOMAIN", "extra1")
        .updated("SF_USER", "extra2")
        .updated("SF_COLLECTION", "extra3") ::
      audioVideoCSVRow3 :: Nil

    extractAudioVideo(rows, 2) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]. Found: [(dans,janvanmansum,jans-test-files), (extra1,extra2,extra3)]", _)) =>
    }
  }

  it should "fail if there is more than one file accessright" in {
    val rows = audioVideoCSVRow1 ::
      audioVideoCSVRow2.updated("SF_ACCESSIBILITY", "KNOWN") ::
      audioVideoCSVRow3 :: Nil

    extractAudioVideo(rows, 2) should matchPattern {
      case Failure(ParseException(2, "Only one row is allowed to contain a value for the column 'SF_ACCESSIBILITY'. Found: [NONE, KNOWN]", _)) =>
    }
  }

  it should "fail if there there are multiple AV_FILE_TITLEs for one file" in {
    val rows = audioVideoCSVRow1 ::
      audioVideoCSVRow2.updated("AV_FILE_TITLE", "another title") ::
      audioVideoCSVRow3 :: Nil

    inside(extractAudioVideo(rows, 2)) {
      case Failure(CompositeException(es)) =>
        val ParseException(2, msg, _) :: Nil = es.toList
        val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
        msg shouldBe s"The column 'AV_FILE_TITLE' can only have one value for file '$file'"
    }
  }

  "date" should "convert the value of the date into the corresponding object" in {
    val row = Map("datum" -> "2016-07-30")

    inside(date("datum")(2)(row).value) {
      case Success(date) => date shouldBe DateTime.parse("2016-07-30")
    }
  }

  it should "return None if the date is not defined" in {
    val row = Map("datum" -> "")

    date("datum")(2)(row) shouldBe empty
  }

  it should "fail if the value does not represent a date" in {
    val row = Map("datum" -> "you can't parse me!")

    date("datum")(2)(row).value should matchPattern {
      case Failure(ParseException(2, "datum value 'you can't parse me!' does not represent a date", _)) =>
    }
  }

  "accessCategory" should "convert the value for DDM_ACCESSRIGHTS into the corresponding enum object" in {
    val row = Map("DDM_ACCESSRIGHTS" -> "ANONYMOUS_ACCESS")
    accessCategory(2)(row).value should matchPattern {
      case Success(AccessCategory.ANONYMOUS_ACCESS) =>
    }
  }

  it should "return None if DDM_ACCESSRIGHTS is not defined" in {
    val row = Map("DDM_ACCESSRIGHTS" -> "")
    accessCategory(2)(row) shouldBe empty
  }

  it should "fail if the DDM_ACCESSRIGHTS value does not correspond to an object in the enum" in {
    val row = Map("DDM_ACCESSRIGHTS" -> "unknown value")
    accessCategory(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid accessright", _)) =>
    }
  }

  "iso639_1Language" should "return true if the tag matches an ISO 639-1 language" in {
    isValidISO639_1Language("en") shouldBe true
  }

  it should "return false if the tag is too long" in {
    isValidISO639_1Language("eng") shouldBe false
  }

  it should "return false if the tag is too short" in {
    isValidISO639_1Language("e") shouldBe false
  }

  it should "return false if the tag does not match a Locale" in {
    isValidISO639_1Language("ac") shouldBe false
  }

  "iso639_2Language (with normal 3-letter tag)" should behave like validLanguage3Tag(parser, "eng")

  "iso639_2Language (with terminology tag)" should behave like validLanguage3Tag(parser, "nld")

  "iso639_2Language (with bibliographic tag)" should behave like validLanguage3Tag(parser, "dut")

  "iso639_2Language (with a random tag)" should behave like invalidLanguage3Tag(parser, "abc")

  "iso639_2Language (with some obscure language tag no one has ever heard about)" should behave like validLanguage3Tag(parser, "day")

  "iso639_2Language (with a 2-letter tag)" should behave like invalidLanguage3Tag(parser, "nl")

  "iso639_2Language (with a too short tag)" should behave like invalidLanguage3Tag(parser, "a")

  "iso639_2Language (with a too long tag)" should behave like invalidLanguage3Tag(parser, "abcdef")

  "iso639_2Language (with encoding tag)" should behave like invalidLanguage3Tag(parser, "encoding=UTF-8")

  "creator" should "return None if none of the fields are defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> ""
    )

    contributor(2)(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CREATOR_ORGANIZATION is defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> ""
    )

    creator(2)(row).value should matchPattern { case Success(CreatorOrganization("org")) => }
  }

  it should "succeed with a person when only DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Success(CreatorPerson(None, "A.", None, "Jones", None, None)) =>
    }
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "X",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "org",
      "DCX_CREATOR_DAI" -> "dai123"
    )

    creator(2)(row).value should matchPattern {
      case Success(CreatorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some("dai123"))) =>
    }
  }

  it should "fail if DCX_CREATOR_INITIALS is not defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "Jones",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CREATOR_INITIALS]", _)) =>
    }
  }

  it should "fail if DCX_CREATOR_SURNAME is not defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "A.",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CREATOR_SURNAME]", _)) =>
    }
  }

  it should "fail if DCX_CREATOR_INITIALS and DCX_CREATOR_SURNAME are both not defined" in {
    val row = Map(
      "DCX_CREATOR_TITLES" -> "Dr.",
      "DCX_CREATOR_INITIALS" -> "",
      "DCX_CREATOR_INSERTIONS" -> "",
      "DCX_CREATOR_SURNAME" -> "",
      "DCX_CREATOR_ORGANIZATION" -> "",
      "DCX_CREATOR_DAI" -> ""
    )

    creator(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CREATOR_SURNAME, DCX_CREATOR_INITIALS]", _)) =>
    }
  }

  "contributor" should "return None if the none of the fields are defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> ""
    )

    contributor(2)(row) shouldBe empty
  }

  it should "succeed with an organisation when only the DCX_CONTRIBUTOR_ORGANIZATION is defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> ""
    )

    contributor(2)(row).value should matchPattern { case Success(ContributorOrganization("org")) => }
  }

  it should "succeed with a person when only DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Success(ContributorPerson(None, "A.", None, "Jones", None, None)) =>
    }
  }

  it should "succeed with a more extensive person when more fields are filled in" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "X",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "org",
      "DCX_CONTRIBUTOR_DAI" -> "dai123"
    )

    contributor(2)(row).value should matchPattern {
      case Success(ContributorPerson(Some("Dr."), "A.", Some("X"), "Jones", Some("org"), Some("dai123"))) =>
    }
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS is not defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "Jones",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CONTRIBUTOR_INITIALS]", _)) =>
    }
  }

  it should "fail if DCX_CONTRIBUTOR_SURNAME is not defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "A.",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CONTRIBUTOR_SURNAME]", _)) =>
    }
  }

  it should "fail if DCX_CONTRIBUTOR_INITIALS and DCX_CONTRIBUTOR_SURNAME are both not defined" in {
    val row = Map(
      "DCX_CONTRIBUTOR_TITLES" -> "Dr.",
      "DCX_CONTRIBUTOR_INITIALS" -> "",
      "DCX_CONTRIBUTOR_INSERTIONS" -> "",
      "DCX_CONTRIBUTOR_SURNAME" -> "",
      "DCX_CONTRIBUTOR_ORGANIZATION" -> "",
      "DCX_CONTRIBUTOR_DAI" -> ""
    )

    contributor(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_CONTRIBUTOR_SURNAME, DCX_CONTRIBUTOR_INITIALS]", _)) =>
    }
  }

  "identifier" should "return None if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are not defined" in {
    val row = Map(
      "DC_IDENTIFIER" -> "",
      "DC_IDENTIFIER_TYPE" -> ""
    )
    identifier(2)(row) shouldBe empty
  }

  it should "fail if DC_IDENTIFIER_TYPE is defined, but DC_IDENTIFIER is not" in {
    val row = Map(
      "DC_IDENTIFIER" -> "",
      "DC_IDENTIFIER_TYPE" -> "ISSN"
    )
    identifier(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DC_IDENTIFIER]", _)) =>
    }
  }

  it should "succeed if DC_IDENTIFIER is defined and DC_IDENTIFIER_TYPE is not" in {
    val row = Map(
      "DC_IDENTIFIER" -> "id",
      "DC_IDENTIFIER_TYPE" -> ""
    )
    identifier(2)(row).value should matchPattern { case Success(Identifier("id", None)) => }
  }

  it should "succeed if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined and DC_IDENTIFIER_TYPE is valid" in {
    val row = Map(
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "ISSN"
    )
    identifier(2)(row).value should matchPattern { case Success(Identifier("123456", Some(IdentifierType.ISSN))) => }
  }

  it should "fail if both DC_IDENTIFIER and DC_IDENTIFIER_TYPE are defined, but DC_IDENTIFIER_TYPE is invalid" in {
    val row = Map(
      "DC_IDENTIFIER" -> "123456",
      "DC_IDENTIFIER_TYPE" -> "INVALID_IDENTIFIER_TYPE"
    )
    identifier(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'INVALID_IDENTIFIER_TYPE' is not a valid identifier type", _)) =>
    }
  }

  "dcType" should "convert the value for DC_TYPE into the corresponding enum object" in {
    val row = Map("DC_TYPE" -> "Collection")
    dcType(2)(row).value should matchPattern { case Success(DcType.COLLECTION) => }
  }

  it should "return None if DC_TYPE is not defined" in {
    val row = Map("DC_TYPE" -> "")
    dcType(2)(row) shouldBe empty
  }

  it should "fail if the DC_TYPE value does not correspond to an object in the enum" in {
    val row = Map("DC_TYPE" -> "unknown value")
    dcType(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid type", _)) =>
    }
  }

  "relation" should "fail if both the link and title are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Only one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined", _)) =>
    }
  }

  it should "fail if the qualifier and both the link and title are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Only one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined", _)) =>
    }
  }

  it should "succeed when only the qualifier and link are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row).value should matchPattern {
      case Success(QualifiedLinkRelation("replaces", "foo")) =>
    }
  }

  it should "succeed when only the qualifier and title are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern {
      case Success(QualifiedTitleRelation("replaces", "bar")) =>
    }
  }

  it should "fail if only the qualifier is defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "replaces",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row).value should matchPattern {
      case Failure(ParseException(2, "When DCX_RELATION_QUALIFIER is defined, one of the values [DCX_RELATION_LINK, DCX_RELATION_TITLE] must be defined as well", _)) =>
    }
  }

  it should "succeed if only the link is defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "foo",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row).value should matchPattern { case Success(LinkRelation("foo")) => }
  }

  it should "succeed if only the title is defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> "bar"
    )

    relation(2)(row).value should matchPattern { case Success(TitleRelation("bar")) => }
  }

  it should "return None if none of these fields are defined" in {
    val row = Map(
      "DCX_RELATION_QUALIFIER" -> "",
      "DCX_RELATION_LINK" -> "",
      "DCX_RELATION_TITLE" -> ""
    )

    relation(2)(row) shouldBe empty
  }

  "subject" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex"
    )

    subject(2)(row).value should matchPattern { case Success(Subject("IX", Some("abr:ABRcomplex"))) => }
  }

  it should "succeed when the scheme is not defined" in {
    val row = Map(
      "DC_SUBJECT" -> "test",
      "DC_SUBJECT_SCHEME" -> ""
    )

    subject(2)(row).value should matchPattern { case Success(Subject("test", None)) => }
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = Map(
      "DC_SUBJECT" -> "",
      "DC_SUBJECT_SCHEME" -> "abr:ABRcomplex"
    )

    subject(2)(row).value should matchPattern { case Success(Subject("", Some("abr:ABRcomplex"))) => }
  }

  it should "fail if the scheme is not recognized" in {
    val row = Map(
      "DC_SUBJECT" -> "IX",
      "DC_SUBJECT_SCHEME" -> "random-incorrect-scheme"
    )

    subject(2)(row).value should matchPattern {
      case Failure(ParseException(2, "The given value for DC_SUBJECT_SCHEME is not allowed. This can only be 'abr:ABRcomplex'", _)) =>
    }
  }

  it should "return None when both fields are empty or blank" in {
    val row = Map(
      "DC_SUBJECT" -> "",
      "DC_SUBJECT_SCHEME" -> ""
    )

    subject(2)(row) shouldBe empty
  }

  "temporal" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    )

    temporal(2)(row).value should matchPattern {
      case Success(Temporal("PALEOLB", Some("abr:ABRperiode"))) =>
    }
  }

  it should "succeed when the scheme is not defined" in {
    val row = Map(
      "DCT_TEMPORAL" -> "test",
      "DCT_TEMPORAL_SCHEME" -> ""
    )

    temporal(2)(row).value should matchPattern { case Success(Temporal("test", None)) => }
  }

  it should "succeed when only the scheme is defined (empty String for the temporal)" in {
    val row = Map(
      "DCT_TEMPORAL" -> "",
      "DCT_TEMPORAL_SCHEME" -> "abr:ABRperiode"
    )

    temporal(2)(row).value should matchPattern { case Success(Temporal("", Some("abr:ABRperiode"))) => }
  }

  it should "fail if the scheme is not recognized" in {
    val row = Map(
      "DCT_TEMPORAL" -> "PALEOLB",
      "DCT_TEMPORAL_SCHEME" -> "random-incorrect-scheme"
    )

    temporal(2)(row).value should matchPattern {
      case Failure(ParseException(2, "The given value for DCT_TEMPORAL_SCHEME is not allowed. This can only be 'abr:ABRperiode'", _)) =>
    }
  }

  it should "return None when both fields are empty or blank" in {
    val row = Map(
      "DCT_TEMPORAL" -> "",
      "DCT_TEMPORAL_SCHEME" -> ""
    )

    temporal(2)(row) shouldBe empty
  }

  "spatialPoint" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> "degrees"
    )

    spatialPoint(2)(row).value should matchPattern {
      case Success(SpatialPoint("12", "34", Some("degrees"))) =>
    }
  }

  it should "succeed when no scheme is defined" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "34",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialPoint(2)(row).value should matchPattern { case Success(SpatialPoint("12", "34", None)) => }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialPoint(2)(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = Map(
      "DCX_SPATIAL_X" -> "12",
      "DCX_SPATIAL_Y" -> "",
      "DCX_SPATIAL_SCHEME" -> "degrees"
    )

    spatialPoint(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_SPATIAL_Y]", _)) =>
    }
  }

  "spatialBox" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> "RD"
    )

    spatialBox(2)(row).value should matchPattern {
      case Success(SpatialBoxx("45", "34", "23", "12", Some("RD"))) =>
    }
  }

  it should "succeed when no scheme is defined" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "23",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "45",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialBox(2)(row).value should matchPattern {
      case Success(SpatialBoxx("45", "34", "23", "12", None)) =>
    }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_SOUTH" -> "",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SCHEME" -> ""
    )

    spatialBox(2)(row) shouldBe empty
  }

  it should "fail if any of the required fields is missing" in {
    val row = Map(
      "DCX_SPATIAL_WEST" -> "12",
      "DCX_SPATIAL_EAST" -> "",
      "DCX_SPATIAL_SOUTH" -> "34",
      "DCX_SPATIAL_NORTH" -> "",
      "DCX_SPATIAL_SCHEME" -> "RD"
    )

    spatialBox(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value(s) for: [DCX_SPATIAL_NORTH, DCX_SPATIAL_EAST]", _)) =>
    }
  }

  "springfield" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection"
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield("randomdomain", "randomuser", "randomcollection")) =>
    }
  }

  it should "convert with a default value for SF_DOMAIN when it is not defined" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> "randomcollection"
    )

    springfield(2)(row).value should matchPattern {
      case Success(Springfield("dans", "randomuser", "randomcollection")) =>
    }
  }

  it should "fail if there is no value for SF_USER" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "",
      "SF_COLLECTION" -> "randomcollection"
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: SF_USER", _)) =>
    }
  }

  it should "fail if there is no value for SF_COLLECTION" in {
    val row = Map(
      "SF_DOMAIN" -> "randomdomain",
      "SF_USER" -> "randomuser",
      "SF_COLLECTION" -> ""
    )

    springfield(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Missing value for: SF_COLLECTION", _)) =>
    }
  }

  it should "fail if the values have invalid characters" in {
    val row = Map(
      "SF_DOMAIN" -> "inv@lïdçhæracter",
      "SF_USER" -> "#%!&@$",
      "SF_COLLECTION" -> "inv***d"
    )

    inside(springfield(2)(row).value) {
      case Failure(CompositeException(es)) =>
        val e1 :: e2 :: e3 :: Nil = es.toList
        e1 should have message "The column 'SF_DOMAIN' contains the following invalid characters: {@, ï, ç, æ}"
        e2 should have message "The column 'SF_USER' contains the following invalid characters: {#, %, !, &, @, $}"
        e3 should have message "The column 'SF_COLLECTION' contains the following invalid characters: {*}"
    }
  }

  it should "return None if there is no value for any of these keys" in {
    val row = Map(
      "SF_DOMAIN" -> "",
      "SF_USER" -> "",
      "SF_COLLECTION" -> ""
    )

    springfield(2)(row) shouldBe empty
  }

  "avFile" should "convert the csv input into the corresponding object" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile, Some("en")))
    avFile(2)(row).value should matchPattern { case Success((`file`, `title`, `subtitles`)) => }
  }

  it should "fail if the value for AV_FILE represents a path that does not exist" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/path/to/file/that/does/not/exist.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/path/to/file/that/does/not/exist.mpg").getAbsoluteFile
    inside(avFile(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"AV_FILE file '$file' does not exist"
    }
  }

  it should "fail if the value for AV_FILE represents a path that does not exist when AV_SUBTITLES is not defined" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/path/to/file/that/does/not/exist.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/path/to/file/that/does/not/exist.mpg").getAbsoluteFile
    inside(avFile(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"AV_FILE file '$file' does not exist"
    }
  }

  it should "fail if the value for AV_SUBTITLES represents a path that does not exist" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/path/to/file/that/does/not/exist.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/path/to/file/that/does/not/exist.srt").getAbsoluteFile
    inside(avFile(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"AV_SUBTITLES file '$file' does not exist"
    }
  }

  it should "fail if the value for AV_SUBTITLES_LANGUAGE does not represent an ISO 639-1 language value" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "ac"
    )

    avFile(2)(row).value should matchPattern {
      case Failure(ParseException(2, "AV_SUBTITLES_LANGUAGE 'ac' doesn't have a valid ISO 639-1 language value", _)) =>
    }
  }

  it should "fail if there is no AV_SUBTITLES value, but there is a AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    inside(avFile(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg shouldBe s"Missing value for AV_SUBTITLES, since AV_SUBTITLES_LANGUAGE does have a value: 'en'"
    }
  }

  it should "succeed if there is a value for AV_SUBTITLES, but no value for AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile))
    avFile(2)(row).value should matchPattern { case Success((`file`, `title`, `subtitles`)) => }
  }

  it should "succeed if there is no value for both AV_SUBTITLES and AV_SUBTITLES_LANGUAGE" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val title = Some("rolling stone")
    avFile(2)(row).value should matchPattern { case Success((`file`, `title`, None)) => }
  }

  it should "succeed if there is no value for AV_FILE_TITLE" in {
    val row = Map(
      "AV_FILE" -> "ruimtereis01/reisverslag/centaur.mpg",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    val file = new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.mpg").getAbsoluteFile
    val subtitles = Some(Subtitles(new File(settings.multidepositDir, "ruimtereis01/reisverslag/centaur.srt").getAbsoluteFile))
    avFile(2)(row).value should matchPattern { case Success((`file`, None, `subtitles`)) => }
  }

  it should "fail if there is no value for AV_FILE, but the other three do have values" in {
    val row = Map(
      "AV_FILE" -> "",
      "AV_FILE_TITLE" -> "rolling stone",
      "AV_SUBTITLES" -> "ruimtereis01/reisverslag/centaur.srt",
      "AV_SUBTITLES_LANGUAGE" -> "en"
    )

    inside(avFile(2)(row).value) {
      case Failure(ParseException(2, msg, _)) =>
        msg should include("No value is defined for AV_FILE")
    }
  }

  it should "return None if all four values do not have any value" in {
    val row = Map(
      "AV_FILE" -> "",
      "AV_FILE_TITLE" -> "",
      "AV_SUBTITLES" -> "",
      "AV_SUBTITLES_LANGUAGE" -> ""
    )

    avFile(2)(row) shouldBe empty
  }

  "fileAccessRight" should "convert the value for SF_ACCESSIBILITY into the corresponding enum object" in {
    val row = Map("SF_ACCESSIBILITY" -> "NONE")
    fileAccessRight(2)(row).value should matchPattern { case Success(FileAccessRights.NONE) => }
  }

  it should "return None if SF_ACCESSIBILITY is not defined" in {
    val row = Map("SF_ACCESSIBILITY" -> "")
    fileAccessRight(2)(row) shouldBe empty
  }

  it should "fail if the SF_ACCESSIBILITY value does not correspond to an object in the enum" in {
    val row = Map("SF_ACCESSIBILITY" -> "unknown value")
    fileAccessRight(2)(row).value should matchPattern {
      case Failure(ParseException(2, "Value 'unknown value' is not a valid file accessright", _)) =>
    }
  }
}
