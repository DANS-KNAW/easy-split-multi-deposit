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

import java.util.UUID
import java.util.UUID.fromString

import better.files.File
import cats.data.{ Chain, NonEmptyList }
import cats.data.Validated.{ Invalid, Valid }
import nl.knaw.dans.easy.multideposit.TestSupportFixture
import nl.knaw.dans.easy.multideposit.model.Instructions
import org.scalatest.BeforeAndAfterEach

trait DepositTestObjects extends AudioVideoTestObjects
  with FileDescriptorTestObjects
  with FileMetadataTestObjects
  with MetadataTestObjects
  with ProfileTestObjects {
  this: TestSupportFixture =>

  lazy val depositCSV @ depositCSVRow1 :: depositCSVRow2 :: Nil = List(
    Map("DATASET" -> "ruimtereis01", "DEPOSITOR_ID" -> "ikke", "BASE_REVISION" ->"1de3f841-0f0d-048b-b3db-4b03ad4834d7") ++ profileCSVRow1 ++ metadataCSVRow1 ++ fileDescriptorCSVRow1 ++ audioVideoCSVRow1,
    Map("DATASET" -> "ruimtereis01", "BASE_REVISION" -> "") ++ profileCSVRow2 ++ metadataCSVRow2 ++ fileDescriptorCSVRow2 ++ audioVideoCSVRow2,
  )

  lazy val depositCSVRow = List(
    DepositRow(2, depositCSVRow1),
    DepositRow(3, depositCSVRow2),
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

    if (multiDepositDir.exists) multiDepositDir.delete()
    File(getClass.getResource("/allfields/input").toURI).copyTo(multiDepositDir)
  }

  private val parser = new MultiDepositParser {
    override val multiDepositDir: File = self.multiDepositDir
    override val userLicenses: Set[String] = self.userLicenses
  }

  import parser._

  "extractDeposit" should "fail if the depositId contains invalid characters" in {
    extractDeposit(multiDepositDir)("ruimtereis01#", depositCSVRow).left.value shouldBe
      Chain(ParseError(2, "The column 'DATASET' contains the following invalid characters: {#}"))
  }

  "parse" should "load the input csv file into the object model" in {
    val instructionsFile = multiDepositDir / "instructions.csv"
    instructionsFile.toJava should exist

    inside(MultiDepositParser.parse(testDir / "md", self.userLicenses)) {
      case Right(datasets) =>
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
    val file = testDir / "input.csv"
    file.write(csv)

    val expectedHeaders = List("DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      2 -> List("abc", "def", "ghi", "jkl"),
      3 -> List("mno", "pqr", "stu", "vwx"),
      4 -> List("yzy", "xwv", "uts", "rqp"),
      5 -> List("onm", "lkj", "ihg", "fed"),
      6 -> List("cba", "abc", "def", "ghi")
    )

    read(file).right.value shouldBe (expectedHeaders, expectedData)
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
    val file = testDir / "input.csv"
    file.write(csv)

    val expectedHeaders = List("DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      2 -> List("a  bc", "def", "ghi", "jkl"),
      3 -> List("mno", "pq\nr", "stu", "vwx"),
      4 -> List("yzy", "xwv", "uts", "rqp"),
      5 -> List("onm", "lkj", "ihg", "fed"),
      6 -> List("cba", "abc", "def", "ghi")
    )

    read(file).right.value shouldBe (expectedHeaders, expectedData)
  }

  it should "parse the input when some cells are empty" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,,jkl
        |mno,,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir / "input.csv"
    file.write(csv)

    val expectedHeaders = List("DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      2 -> List("abc", "def", "", "jkl"),
      3 -> List("mno", "", "stu", "vwx"),
      4 -> List("yzy", "xwv", "uts", "rqp"),
      5 -> List("onm", "lkj", "", "fed"),
      6 -> List("cba", "abc", "def", "ghi")
    )

    read(file).right.value shouldBe (expectedHeaders, expectedData)
  }

  it should "parse the input when some cells are blank and leave these cells empty in the result" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,  ,stu,vwx
        |,xwv,uts,rqp
        |onm,lkj, ,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir / "input.csv"
    file.write(csv)

    val expectedHeaders = List("DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      2 -> List("abc", "def", "ghi", "jkl"),
      3 -> List("mno", "", "stu", "vwx"),
      4 -> List("", "xwv", "uts", "rqp"),
      5 -> List("onm", "lkj", "", "fed"),
      6 -> List("cba", "abc", "def", "ghi")
    )

    read(file).right.value shouldBe (expectedHeaders, expectedData)
  }

  it should "parse the input while leaving out blank rows" in {
    val csv =
      """DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir / "input.csv"
    file.write(csv)

    val expectedHeaders = List("DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List(
      2 -> List("abc", "def", "ghi", "jkl"),
      3 -> List("mno", "pqr", "stu", "vwx"),
      5 -> List("onm", "lkj", "ihg", "fed"),
      6 -> List("cba", "abc", "def", "ghi")
    )

    read(file).right.value shouldBe (expectedHeaders, expectedData)
  }

  it should "parse the input if it only contains a row of headers and no data" in {
    val csv = "DATASET,DEPOSITOR_ID,SF_USER,SF_DOMAIN"
    val file = testDir / "input.csv"
    file.write(csv)

    val expectedHeaders = List("DATASET", "DEPOSITOR_ID", "SF_USER", "SF_DOMAIN")
    val expectedData = List.empty[(Int, String)]

    read(file).right.value shouldBe (expectedHeaders, expectedData)
  }

  it should "fail when the input csv file is empty" in {
    val csv = ""
    val file = testDir / "input.csv"
    file.write(csv)

    read(file).left.value shouldBe EmptyInstructionsFileError(file)
  }

  it should "fail when the input contains invalid headers" in {
    val csv =
      """DATASET,foo,SF_USER,SF_DOMAIN
        |abc,def,ghi,jkl
        |mno,pqr,stu,vwx
        |yzy,xwv,uts,rqp
        |onm,lkj,ihg,fed
        |cba,abc,def,ghi""".stripMargin
    val file = testDir / "input.csv"
    file.write(csv)

    inside(read(file).left.value) {
      case ParseError(0, msg) =>
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
    val file = testDir / "input.csv"
    file.write(csv)

    inside(read(file).left.value) {
      case ParseError(0, msg) =>
        msg should include("duplicate headers: [DEPOSITOR_ID]")
    }
  }

  "detectEmptyDepositCells" should "succeed when no elements in the input are empty" in {
    val dsIds = List("ds1", "ds1", "ds2", "ds2", "ds2", "ds3")

    detectEmptyDepositCells(dsIds) shouldBe a[Right[_, _]]
  }

  it should "fail when any number of elements in the input are blank" in {
    val dsIds = List("ds1", "", "ds2", "ds2", "   ", "ds3")

    detectEmptyDepositCells(dsIds).left.value.toNonEmptyList shouldBe NonEmptyList.of(
      ParseError(3, "Row 3 does not have a depositId in column DATASET"),
      ParseError(6, "Row 6 does not have a depositId in column DATASET"),
    )
  }

  "extractInstructions" should "convert the csv input to the corresponding output" in {
    extractInstructions("ruimtereis01", 2, depositCSVRow) shouldBe Valid(instructions)
  }

  it should "fail if there are multiple distinct depositorUserIDs" in {
    val rows = DepositRow(2, depositCSVRow1) ::
      DepositRow(3, depositCSVRow2 + ("DEPOSITOR_ID" -> "ikke2")) ::
      Nil

    extractInstructions("ruimtereis01", 2, rows) shouldBe
      Invalid(Chain(ParseError(2, "Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [ikke, ikke2]")))
  }

  it should "succeed if there are multiple depositorUserIDs that are all equal" in {
    val rows = DepositRow(2, depositCSVRow1) ::
      DepositRow(2, depositCSVRow2 + ("DEPOSITOR_ID" -> "ikke")) ::
      Nil

    extractInstructions("ruimtereis01", 2, rows) shouldBe Valid(instructions)
  }

  it should "fail if there are multiple distinct base revisions" in {
    val row = DepositRow(3, Map("DATASET" -> "ruimtereis01", "BASE_REVISION" -> "9de3f841-0f0d-048b-b3db-4b03ad4834d7") ++ profileCSVRow2 ++ metadataCSVRow2 ++ fileDescriptorCSVRow2 ++ audioVideoCSVRow2)
    val rows = DepositRow(2, depositCSVRow1) :: row :: Nil

    extractInstructions("ruimtereis01", 2, rows) shouldBe
      Invalid(Chain(ParseError(2, "At most one row is allowed to contain a value for the column 'BASE_REVISION'. Found: [1de3f841-0f0d-048b-b3db-4b03ad4834d7, 9de3f841-0f0d-048b-b3db-4b03ad4834d7]")))
  }

  it should "not fail if there are multiple nondistinct base revisions" in {
    val row = DepositRow(3, Map("DATASET" -> "ruimtereis01", "BASE_REVISION" -> "1de3f841-0f0d-048b-b3db-4b03ad4834d7") ++ profileCSVRow2 ++ metadataCSVRow2 ++ fileDescriptorCSVRow2 ++ audioVideoCSVRow2)
    val rows = DepositRow(2, depositCSVRow1) :: row :: Nil

    extractInstructions("ruimtereis01", 2, rows) shouldBe a[Valid[_]]
  }

  "uuid" should "fail if the base revision does not conform to uuid format" in {
    uuid(2, "BASE_REVISION")("abcd-12xy").left.value shouldBe
      ParseError(2, "BASE_REVISION value 'abcd-12xy' does not conform to the UUID format")
  }

  it should "not fail if the base revision conforms to uuid format" in {
    val uuidString = "1de3f841-0f0d-048b-b3db-4b03ad4834d7"
    uuid(2, "BASE_REVISION")(uuidString).right.value shouldBe Some(UUID.fromString(uuidString))
  }
}
