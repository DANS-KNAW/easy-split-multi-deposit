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
import org.joda.time.DateTime

class ParserUtilsSpec extends TestSupportFixture {
  self =>

  private val parser = new ParserUtils with InputPathExplorer {
    val multiDepositDir: File = self.multiDepositDir
  }

  import parser._

  "extractExactlyOne" should "find the value for the given rows" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
    )

    extractExactlyOne(2, "FOO", rows).value shouldBe "abc"
  }

  it should "filter out the blank values" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "")),
    )

    extractExactlyOne(2, "BAR", rows).value shouldBe "def"
  }

  it should "fail when the output is empty" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractExactlyOne(2, "QUX", rows).invalidValue shouldBe
      ParseError(2, "There should be one non-empty value for QUX").chained
  }

  it should "fail when the input contains multiple distinct values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractExactlyOne(2, "FOO", rows).invalidValue shouldBe
      ParseError(2, "Only one row is allowed to contain a value for the column 'FOO'. Found: [abc, ghi]").chained
  }

  it should "succeed when the input contains multiple identical values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "jkl")),
    )

    extractExactlyOne(2, "FOO", rows).value shouldBe "abc"
  }

  "extractAtLeastOne" should "find the values for the given rows" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractAtLeastOne(2, "FOO", rows).value.toList should contain inOrderOnly("abc", "ghi")
  }

  it should "filter out the blank values" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "")),
    )

    extractAtLeastOne(2, "BAR", rows).value.toList should contain only "def"
  }

  it should "fail when the output is empty" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractAtLeastOne(2, "QUX", rows).invalidValue shouldBe
      ParseError(2, "There should be at least one non-empty value for QUX").chained
  }

  it should "succeed when the input contains multiple identical values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "jkl")),
    )

    extractAtLeastOne(2, "FOO", rows).value.toList should contain only "abc"
  }

  "extractAtMostOne" should "find the value for the given rows" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
    )

    extractAtMostOne(2, "FOO", rows).value.value shouldBe "abc"
  }

  it should "filter out the blank values" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "")),
    )

    extractAtMostOne(2, "BAR", rows).value.value shouldBe "def"
  }

  it should "return a None when the output is empty" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractAtMostOne(2, "QUX", rows).value shouldBe empty
  }

  it should "fail when the input contains multiple distinct values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractAtMostOne(2, "FOO", rows).invalidValue shouldBe
      ParseError(2, "At most one row is allowed to contain a value for the column 'FOO'. Found: [abc, ghi]").chained
  }

  it should "succeed when the input contains multiple identical values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "jkl")),
    )

    extractAtMostOne(2, "FOO", rows).value.value shouldBe "abc"
  }

  "extractList curried" should "for each row run the given function and collect the results" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(3, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractList(rows)(i => Some(i.rowNum.toValidated)).value should contain inOrderOnly(2, 3)
  }

  it should "leave out the rows for which the function returns a None" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(3, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractList(rows) {
      case DepositRow(rowNum, _) if rowNum % 2 == 0 => Some(rowNum.toValidated)
      case _ => None
    }.value should contain only 2
  }

  it should "iterate over all rows and aggregate all errors until the end" in {
    val rows = List(
      DepositRow(2, Map("FOO" -> "abc", "BAR" -> "def")),
      DepositRow(3, Map("FOO" -> "ghi", "BAR" -> "jkl")),
    )

    extractList(rows)(i => Some(ParseError(i.rowNum, s"foo ${ i.rowNum }").toInvalid))
      .invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "foo 2"),
      ParseError(3, "foo 3")
    )
  }

  "checkValidChars" should "succeed with the input value when all characters are valid" in {
    checkValidChars("valid-input", 2, "TEST").value shouldBe "valid-input"
  }

  it should "fail when the input contains invalid characters" in {
    checkValidChars("#$%", 2, "TEST").invalidValue shouldBe
      ParseError(2, "The column 'TEST' contains the following invalid characters: {#, $, %}").chained
  }

  "date" should "convert the value of the date into the corresponding object" in {
    date(2, "datum")("2016-07-30").value shouldBe DateTime.parse("2016-07-30")
  }

  it should "fail if the value does not represent a date" in {
    date(2, "datum")("you can't parse me!").invalidValue shouldBe
      ParseError(2, "datum value 'you can't parse me!' does not represent a date").chained
  }

  "missingRequired" should "return a ParseError listing the one missing column" in {
    val row = DepositRow(2, Map("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4"))

    missingRequired(row, Set("a", "b", "c", "d", "e")) shouldBe ParseError(2, "Missing value for: e")
  }

  it should "return a ParseError listing the missing columns" in {
    val row = DepositRow(2, Map("a" -> "1", "b" -> "2", "c" -> "3", "d" -> ""))

    missingRequired(row, Set("a", "b", "c", "d", "e")) shouldBe ParseError(2, "Missing value(s) for: [d, e]")
  }

  it should "throw an IllegalArgumentException if no columns were missing" in {
    val row = DepositRow(2, Map("a" -> "1", "b" -> "2", "c" -> "3"))

    the[IllegalArgumentException] thrownBy missingRequired(row, Set("a", "b", "c")) should have message "requirement failed: the list of missing elements is supposed to be non-empty"
  }
}
