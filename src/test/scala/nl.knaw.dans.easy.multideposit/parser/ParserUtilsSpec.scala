/*
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

import java.net.URI

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
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
    )

    extractExactlyOne(2, Headers.Title, rows).value shouldBe "abc"
  }

  it should "filter out the blank values" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "")),
    )

    extractExactlyOne(2, Headers.Description, rows).value shouldBe "def"
  }

  it should "fail when the output is empty" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractExactlyOne(2, Headers.Creator, rows).invalidValue shouldBe
      ParseError(2, "There should be one non-empty value for DC_CREATOR").chained
  }

  it should "fail when the input contains multiple distinct values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractExactlyOne(2, Headers.Title, rows).invalidValue shouldBe
      ParseError(2, "Only one row is allowed to contain a value for the column 'DC_TITLE'. Found: [abc, ghi]").chained
  }

  it should "succeed when the input contains multiple identical values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "jkl")),
    )

    extractExactlyOne(2, Headers.Title, rows).value shouldBe "abc"
  }

  "extractAtLeastOne" should "find the values for the given rows" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractAtLeastOne(2, Headers.Title, rows).value.toList should contain inOrderOnly("abc", "ghi")
  }

  it should "filter out the blank values" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "")),
    )

    extractAtLeastOne(2, Headers.Description, rows).value.toList should contain only "def"
  }

  it should "fail when the output is empty" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractAtLeastOne(2, Headers.Creator, rows).invalidValue shouldBe
      ParseError(2, "There should be at least one non-empty value for DC_CREATOR").chained
  }

  it should "succeed when the input contains multiple identical values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "jkl")),
    )

    extractAtLeastOne(2, Headers.Title, rows).value.toList should contain only "abc"
  }

  "extractAtMostOne" should "find the value for the given rows" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
    )

    extractAtMostOne(2, Headers.Title, rows).value.value shouldBe "abc"
  }

  it should "filter out the blank values" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "")),
    )

    extractAtMostOne(2, Headers.Description, rows).value.value shouldBe "def"
  }

  it should "return a None when the output is empty" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractAtMostOne(2, Headers.Creator, rows).value shouldBe empty
  }

  it should "fail when the input contains multiple distinct values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractAtMostOne(2, Headers.Title, rows).invalidValue shouldBe
      ParseError(2, "At most one row is allowed to contain a value for the column 'DC_TITLE'. Found: [abc, ghi]").chained
  }

  it should "succeed when the input contains multiple identical values for the same columnName" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "jkl")),
    )

    extractAtMostOne(2, Headers.Title, rows).value.value shouldBe "abc"
  }

  "extractList curried" should "for each row run the given function and collect the results" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(3, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractList(rows)(i => Some(i.rowNum.toValidated)).value should contain inOrderOnly(2, 3)
  }

  it should "leave out the rows for which the function returns a None" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(3, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractList(rows) {
      case DepositRow(rowNum, _) if rowNum % 2 == 0 => Some(rowNum.toValidated)
      case _ => None
    }.value should contain only 2
  }

  it should "iterate over all rows and aggregate all errors until the end" in {
    val rows = List(
      DepositRow(2, Map(Headers.Title -> "abc", Headers.Description -> "def")),
      DepositRow(3, Map(Headers.Title -> "ghi", Headers.Description -> "jkl")),
    )

    extractList(rows)(i => Some(ParseError(i.rowNum, s"foo ${ i.rowNum }").toInvalid))
      .invalidValue.toNonEmptyList.toList should contain inOrderOnly(
      ParseError(2, "foo 2"),
      ParseError(3, "foo 3")
    )
  }

  "checkValidChars" should "succeed with the input value when all characters are valid" in {
    checkValidChars("valid-input", 2, Headers.Title).value shouldBe "valid-input"
  }

  it should "fail when the input contains invalid characters" in {
    checkValidChars("#$%", 2, Headers.Title).invalidValue shouldBe
      ParseError(2, "The column 'DC_TITLE' contains the following invalid characters: {#, $, %}").chained
  }

  "date" should "convert the value of the date into the corresponding object" in {
    date(2, Headers.Date)("2016-07-30").value shouldBe DateTime.parse("2016-07-30")
  }

  it should "fail if the value does not represent a date" in {
    date(2, Headers.Date)("you can't parse me!").invalidValue shouldBe
      ParseError(2, "DCT_DATE value 'you can't parse me!' does not represent a date").chained
  }

  "url" should "convert the value of a URI into the corresponding object" in {
    uri(2, Headers.RelationLink)("http://does.not.exist.dans.knaw.nl/").value shouldBe new URI("http://does.not.exist.dans.knaw.nl/")
  }

  it should "fail if the value does not represent a URI" in {
    uri(2, Headers.RelationLink)("you can't parse me!").invalidValue shouldBe
      ParseError(2, "DCX_RELATION_LINK value 'you can't parse me!' is not a valid URI").chained
  }

  it should "fail if the value does not represent a URI with one of the accepted schemes" in {
    uri(2, Headers.RelationLink)("javascript://hello-world").invalidValue shouldBe
      ParseError(2, "DCX_RELATION_LINK value 'javascript://hello-world' is a valid URI but doesn't have one of the accepted protocols: {http, https}").chained
  }

  "missingRequired" should "return a ParseError listing the one missing column" in {
    val row = DepositRow(2, Map(Headers.Title -> "1", Headers.Description -> "2", Headers.Subject -> "3", Headers.Temporal -> "4"))

    missingRequired(row, Headers.Title, Headers.Description, Headers.Subject, Headers.Temporal, Headers.Language) shouldBe ParseError(2, "Missing value for: DC_LANGUAGE")
  }

  it should "return a ParseError listing the missing columns" in {
    val row = DepositRow(2, Map(Headers.Title -> "1", Headers.Description -> "2", Headers.Subject -> "3", Headers.Temporal -> ""))

    missingRequired(row, Headers.Title, Headers.Description, Headers.Subject, Headers.Temporal, Headers.Language) shouldBe ParseError(2, "Missing value(s) for: [DCT_TEMPORAL, DC_LANGUAGE]")
  }

  it should "throw an IllegalArgumentException if no columns were missing" in {
    val row = DepositRow(2, Map(Headers.Title -> "1", Headers.Description -> "2", Headers.Subject -> "3"))

    the[IllegalArgumentException] thrownBy missingRequired(row, Headers.Title, Headers.Description, Headers.Subject) should have message "requirement failed: the list of missing elements is supposed to be non-empty"
  }
}
