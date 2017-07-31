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

import nl.knaw.dans.easy.multideposit.model._
import nl.knaw.dans.easy.multideposit.{ ParseException, UnitSpec }
import nl.knaw.dans.lib.error.CompositeException

import scala.util.{ Failure, Success }

trait LanguageBehavior {
  this: UnitSpec =>
  def validLanguage3Tag(parser: ParserUtils, lang: String): Unit = {
    it should "succeed when the language tag is valid" in {
      val row = Map("taal" -> lang)
      parser.iso639_2Language("taal")(2)(row).value should matchPattern { case Success(`lang`) => }
    }
  }

  def invalidLanguage3Tag(parser: ParserUtils, lang: String): Unit = {
    it should "fail when the language tag is invalid" in {
      val row = Map("taal" -> lang)
      val errorMsg = s"Value '$lang' is not a valid value for taal"
      parser.iso639_2Language("taal")(2)(row).value should matchPattern { case Failure(ParseException(2, `errorMsg`, _)) => }
    }
  }
}

class ParserUtilsSpec extends UnitSpec with LanguageBehavior {

  private val parser = new ParserUtils {}

  import parser._

  "getRowNum" should "extract the row number from the deposit row" in {
    val row = Map("ROW" -> "2", "TEST" -> "abc")

    getRowNum(row) shouldBe 2
  }

  it should "throw a NoSuchElementException when ROW is not a header in the deposit" in {
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
      case Failure(CompositeException(e1 :: e2 :: Nil)) =>
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
      case Failure(CompositeException(e1 :: e2 :: Nil)) =>
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
    checkValidChars("valid-input", 2, "TEST") should matchPattern { case Success("valid-input") => }
  }

  it should "fail when the input contains invalid characters" in {
    checkValidChars("#$%", 2, "TEST") should matchPattern {
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
}
