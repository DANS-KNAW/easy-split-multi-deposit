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
package nl.knaw.dans.easy.multideposit

import nl.knaw.dans.lib.error._

import scala.util.{ Failure, Try }

class MultiDepositSpec extends UnitSpec {

  "StringToOption.toOption" should "yield an Option.empty when given an empty String" in {
    "".toOption shouldBe empty
  }

  it should "yield an Option.empty when given a String with spaces only" in {
    "   ".toOption shouldBe empty
  }

  it should "yield the original String wrapped in Option when given a non-blank String" in {
    "abc".toOption.value shouldBe "abc"
  }

  "StringToOption.toIntOption" should "yield an Option.empty when given an empty String" in {
    "".toIntOption shouldBe empty
  }

  it should "yield an Option.empty when given a String with spaces only" in {
    "   ".toIntOption shouldBe empty
  }

  it should "yield an Option.empty when given a String with non-blank characters other than numbers" in {
    "abc".toIntOption shouldBe empty
  }

  it should "yield the parsed Int wrapped in Option when given a String with non-blank number characters" in {
    "30071992".toIntOption.value shouldBe 30071992
  }

  "TryExceptionHandling.onError" should "return the value when provided with a Success" in {
    Try("abc").getOrRecover(_ => "foobar") shouldBe "abc"
  }

  it should "return the onError value when provided with a failure" in {
    Try[String](throw new Exception).getOrRecover(_ => "foobar") shouldBe "foobar"
  }

  "TryExceptionHandling.combine" should "apply the function in the left Try to the value in the right Try" in {
    Try { (i: Int) => i + 1 } combine Try { 1 } shouldBe Try { 2 }
  }

  it should "fail if the function throws an exception" in {
    val err = new IllegalArgumentException("foo")
    Try { (_: Int) => throw err } combine Try { 1 } should matchPattern { case Failure(`err`) => }
  }

  it should "fail if the left Try is a Failure" in {
    val err = new IllegalArgumentException("foo")
    Try { throw err } combine Try { 1 } should matchPattern { case Failure(`err`) => }
  }

  it should "fail if the right Try is a Failure" in {
    val err = new IllegalArgumentException("foo")
    Try { (i: Int) => i + 1 } combine Try { throw err } should matchPattern { case Failure(`err`) => }
  }

  it should "fail if both sides are 'normal' exceptions" in {
    val err1 = new IllegalArgumentException("foo")
    val err2 = new IllegalArgumentException("bar")
    inside(Try { throw err1 } combine Try { throw err2 }) {
      case Failure(CompositeException(es)) => es should contain inOrderOnly(err1, err2)
    }
  }

  it should "fail if both sides are CompositeExceptions" in {
    val err1 = new IllegalArgumentException("foo")
    val err2 = new IllegalArgumentException("bar")
    val err3 = new IllegalArgumentException("baz")
    val err4 = new IllegalArgumentException("qux")

    val left = List[Try[Int => Int]](Failure(err1), Failure(err2)).collectResults.map(_.head)
    val right = List[Try[Int]](Failure(err3), Failure(err4)).collectResults.map(_.head)

    inside(left combine right) {
      case Failure(CompositeException(es)) => es should contain inOrderOnly(err1, err2, err3, err4)
    }
  }

  it should "fail if the left side is a CompositeException" in {
    val err1 = new IllegalArgumentException("foo")
    val err2 = new IllegalArgumentException("bar")
    val err3 = new IllegalArgumentException("baz")

    val left = List[Try[Int => Int]](Failure(err1), Failure(err2)).collectResults.map(_.head)
    val right = Failure(err3)

    inside(left combine right) {
      case Failure(CompositeException(es)) => es should contain inOrderOnly(err1, err2, err3)
    }
  }

  it should "fail if the right side is a CompositeException" in {
    val err1 = new IllegalArgumentException("foo")
    val err2 = new IllegalArgumentException("bar")
    val err3 = new IllegalArgumentException("baz")

    val left = Failure(err1)
    val right = List[Try[Int]](Failure(err2), Failure(err3)).collectResults.map(_.head)

    inside(left combine right) {
      case Failure(CompositeException(es)) => es should contain inOrderOnly(err1, err2, err3)
    }
  }
}
