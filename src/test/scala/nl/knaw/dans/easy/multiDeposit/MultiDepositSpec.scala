/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.multideposit

import org.scalatest.BeforeAndAfter
import rx.lang.scala.observers.TestSubscriber

import scala.util.{Failure, Success, Try}

class MultiDepositSpec extends UnitSpec with BeforeAndAfter {

  val dataset1 = new Dataset
  val dataset2 = new Dataset

  before {
    dataset1 ++= testDataset1
    dataset2 ++= testDataset2
  }

  after {
    dataset1.clear
    dataset2.clear
  }

  "StringToOption.toOption" should "yield an Option.empty when given an empty String" in {
    "".toOption shouldBe Option.empty
  }

  it should "yield an Option.empty when given a String with spaces only" in {
    "   ".toOption shouldBe Option.empty
  }

  it should "yield the original String wrapped in Option when given a non-blank String" in {
    "abc".toOption shouldBe Option("abc")
  }

  "StringToOption.toIntOption" should "yield an Option.empty when given an empty String" in {
    "".toIntOption shouldBe Option.empty
  }

  it should "yield an Option.empty when given a String with spaces only" in {
    "   ".toIntOption shouldBe Option.empty
  }

  it should "yield an Option.empty when given a String with non-blank characters other than numbers" in {
    "abc".toIntOption shouldBe Option.empty
  }

  it should "yield the parsed Int wrapped in Option when given a String with non-blank number characters" in {
    "30071992".toIntOption shouldBe Option(30071992)
  }

  "TryExceptionHandling.onError" should "return the value when provided with a Success" in {
    Try("abc") getOrElse "foobar" shouldBe "abc"
  }

  it should "return the onError value when provided with a failure" in {
    Try[String](throw new Exception) getOrElse "foobar" shouldBe "foobar"
  }

  "DatasetExtensions.getValue" should "return the correct value when provided with the correct parameters" in {
    dataset1.getValue("ROW")(0) shouldBe Some("2")
  }

  it should "return None when the key is not in the dataset" in {
    dataset1.getValue("ROW!")(0) shouldBe None
  }

  it should "return None when the row is not in the dataset" in {
    dataset1.getValue("ROW")(10) shouldBe None
  }

  it should "return None when value is blank" in {
    dataset1.getValue("DDM_CREATED")(1) shouldBe None
  }

  "extractFileParameters" should "succeed with correct dataset1" in {
    val testSubscriber = TestSubscriber[FileParameters]
    extractFileParameters(dataset1).subscribe(testSubscriber)

    testSubscriber.assertValues(testFileParameters1 :_*)
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "succeed with correct dataset2" in {
    val testSubscriber = TestSubscriber[FileParameters]
    extractFileParameters(dataset2).subscribe(testSubscriber)

    testSubscriber.assertValues(testFileParameters2 :_*)
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "return Nil when the dataset is empty" in {
    val testSubscriber = TestSubscriber[FileParameters]
    extractFileParameters(new Dataset).subscribe(testSubscriber)

    testSubscriber.assertNoValues
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "return the fileParameters without row number when these are not supplied" in {
    val res = FileParameters(None, Option("videos/centaur.mpg"), Option("footage/centaur.mpg"), Option("http://zandbak11.dans.knaw.nl/webdav"), None, Option("Yes"))

    val testSubscriber = TestSubscriber[FileParameters]
    extractFileParameters(dataset1 -= "ROW").subscribe(testSubscriber)

    testSubscriber.assertValues(res)
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "return Nil when ALL extracted fields are removed from the dataset" in {
    dataset1 --= List("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")

    val testSubscriber = TestSubscriber[FileParameters]
    extractFileParameters(dataset1).subscribe(testSubscriber)

    testSubscriber.assertNoValues
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  "generateErrorReport" should "return an empty String when no header nor failure is provided" in {
    generateErrorReport("", Nil) shouldBe ""
  }

  it should "return only the header when no failure is supplied" in {
    generateErrorReport("foobar", Nil) shouldBe "foobar\n"
  }

  it should "return a formatted list of failure reports when no header is supplied" in {
    val f1 = Failure(ActionException(0, "foo"))
    val f2 = Failure(ActionException(1, "bar"))

    generateErrorReport("", List(f1, f2)) shouldBe " - row 0: foo\n - row 1: bar"
  }

  it should "return a header and formatted list of failure reports when no header is supplied" in {
    val f1 = Failure(ActionException(0, "foo"))
    val f2 = Failure(ActionException(1, "bar"))

    generateErrorReport("foobar", List(f1, f2)) shouldBe "foobar\n - row 0: foo\n - row 1: bar"
  }

  it should "fail if something else than a Failure[ActionException] is provided" in {
    val f1 = Failure(ActionException(0, "foo"))
    val f2 = Failure(new IllegalArgumentException("bar"))

    an [AssertionError] should be thrownBy generateErrorReport("", List(f1, f2))
  }

  it should "exclude Successes" in {
    val f1 = Failure(ActionException(0, "foo"))
    val f2 = Success(1)

    generateErrorReport("foobar", List(f1, f2)) shouldBe "foobar\n - row 0: foo"
  }

  it should "return a sorted list of failure reports" in {
    val f1 = Failure(ActionException(9, "foo"))
    val f2 = Failure(ActionException(3, "bar"))

    generateErrorReport("foobar", List(f1, f2)) shouldBe "foobar\n - row 3: bar\n - row 9: foo"
  }
}
