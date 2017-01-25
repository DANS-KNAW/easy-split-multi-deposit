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

import nl.knaw.dans.easy.multideposit.Main.extractFileParameters
import org.scalatest.BeforeAndAfter

import scala.util.Try

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
    Try("abc").onError(_ => "foobar") shouldBe "abc"
  }

  it should "return the onError value when provided with a failure" in {
    Try[String](throw new Exception).onError(_ => "foobar") shouldBe "foobar"
  }

  "DatasetExtensions.getValue" should "return the correct value when provided with the correct parameters" in {
    dataset1.getValue("ROW")(0).value shouldBe "2"
  }

  it should "return None when the key is not in the dataset" in {
    dataset1.getValue("ROW!")(0) shouldBe empty
  }

  it should "return None when the row is not in the dataset" in {
    dataset1.getValue("ROW")(10) shouldBe empty
  }

  it should "return None when value is blank" in {
    dataset1.getValue("DDM_CREATED")(1) shouldBe empty
  }

  // TODO move to other class, since this method is now part of Main
  "extractFileParameters" should "succeed with correct dataset1" in {
    extractFileParameters(dataset1) should contain theSameElementsInOrderAs testFileParameters1
  }

  it should "succeed with correct dataset2" in {
    extractFileParameters(dataset2) should contain theSameElementsInOrderAs testFileParameters2
  }

  it should "return Nil when the dataset is empty" in {
    extractFileParameters(new Dataset) shouldBe empty
  }

  it should "return the fileParameters without row number when these are not supplied" in {
    val res = FileParameters(None, Option("videos/centaur.mpg"), Option("footage/centaur.mpg"), Option("http://zandbak11.dans.knaw.nl/webdav"), None, Option("Yes"))
    extractFileParameters(dataset1 -= "ROW") should contain only res
  }

  it should "return Nil when ALL extracted fields are removed from the dataset" in {
    dataset1 --= List("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")

    extractFileParameters(dataset1) shouldBe empty
  }
}
