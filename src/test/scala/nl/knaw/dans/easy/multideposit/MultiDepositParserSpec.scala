/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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

import java.io.File
import java.util.NoSuchElementException

import nl.knaw.dans.easy.multideposit.MultiDepositParser._
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable.ListBuffer
import scala.util.{ Failure, Success }

class MultiDepositParserSpec extends UnitSpec with BeforeAndAfterAll {

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  "validateDatasetHeaders" should "succeed when given an empty list" in {
    val headers = Nil

    validateDatasetHeaders(headers) shouldBe a[Success[_]]
  }

  it should "succeed when given a subset of the valid headers" in {
    val headers = List("DATASET", "FILE_STORAGE_PATH")

    validateDatasetHeaders(headers) shouldBe a[Success[_]]
  }

  it should "fail when the input contains invalid headers" in {
    val headers = List("FILE_SIP", "dataset")

    val validate = validateDatasetHeaders(headers)

    inside(validate) {
      case Failure(ActionException(row, msg, _)) =>
        row shouldBe 0
        msg should include ("unknown headers: dataset")
    }
  }

  "parse" should "fail with empty instruction file" in {
    val csv = new File(testDir, "instructions.csv")
    csv.write("")

    inside(parse(csv)) {
      case Failure(EmptyInstructionsFileException(file)) => file shouldBe csv
    }
  }

  it should "fail without DATASET in instructions file?" in {
    val csv = new File(testDir, "instructions.csv")
    csv.write("SF_COLLECTION,FILE_AUDIO_VIDEO\nx,y")

    inside(parse(csv)) {
      case Failure(e) => e.getMessage should include ("No dataset ID found")
    }
  }

  it should "not complain about an invalid combination in instructions file?" in {
    val csv = new File(testDir, "instructions.csv")
    csv.write("DATASET,FILE_SIP\ndataset1,x\ndataset1,y")

    inside(parse(csv)) {
      case Success(ListBuffer((id, ds))) =>
        id shouldBe "dataset1"
        ds should contain ("ROW" -> List("2", "3"))
        ds should contain ("DATASET" -> List("dataset1", "dataset1"))
        ds should contain ("FILE_SIP" -> List("x", "y"))
    }
  }

  it should "succeed with Roundtrip_MD/spacetravel" in {
    val csv = new File(getClass.getResource("/spacetravel/instructions.csv").toURI)

    inside(parse(csv).map(_.map(_._1))) {
      case Success(ids) => ids should contain allOf ("ruimtereis01", "ruimtereis02")
    }
  }

  it should "not include whitespace identifiers" in {
    val csv = new File(getClass.getResource("/instructions_with_whitespace.csv").toURI)

    inside(parse(csv).map(_.map(_._1))) {
      case Success(ids) => ids should contain only "ruimtereis01"
    }
  }
}
