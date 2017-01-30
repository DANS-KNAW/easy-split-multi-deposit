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
package nl.knaw.dans.easy.multideposit.actions

import java.io.File

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.actions.CreateSpringfieldActions._
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}
import scala.xml.Utility

class CreateSpringfieldActionsSpec extends UnitSpec with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    springfieldInbox = new File(testDir, "springFieldInbox")
  )

  override def afterAll: Unit = testDir.getParentFile.deleteDirectory()

  def testDataset: Dataset = {
    new Dataset +=
      "SF_DOMAIN" -> List("dans") +=
      "SF_USER" -> List("someDeveloper") +=
      "SF_COLLECTION" -> List("scala") +=
      "SF_PRESENTATION" -> List("unit-test") +=
      "FILE_AUDIO_VIDEO" -> List("yes") +=
      "FILE_SIP" -> List("videos/some.mpg") +=
      "FILE_SUBTITLES" -> List("videos/some.txt") +=
      "FILE_DATASET" -> List("footage/some.mpg")
  }
  def datasets(dataset: Dataset = testDataset): Datasets = {
    new Datasets += ("dataset-1" -> dataset)
  }

  "execute" should "not create a file when an empty ListBuffer was passed on" in {
    val datasets = new Datasets

    CreateSpringfieldActions(1, datasets).execute shouldBe a[Success[_]]
    springfieldInboxActionsFile(settings) should not (exist)
  }

  it should "fail with too little instructions" in {
    val datasets = new Datasets() += ("dataset-1" -> (testDataset -= "FILE_SIP"))

    inside(CreateSpringfieldActions(1, datasets).execute()) {
      case Failure(ActionException(_, message, _)) => message should include ("Invalid video object")
    }
  }

  it should "create the correct file with the correct input" in {
    CreateSpringfieldActions(1, datasets()).execute shouldBe a[Success[_]]
    val generated = {
      val xmlFile = springfieldInboxActionsFile(settings)
      xmlFile should exist
      xmlFile.read()
    }

    generated should {
      include("subtitles=\"videos/some.txt\"") and
        include("src=\"videos/some.mpg\"") and
        include("target=\"/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test\"")
    }
  }

  "writeSpringfieldXml" should "create the correct file with the correct input" in {
    writeSpringfieldXml(1, datasets()) shouldBe a[Success[_]]
    val generated = {
      val xmlFile = springfieldInboxActionsFile(settings)
      xmlFile should exist
      xmlFile.read()
    }

    generated should {
      include("subtitles=\"videos/some.txt\"") and
        include("src=\"videos/some.mpg\"") and
        include("target=\"/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test\"")
    }
  }

  "toXML" should "return an empty xml when given empty datasets" in {
    toXML(new Datasets) should be (empty)
  }

  it should "return the xml when datasets are supplied with the proper fields set" in {
    toXML(datasets()).map(_.get).map(Utility.trim).value shouldBe
      Utility.trim {
        <actions>
          <add target="/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test">
            <video src="videos/some.mpg" target="0" subtitles="videos/some.txt"/>
          </add>
        </actions>
      }
  }

  "createAddElement" should "return an empy xml when given an empty list of videos" in {
    inside(createAddElement("target value", List.empty)) {
      case Success(xml) => Utility.trim(xml) shouldBe Utility.trim(<add target="target value"></add>)
    }
  }

  it should "throw a RuntimeException when an incorrect video is supplied" in {
    val videos = List(
      Video("0", Some("videos/some0.mpg"), Some("videos/some0.txt")),
      Video("1", None, None),
      Video("2", Some("videos/some2.mpg"), Some("videos/some2.txt"))
    )

    inside(createAddElement("target value", videos)) {
      case Failure(e: RuntimeException) => e.getMessage should include ("Invalid video object")
    }
  }

  it should "return the xml from a valid list of videos" in {
    val videos = List(
      Video("0", Some("videos/some0.mpg"), Some("videos/some0.txt")),
      Video("1", Some("videos/some1.mpg"), None),
      Video("2", Some("videos/some2.mpg"), Some("videos/some2.txt"))
    )

    inside(createAddElement("target value", videos)) {
      case Success(xml) => Utility.trim(xml) shouldBe
        Utility.trim(<add target="target value">
          <video src="videos/some0.mpg" target="0" subtitles="videos/some0.txt"/>
          <video src="videos/some1.mpg" target="1"/>
          <video src="videos/some2.mpg" target="2" subtitles="videos/some2.txt"/>
        </add>)
    }
  }

  "extractVideos" should "return an empty map when given an empty dataset" in {
    extractVideos(new Dataset) should be (empty)
  }

  it should "return an empty map when the FILE_AUDIO_VIDEO field is blank" in {
    extractVideos(testDataset += ("FILE_AUDIO_VIDEO" -> List(""))) should be (empty)
  }

  it should "return an empty map when the FILE_AUDIO_VIDEO field is not yes" in {
    extractVideos(testDataset += ("FILE_AUDIO_VIDEO" -> List("nope!"))) should be (empty)
  }

  it should "return a filled map when given the correct input with a single row in the dataset" in {
    extractVideos(testDataset) should {
      have size 1 and
        contain("/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test" ->
          List(Video("0", Some("videos/some.mpg"), Some("videos/some.txt"))))
    }
  }

  it should "return a filled map when given the correct input with multiple rows in the dataset" +
    "and only the first row filled" in {
    def testDataset2 = {
      val dataset = new Dataset
      dataset += "SF_DOMAIN" -> List("dans", "")
      dataset += "SF_USER" -> List("someDeveloper", "")
      dataset += "SF_COLLECTION" -> List("scala", "")
      dataset += "SF_PRESENTATION" -> List("unit-test", "")
      dataset += "FILE_AUDIO_VIDEO" -> List("yes", "")
      dataset += "FILE_SIP" -> List("videos/some.mpg", "")
      dataset += "FILE_SUBTITLES" -> List("videos/some.txt", "")
      dataset += "FILE_DATASET" -> List("footage/some.mpg", "")
    }

    extractVideos(testDataset2) should {
      have size 1 and
        contain("/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test" ->
          List(Video("0", Some("videos/some.mpg"), Some("videos/some.txt"))))
    }
  }

  it should "return a filled map when given the correct input with multiple rows in the dataset" in {
    def testDataset3 = {
      val dataset = new Dataset
      dataset += "SF_DOMAIN" -> List("dans", "")
      dataset += "SF_USER" -> List("someDeveloper", "")
      dataset += "SF_COLLECTION" -> List("scala", "")
      dataset += "SF_PRESENTATION" -> List("unit-test", "")
      dataset += "FILE_AUDIO_VIDEO" -> List("yes", "yes")
      dataset += "FILE_SIP" -> List("videos/some.mpg", "audio/herrie.mp3")
      dataset += "FILE_SUBTITLES" -> List("videos/some.txt", "")
      dataset += "FILE_DATASET" -> List("footage/some.mpg", "")
    }

    extractVideos(testDataset3) should {
      have size 1 and
        contain("/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test" ->
          List(Video("0", Some("videos/some.mpg"), Some("videos/some.txt")),
            Video("1", Some("audio/herrie.mp3"), None)))
    }
  }

  "getSpringfieldPath" should "not yield a path when given an empty dataset" in {
    getSpringfieldPath(new Dataset, 0) should be (empty)
  }

  it should "not yield a path when no domain is specified in the dataset" in {
    getSpringfieldPath(testDataset -= "SF_DOMAIN", 0) should be (empty)
  }

  it should "not yield a path when no user is specified in the dataset" in {
    getSpringfieldPath(testDataset -= "SF_DOMAIN", 0) should be (empty)
  }

  it should "not yield a path when no collection is specified in the dataset" in {
    getSpringfieldPath(testDataset -= "SF_COLLECTION", 0) should be (empty)
  }

  it should "not yield a path when no presentation is specified in the dataset" in {
    getSpringfieldPath(testDataset -= "SF_PRESENTATION", 0) should be (empty)
  }

  it should "not yield a path when given the incorrect index" in {
    getSpringfieldPath(testDataset, 1) should be (empty)
  }

  it should "yield the path when given a correct dataset" in {
    getSpringfieldPath(testDataset, 0).value shouldBe "/domain/dans/user/someDeveloper/collection/scala/presentation/unit-test"
  }
}
