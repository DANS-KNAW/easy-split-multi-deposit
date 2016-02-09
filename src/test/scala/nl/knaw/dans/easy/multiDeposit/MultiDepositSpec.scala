package nl.knaw.dans.easy.multiDeposit

import org.scalatest.{DoNotDiscover, BeforeAndAfter}

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

  it should "succeed with correct dataset1" in {
    extractFileParametersList(dataset1) shouldBe testFileParameters1
  }

  it should "succeed with correct dataset2" in {
    extractFileParametersList(dataset2) shouldBe testFileParameters2
  }

  it should "return Nil when the dataset is empty" in {
    extractFileParametersList(new Dataset) shouldBe Nil
  }

  it should "return the fileParameters without row number when these are not supplied" in {
    val res = FileParameters(None, Option("videos/centaur.mpg"), Option("footage/centaur.mpg"), Option("http://zandbak11.dans.knaw.nl/webdav"), None, Option("Yes"))
    extractFileParametersList(dataset1 -= "ROW") shouldBe List(res)
  }

  it should "return Nil when ALL extracted fields are removed from the dataset" in {
    dataset1 --= List("ROW", "FILE_SIP", "FILE_DATASET", "FILE_STORAGE_SERVICE", "FILE_STORAGE_PATH", "FILE_AUDIO_VIDEO")
    extractFileParametersList(dataset1) shouldBe Nil
  }
}
