package nl.knaw.dans.easy.multiDeposit

import java.io.File

import nl.knaw.dans.easy.multiDeposit.MultiDepositParser._

import org.apache.commons.io.FileUtils._

class MultiDepositParserSpec extends UnitSpec {

  //TODO rewrite tests to use the Rx Testing framework as soon as this is out of beta
  // to do so we need to upgrade the RxScala version in Maven

  "validateDatasetHeaders" should "succeed when given an empty list" in {
    val headers = Nil

    validateDatasetHeaders(headers).isSuccess shouldBe true
  }

  it should "succeed when given a subset of the valid headers" in {
    val headers = List("DATASET_ID", "FILE_STORAGE_PATH")

    validateDatasetHeaders(headers).isSuccess shouldBe true
  }

  it should "fail when the input contains invalid headers" in {
    val headers = List("FILE_SIP", "dataset_id")
    val result = validateDatasetHeaders(headers)

    result.isFailure shouldBe true
    (the [ActionException] thrownBy result.get).message should include ("unknown headers: dataset_id")
  }

  "parse" should "fail with empty instruction file" in {
    val csv = new File(testDir, "md/instructions.csv")
    write(csv, "")

    the[NoSuchElementException] thrownBy
      MultiDepositParser.parse(csv).toBlocking.toList should
      have message "next on empty iterator"
  }

  it should "fail without DATASET_ID in instructions file?" in {
    val csv = new File(testDir, "instructions.csv")
    write(csv, "SF_PRESENTATION,FILE_AUDIO_VIDEO\nx,y")

    the[Exception] thrownBy
      MultiDepositParser.parse(csv).toBlocking.toList should
      have message "java.lang.Exception: No dataset ID found"
  }

  it should "not complain about an invalid combination in instructions file?" in {

    val csv = new File(testDir, "instructions.csv")
    write(csv, "DATASET_ID,FILE_SIP\ndataset1,x")

    MultiDepositParser.parse(csv)
      .toBlocking.toList.head.head._1 shouldBe "dataset1"
  }

  it should "succeed with Roundtrip/sip-demo-2015-02-24" in {

    val csv = new File("src/test/resources/Roundtrip/sip-demo-2015-02-24/instructions.csv")
    MultiDepositParser.parse(csv).toBlocking.toList.head.head._1 shouldBe "ruimtereis01"
  }
}
