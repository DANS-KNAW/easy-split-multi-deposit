package nl.knaw.dans.easy.multiDeposit

import java.io.File

import nl.knaw.dans.easy.multiDeposit.MultiDepositParser._

import org.apache.commons.io.FileUtils._
import rx.lang.scala.observers.TestSubscriber
import rx.lang.scala.ObservableExtensions

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MultiDepositParserSpec extends UnitSpec {

  "validateDatasetHeaders" should "succeed when given an empty list" in {
    val headers = Nil

    validateDatasetHeaders(headers).isSuccess shouldBe true
  }

  it should "succeed when given a subset of the valid headers" in {
    val headers = List("DATASET", "FILE_STORAGE_PATH")

    validateDatasetHeaders(headers).isSuccess shouldBe true
  }

  it should "fail when the input contains invalid headers" in {
    val headers = List("FILE_SIP", "dataset")

    val validate = validateDatasetHeaders(headers)

    validate.isFailure shouldBe true
    (the [ActionException] thrownBy validate.get).message should
      include ("unknown headers: dataset")
    (the [ActionException] thrownBy validate.get).row shouldBe 0
  }

//  it should "throw an exception with text \"unknown header: <name>\" when the input contains invalid headers" in {
//    val headers = List("FILE_SIP", "dataset")
//
//    (the [ActionException] thrownBy validateDatasetHeaders(headers).get).message should
//      include ("unknown headers: dataset")
//  }
//
//  it should "throw an exception on row 0 when the input contains invalid headers" in {
//    val headers = List("FILE_SIP", "dataset")
//    (the [ActionException] thrownBy validateDatasetHeaders(headers).get).row shouldBe 0
//  }

  "parse" should "fail with empty instruction file" in {
    val csv = new File(testDir, "md/instructions.csv")
    write(csv, "")

    val testSubscriber = TestSubscriber[Datasets]
    parse(csv).subscribe(testSubscriber)

    testSubscriber.assertNoValues
    testSubscriber.assertError(classOf[NoSuchElementException])
    testSubscriber.assertNotCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "fail without DATASET in instructions file?" in {
    val csv = new File(testDir, "instructions.csv")
    write(csv, "SF_PRESENTATION,FILE_AUDIO_VIDEO\nx,y")

    val testSubscriber = TestSubscriber[Datasets]
    parse(csv).subscribe(testSubscriber)

    testSubscriber.assertNoValues
    testSubscriber.assertError(classOf[Exception])
    testSubscriber.assertNotCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "not complain about an invalid combination in instructions file?" in {
    val csv = new File(testDir, "instructions.csv")
    write(csv, "DATASET,FILE_SIP\ndataset1,x")

    val testSubscriber = TestSubscriber[Datasets]
    parse(csv).subscribe(testSubscriber)

    val dataset = mutable.HashMap(
      "ROW" -> List("2"),
      "DATASET" -> List("dataset1"),
      "FILE_SIP" -> List("x"))
    val expected = ListBuffer(("dataset1", dataset))
    testSubscriber.assertValue(expected)
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }

  it should "succeed with Roundtrip/sip-demo-2015-02-24" in {
    val csv = new File("src/test/resources/Roundtrip_MD/sip-demo-2015-02-24/instructions.csv")

    val testSubscriber = TestSubscriber[String]
    parse(csv).flatMap(_.map(_._1).toObservable).subscribe(testSubscriber)

    testSubscriber.assertValues("ruimtereis01", "ruimtereis02")
    testSubscriber.assertNoErrors
    testSubscriber.assertCompleted
    testSubscriber.assertUnsubscribed
  }
}
