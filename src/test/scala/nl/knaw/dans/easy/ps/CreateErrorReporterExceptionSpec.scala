package nl.knaw.dans.easy.ps

import scala.util.Failure

class CreateErrorReporterExceptionSpec extends UnitSpec {

  "createErrorReporterException" should "yield empty message if input is empty" in {
    createErrorReporterException("", List()).getMessage shouldBe empty
  }

  it should "yield no heading if none specified" in {
    createErrorReporterException("", List(Failure(ActionException("1", "some error message"))))
      .getMessage should startWith(" - row 1:")
  }

  it should "yield a heading if specified" in {
    createErrorReporterException("My heading", List(Failure(ActionException("1", "some error message"))))
      .getMessage should startWith("My heading")
  }

  it should "accept no other exceptions and ActionException and ActionExceptionList" in {
    intercept[AssertionError] {
      createErrorReporterException("My heading", List(Failure(new Exception("Incorrect exception type"))))
    }
  }

  it should "handle ActionExceptionLists as sequences of ActionExceptions" in {
    createErrorReporterException("", List(
      Failure(ActionException("1", "")),
      Failure(ActionExceptionList(List(ActionException("2", ""), ActionException("3", "")))),
      Failure(ActionException("4", ""))))
      .getMessage should fullyMatch regex """^\s*- row 1:\s+- row 2:\s+- row 3:\s+- row 4:\s*$"""
  }

  it should "sort ActionExceptions by row number (numerically, *not* String-wise)" in {
    createErrorReporterException("", List(
      Failure(ActionException("2", "")),
      Failure(ActionException("100", "")),
      Failure(ActionException("1", ""))))
      .getMessage should fullyMatch regex """^\s*- row 1:\s+- row 2:\s+- row 100:\s*$"""
  }

}