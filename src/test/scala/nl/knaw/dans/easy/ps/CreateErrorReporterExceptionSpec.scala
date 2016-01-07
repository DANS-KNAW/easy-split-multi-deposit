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