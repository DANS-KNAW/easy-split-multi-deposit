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

import java.io.{File, FileNotFoundException}

import nl.knaw.dans.easy.multideposit.{ActionException, Settings, UnitSpec, _}
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success}

class CopyToSpringfieldInboxSpec extends UnitSpec {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    springfieldInbox = new File(testDir, "springFieldInbox")
  )

  def createFile(fileName: MultiDepositKey): Unit = {
    val file = multiDepositDir(fileName)
    file.getParentFile.mkdirs
    file.write("")
  }

  "checkPreconditions" should "fail if file does not exist" in {
    inside(CopyToSpringfieldInbox(1, "videos/some_checkPreFail.mpg").checkPreconditions) {
      case Failure(ActionException(_, message, _)) => message should include ("Cannot find MD file")
    }
  }

  it should "succeed if file exist" in {
    createFile("videos/some_checkPreSuccess.mpg")

    CopyToSpringfieldInbox(1, "videos/some_checkPreSuccess.mpg").checkPreconditions shouldBe a[Success[_]]
  }

  "execute" should "succeed if file exist" in {
    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(1, "videos/some.mpg").execute shouldBe a[Success[_]]
  }

  it should "fail if file does not exist" in {
    inside(CopyToSpringfieldInbox(1, "videos/some_error.mpg").execute()) {
      case Failure(ActionException(_, message, cause)) =>
        message should include ("videos/some_error.mpg")
        cause shouldBe a[FileNotFoundException]
    }
  }

  "rollback" should "delete the files and directories that were added by action.run()" in {
    createFile("videos/some_rollback.mpg")
    val action = CopyToSpringfieldInbox(1, "videos/some_rollback.mpg")

    action.execute shouldBe a[Success[_]]

    springfieldInboxDir("videos/some_rollback.mpg") should exist

    action.rollback shouldBe a[Success[_]]

    springfieldInboxDir("videos/some_rollback.mpg") should not (exist)
    springfieldInboxDir("videos") should not (exist)
  }
}
