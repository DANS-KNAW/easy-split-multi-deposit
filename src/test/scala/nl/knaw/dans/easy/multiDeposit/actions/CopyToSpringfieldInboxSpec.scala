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
package nl.knaw.dans.easy.multideposit.actions

import java.io.{FileNotFoundException, File}

import nl.knaw.dans.easy.multideposit._
import nl.knaw.dans.easy.multideposit.{ActionException, Settings, UnitSpec}
import nl.knaw.dans.easy.ps.MdKey
import org.scalatest.BeforeAndAfterAll

import scala.util.Success

class CopyToSpringfieldInboxSpec extends UnitSpec with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    springfieldInbox = new File(testDir, "springFieldInbox")
  )

  override def afterAll = testDir.getParentFile.deleteDirectory()

  def createFile(fileName: MdKey) = {
    val file = new File(settings.multidepositDir, fileName)
    file.getParentFile.mkdirs
    file.write("")
  }

  "checkPreconditions" should "fail if file does not exist" in {
    val pre = CopyToSpringfieldInbox(1, "videos/some_checkPreFail.mpg").checkPreconditions

    (the [ActionException] thrownBy pre.get).row shouldBe 1
    (the [ActionException] thrownBy pre.get).message should include ("Cannot find MD file:")
  }

  it should "succeed if file exist" in {
    createFile("videos/some_checkPreSuccess.mpg")

    CopyToSpringfieldInbox(1, "videos/some_checkPreSuccess.mpg").checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed if file exist" in {
    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(1, "videos/some.mpg").run shouldBe a[Success[_]]
  }

  it should "fail if file does not exist" in {
    val run = CopyToSpringfieldInbox(1, "videos/some_error.mpg").run()
    (the [FileNotFoundException] thrownBy run.get).getMessage should include ("videos/some_error.mpg")
  }

  "rollback" should "delete the files and directories that were added by action.run()" in {
    createFile("videos/some_rollback.mpg")
    val action = CopyToSpringfieldInbox(1, "videos/some_rollback.mpg")

    action.run() shouldBe a[Success[_]]

    new File(settings.springfieldInbox, "videos/some_rollback.mpg").exists() shouldBe true

    action.rollback shouldBe a[Success[_]]

    new File(settings.springfieldInbox, "videos/some_rollback.mpg").exists() shouldBe false
    new File(settings.springfieldInbox, "videos").exists() shouldBe false
  }
}
