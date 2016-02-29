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

import java.io.File

import nl.knaw.dans.easy.multideposit.{Settings, UnitSpec, _}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.util.Success

class CreateOutputDepositDirSpec extends UnitSpec with BeforeAndAfter with BeforeAndAfterAll {

  implicit val settings = Settings(
    multidepositDir = new File(testDir, "md"),
    depositDir = new File(testDir, "dd")
  )

  override def beforeAll = testDir.mkdirs

  before {
    // create depositDir base directory
    val baseDir = settings.depositDir
    baseDir.mkdir
    baseDir.exists shouldBe true
  }

  after {
    // clean up stuff after the test is done
    val baseDir = settings.depositDir
    baseDir.deleteDirectory()
    baseDir.exists shouldBe false
  }

  override def afterAll = testDir.getParentFile.deleteDirectory()

  "checkPreconditions" should "always succeed" in {
    CreateOutputDepositDir(1, "ds1").checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "create the directories" in {
    // test is in seperate function,
    // since we want to reuse the code
    runTest()
  }

  "rollback" should "" in {
    // setup for this test
    runTest()

    // roll back the creation of the directories
    CreateOutputDepositDir(1, "ds1").rollback() shouldBe a[Success[_]]

    // test that the directories are really not there anymore
    new File(testDir, "dd/md-ds1").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag/metadata").exists shouldBe false
  }

  def runTest(): Unit = {
    // directories do not exist before
    new File(testDir, "dd/md-ds1").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag").exists shouldBe false
    new File(testDir, "dd/md-ds1/bag/metadata").exists shouldBe false

    // creation of directories
    CreateOutputDepositDir(1, "ds1").run() shouldBe a[Success[_]]

    // test existance after creation
    new File(testDir, "dd/md-ds1").exists shouldBe true
    new File(testDir, "dd/md-ds1/bag").exists shouldBe true
    new File(testDir, "dd/md-ds1/bag/metadata").exists shouldBe true
  }

}
