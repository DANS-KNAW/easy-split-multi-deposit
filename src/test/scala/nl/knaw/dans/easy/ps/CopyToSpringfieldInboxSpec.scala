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

import java.io.FileNotFoundException

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils
import org.scalatest.Ignore

import scala.util.Success

@Ignore
class CopyToSpringfieldInboxSpec extends UnitSpec {

  implicit val s = Settings(
    sipDir = file(testDir, "sip"),
    springfieldInbox = file(testDir, "springFieldInbox")
  )

  "checkPreconditions" should "fail if file does not exist" in {

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .checkPreconditions should failWithActionExceptionMatching (row = "1",msg = ".*Cannot find SIP file.*")
  }


  it should "succeed if file exist" in {

    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed if file exist" in {

    createFile("videos/some.mpg")

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .run shouldBe a[Success[_]]
  }

  it should "fail if file does not exist" in {

    CopyToSpringfieldInbox(row = "1", fileSip = "videos/some.mpg")
      .run should failWith (a[FileNotFoundException], "videos/some.mpg")
  }

  def createFile(fileName: MdKey): Unit = {
    val theFile = file(s.sipDir, fileName)
    theFile.getParentFile.mkdirs()
    FileUtils.write(theFile, "")
  }
}
