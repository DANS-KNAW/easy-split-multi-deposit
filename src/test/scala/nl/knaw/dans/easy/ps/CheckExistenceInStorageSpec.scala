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

import nl.knaw.dans.easy.ps.CustomMatchers._
import org.apache.commons.io.FileUtils._

import scala.util.Success

class CheckExistenceInStorageSpec extends UnitSpec {

  "checkPreconditions and run" should "succeed if file exists" in {

    implicit val s = mockSettings()
    write(file(storageLocation,"zandbak/target.mpg"), "dummy content")
    val action = CheckExistenceInStorage(
      row = "",
      fileStorageService = "zandbak",
      fileStoragePath = "target.mpg"
    )
    action.checkPreconditions shouldBe a[Success[_]]
    action.run shouldBe a[Success[_]]
  }

  "checkPreconditions and run" should "fail if file does not exist" in {

    implicit val s = mockSettings()
    val action = CheckExistenceInStorage(
      row = "1",
      fileStorageService = "zandbak",
      fileStoragePath = "target.mpg"
    )
    action.checkPreconditions should failWithActionExceptionMatching(row = "1", msg = "File not present in storage")
    action.run should failWithActionExceptionMatching(row = "1", msg = "File not present in storage")
  }
}
