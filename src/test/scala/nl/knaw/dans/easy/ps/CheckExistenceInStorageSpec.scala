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
