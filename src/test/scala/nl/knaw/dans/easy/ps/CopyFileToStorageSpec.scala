package nl.knaw.dans.easy.ps

import org.apache.commons.io.FileUtils.write

import scala.util.{Failure, Success}

class CopyFileToStorageSpec extends UnitSpec {

  "Settings.resolve" should "throw an exception without storageServices" in {

    the[NullPointerException] thrownBy
      mockSettings(storageServicesInstance = null).resolve("xyz") should have message null
  }

  "constructor" should "throw an exception without storageServices" in {

    implicit val s = mockSettings(storageServicesInstance = null)
    the[NullPointerException] thrownBy CopyFileToStorage(
      row = "",
      storageService = "",
      fileSip = "",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )should have message null
  }

  "checkPreconditions" should "fail without connector and with empty strings as arguments" in {

    implicit val s = mockSettings(storageConnectorInstance = null)
    val action = CopyFileToStorage(
      row = "",
      storageService = "",
      fileSip = "",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )
    inside(action.checkPreconditions) {
      case Failure(ActionExceptionList(aes: Seq[ActionException])) =>
        aes.length shouldBe 2
        aes(0).message should startWith("Unable to determine whether storage location // is free")
        aes(1).message should startWith("Cannot find SIP file")
    }
  }

  it should "fail without connector and with just an existing sip file" in {

    implicit val s = mockSettings(storageConnectorInstance = null)
    write(file(s.sipDir, "source.mpg"), "dummy content")
    val action = CopyFileToStorage(
      row = "",
      storageService = "",
      fileSip = "source.mpg",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )
    inside(action.checkPreconditions) {
      case Failure(ActionExceptionList(aes: Seq[ActionException])) =>
        aes.length shouldBe 1
        aes.head.message should startWith("Unable to determine whether storage location // is free")
    }
  }

  it should "fail if location is occupied" in {

    implicit val s = mockSettings()
    write(file(storageLocation,"zandbak/target.mpg"),"")
    val action = CopyFileToStorage(
      row = "",
      storageService = "zandbak",
      fileSip = "source.mpg",
      fileStorageDatasetPath = Some("target.mpg"),
      fileStorageFilePath = ""
    )
    inside(action.checkPreconditions) {
      case Failure(ActionExceptionList(aes: Seq[ActionException])) =>
        aes.length shouldBe 2
        aes(0).message should startWith("Storage location already occupied")
        aes(1).message should startWith("Cannot find SIP file")
    }
  }

  it should "succeed if target is free and source exists" in {

    implicit val s = mockSettings()
    write(file(s.sipDir, "source.mpg"), "dummy content")
    val action = CopyFileToStorage(
      row = "",
      storageService = "zandbak",
      fileSip = "source.mpg",
      fileStorageDatasetPath = Some("target.mpg"),
      fileStorageFilePath = ""
    )
    action.checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "succeed" in {

    implicit val s = mockSettings()
    write(file(s.sipDir, "source.mpg"), "dummy content")
    val action = CopyFileToStorage(
      row = "",
      storageService = "zandbak",
      fileSip = "source.mpg",
      fileStorageDatasetPath = Some("target.mpg"),
      fileStorageFilePath = ""
    )
    action.run shouldBe a[Success[_]]
    file(storageLocation,"zandbak/target.mpg") should be a 'file
  }
}
