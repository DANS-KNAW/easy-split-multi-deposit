package nl.knaw.dans.easy.ps


import java.io.FileNotFoundException

import nl.knaw.dans.easy.ps.CustomMatchers._
import scala.util.{Failure, Success}
import language.postfixOps

class CreateDataFileInstructionsSpec extends UnitSpec {

  implicit val s = Settings(
    ebiuDir = file(testDir, "ebiu"),
    sipDir = file(testDir, "sip"),
    storageServices = Map()
  )

  "checkPreconditions" should "fail if no fileStorageDatasetPath is provided" in {

    val action = CreateDataFileInstructions(row = "",
      fileSip = None,
      datasetId = "",
      fileInDataset = "",
      fileStorageService = "",
      fileStorageDatasetPath = None,
      fileStorageFilePath = ""
    )
    inside(action.checkPreconditions) {
      case Failure(ActionExceptionList(aes: Seq[ActionException])) =>
        aes.length shouldBe 1
        aes.head.message shouldBe "path on storage could not be determined"
    }
  }

  it should "succeed if fileStorageDatasetPath is an empty string?" in {

    val action = CreateDataFileInstructions(row = "",
      fileSip = None,
      datasetId = "",
      fileInDataset = "",
      fileStorageService = "",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )
    action.checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "throw a FileNotFoundException without an existing filedata directory" in {

    val action = CreateDataFileInstructions(row = "1",
      fileSip = None,
      datasetId = "",
      fileInDataset = "",
      fileStorageService = "",
      fileStorageDatasetPath = Some(""),
      fileStorageFilePath = ""
    )
    the [FileNotFoundException] thrownBy action.run() should
      have message file(s.ebiuDir, "filedata/.properties")+" (No such file or directory)"
  }

  it should "create a properties file" in {

    val propertiesFile = file(s.ebiuDir, "dataset-1/filedata/some.mpeg.properties")
    propertiesFile.getParentFile.mkdirs()

    val action = CreateDataFileInstructions(row = "1",
      fileSip = None,
      datasetId = "dataset-1",
      fileInDataset = "some.mpeg",
      fileStorageService = "ss",
      fileStorageDatasetPath = Some("dsp"),
      fileStorageFilePath = "sfp"
    )
    action.run shouldBe a[Success[_]]
    propertiesFile should be a 'file
    // TODO assert content
  }

  "rollback" should "succeed and leave created properties file?" in {

    val propertiesFile = file(s.ebiuDir, "dataset-1/filedata/some.mpeg.properties")
    propertiesFile.getParentFile.mkdirs()

    val action = CreateDataFileInstructions(row = "1",
      fileSip = None,
      datasetId = "dataset-1",
      fileInDataset = "some.mpeg",
      fileStorageService = "ss",
      fileStorageDatasetPath = Some("dsp"),
      fileStorageFilePath = "sfp"
    )
    action.run()

    action.rollback() shouldBe a[Success[_]]
    propertiesFile should exist
  }
}
