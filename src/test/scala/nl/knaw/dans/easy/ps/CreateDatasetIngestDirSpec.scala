
package nl.knaw.dans.easy.ps

import nl.knaw.dans.easy.ps.CustomMatchers._
import nl.knaw.dans.easy.ps.Main._
import scala.util.{Failure, Success}
import org.apache.commons.io.FileUtils.write

class CreateDatasetIngestDirSpec extends UnitSpec {

  implicit val s = Settings(
    ebiuDir = file(testDir, "ebiu"),
    sipDir = file(testDir, "sip")
  )

  def createSipFiles() = {
    file(testDir, "ebiu").mkdirs()
    file(testDir, "sip").mkdirs()
    Array("skipme.txt", "copyme.txt", "copymenot.txt", "copymetoo.txt")
      .foreach { fileName => write(file(testDir, "sip/dataset-1", fileName), "dummy content") }
  }

  // constructor arguments for the class under test
  val instructions: Dataset = {
    dataset(Map(
      "ROW" -> List("1", "2"),
      "DATASET_ID" -> List("dataset-1", "dataset-1"),
      "FILE_SIP" -> List("skipme.txt", "dataset-1/copymenot.txt")
    ))
  }
  val firstInstructionRowNr = instructions(key = "ROW").head
  val fileParametersList = extractFileParametersList(instructions)

  "getFileActions" should "fail when path is missing in FILE_SIP" in {

    // beyond the scope of the test class but explains why
    // - skip me is copied to ebiu/dataset-1
    // - fileParameterList is not extracted by CreateDatasetIngestDir
    //TODO shouldn't the tested code have a class that takes instructions and delivers fileParametersList/storagePath/fileActions?
    //TODO shouldn't this input error be detected before we start copying (large) files?
    //TODO shouldn't fileParametersList contain skipme despite the missing path in the instructions? That would make this test independent.
    val errorMsg = getFileActions(instructions, fileParametersList, calculateDatasetStoragePath(instructions))
      .head match {
        case Failure(e) => e.getMessage()
        case _ => ""
      }
    errorMsg should fullyMatch regex ".*Invalid combination of file parameters.*skipme.*"
  }

  "checkPreconditions" should "fail with empty dataset-map" in {
    val action = CreateDatasetIngestDir("1", dataset(Map()), null)

    the[NoSuchElementException] thrownBy action.checkPreconditions should
      have message "key not found: DATASET_ID"
  }

  it  should "fail if dataset folder already exists" in {
    file(testDir, "ebiu/dataset-1").mkdirs()
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, null)

    action.checkPreconditions should failWithActionExceptionMatching (row = "1",msg = ".*cannot be created.*already exists.*")
  }

  it  should "succeed with emtpy ebiu dir" in {
    file(testDir, "ebiu").mkdirs()
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, null)

    action.checkPreconditions shouldBe a[Success[_]]
  }

  "run" should "fail if ebiu dir does not exists" in {
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, null)

    action.run should failWithActionExceptionMatching (row = "1",msg = ".*Could not create .* at .*/ebiu/dataset-1")
    file(testDir, "ebiu/dataset-1") should not (exist)
  }

  it should "succeed with sipDir that does not exist" in {
    file(testDir, "ebiu").mkdirs()
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, fileParametersList)

    action.run shouldBe a[Success[_]]
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR) should be a 'directory
    file(testDir, "ebiu/dataset-1", EBIU_METADATA_DIR) should be a 'directory
  }

  it should "copy some files of the sipDir" in {
    createSipFiles()
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, fileParametersList)

    action.run shouldBe a[Success[_]]
    // TODO alleen instruction file copieeren volgens: https://drivenbydata.atlassian.net/secure/attachment/15811/IMG_1356.JPG
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "copymenot.txt") should not (exist)
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "copyme.txt") should be a 'file
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "copymetoo.txt") should be a 'file
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "skipme.txt") should be a 'file
  }

  it should "copy all files of the sipDir" in {
    createSipFiles()
    val instructions: Dataset = {dataset(Map("ROW" -> List("1"),"DATASET_ID" -> List("dataset-1")))}
    val action = CreateDatasetIngestDir("1", instructions, extractFileParametersList(instructions))

    action.run shouldBe a[Success[_]]
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "copymenot.txt") should be a 'file
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "copyme.txt") should be a 'file
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "copymetoo.txt") should be a 'file
    file(testDir, "ebiu/dataset-1", EBIU_FILEDATA_DIR, "skipme.txt") should be a 'file
  }

  "rollback" should "succeed after run with sipDir that does not exist" in {
    file(testDir, "ebiu").mkdirs()
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, fileParametersList)
    action.run()

    action.rollback shouldBe a[Success[_]]
    file(testDir, "ebiu") should be a 'directory
    file(testDir, "ebiu/dataset-1") should not(exist)
  }

  it should "succeed after run with content in sipDir" in {
    createSipFiles()
    val action = CreateDatasetIngestDir(firstInstructionRowNr, instructions, fileParametersList)
    action.run()

    action.rollback shouldBe a[Success[_]]
    file(testDir, "ebiu") should be a 'directory
    file(testDir, "ebiu/dataset-1") should not(exist)
  }
}
